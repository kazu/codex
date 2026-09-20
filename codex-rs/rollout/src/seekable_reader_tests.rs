use std::cell::Cell;
use std::fs;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use pretty_assertions::assert_eq;

use super::*;

#[tokio::test]
async fn dropping_the_caller_stops_compressed_materialization() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl.zst");
    let original = vec![b'x'; 1024 * 1024];
    let compressed = zstd::stream::encode_all(original.as_slice(), /*level*/ 3)?;
    fs::write(&path, &compressed)?;

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let read_path = path.clone();
    let task = tokio::spawn(async move {
        let (stop, keep_running) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::task::spawn_blocking(move || {
            let checks = Cell::new(0);
            let started = Cell::new(Some(started_tx));
            let result = open_rollout_seekable_reader(&read_path, || {
                checks.set(checks.get() + 1);
                // Check on open and around the first source read and decoded chunk.
                // Pause after that chunk so a check only before decompression cannot pass.
                if checks.get() >= 5
                    && let Some(started) = started.take()
                {
                    let _ = started.send(());
                    let _ = resume_rx.recv();
                }
                stop.is_closed()
            });
            let _ = finished_tx.send(result.err().map(|error| error.kind()));
        });
        let result = worker.await;
        drop(keep_running);
        result
    });

    started_rx.await?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    resume_tx.send(())?;
    assert_eq!(finished_rx.await?, Some(io::ErrorKind::Other));
    assert_eq!(fs::read(&path)?, compressed);
    assert!(!path.with_extension("").exists());
    Ok(())
}

#[tokio::test]
async fn framed_readers_preserve_logical_bytes_and_materialization() -> anyhow::Result<()> {
    use crate::framed::FramedWriter;
    use std::num::NonZeroUsize;

    let records = ["first\r\n", "second 🦀\n", "third\n", "fourth\n"];
    for count in [0, 1, 2, 3, 4] {
        let home = tempfile::tempdir()?;
        let path = home.path().join("rollout.jsonl");
        let archive = path.with_extension("seek.zst");
        let pending = home.path().join(".pendings.rollout.seek.zst");
        let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 3).unwrap())?;
        for record in &records[..count] {
            writer.append(record.as_bytes())?;
            writer.promote()?;
        }
        drop(writer);
        let expected = records[..count].concat();
        if archive.exists() {
            // A leftover pending file must never supplement or replace the archive.
            fs::write(
                &pending,
                [0u64.to_le_bytes().as_slice(), b"stale\n"].concat(),
            )?;
        }
        for input in [&path, &archive, &pending] {
            let mut lines = crate::open_rollout_line_reader(input).await?;
            let mut actual = Vec::new();
            while let Some(line) = lines.next_line().await? {
                actual.push(line);
            }
            assert_eq!(actual, expected.lines().collect::<Vec<_>>());
            let mut reader = open_rollout_seekable_reader(input)?;
            assert_eq!(reader.metadata()?.len(), expected.len() as u64);
            for offset in 0..=expected.len() {
                reader.seek(SeekFrom::Start(offset as u64))?;
                let mut suffix = Vec::new();
                reader.read_to_end(&mut suffix)?;
                assert_eq!(suffix, expected.as_bytes()[offset..]);
                assert_eq!(
                    read_rollout_prefix(input, offset)?,
                    Some(expected.as_bytes()[..offset].to_vec())
                );
                assert!(rollout_contains_prefix(input, offset as u64)?);
            }
            assert!(!rollout_contains_prefix(input, expected.len() as u64 + 1)?);
        }
        let physical = if archive.exists() { &archive } else { &pending };
        assert_eq!(
            crate::materialize_rollout_for_reference(physical).await?,
            path
        );
        assert_eq!(crate::materialize_rollout_for_reference(&path).await?, path);
        assert_eq!(fs::read(&path)?, expected.as_bytes());
        assert!(!archive.exists());
        assert!(!pending.exists());
    }
    Ok(())
}

#[tokio::test]
async fn framed_readers_report_corrupt_authoritative_archive() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let pending = home.path().join(".pendings.rollout.seek.zst");
    fs::write(
        &pending,
        [0u64.to_le_bytes().as_slice(), b"stale\n"].concat(),
    )?;
    fs::write(path.with_extension("seek.zst"), b"invalid zstd")?;
    let result = async {
        let mut reader = crate::open_rollout_line_reader(&path).await?;
        while reader.next_line().await?.is_some() {}
        Ok::<_, io::Error>(())
    }
    .await;
    assert!(result.is_err());
    assert!(read_rollout_prefix(&path, /*max_bytes*/ 100).is_err());
    assert!(open_rollout_seekable_reader(&path).is_err());
    assert!(
        crate::materialize_rollout_for_reference(&path)
            .await
            .is_err()
    );
    assert!(!path.exists());
    assert!(pending.exists());
    Ok(())
}

#[tokio::test]
async fn framed_readers_retain_open_pending_across_publication() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let mut writer =
        crate::framed::FramedWriter::open(&path, std::num::NonZeroUsize::new(/*n*/ 2).unwrap())?;
    writer.append(b"first\n")?;
    let mut lines = crate::open_rollout_line_reader(&path).await?;
    let mut snapshot = open_rollout_seekable_reader(&path)?;
    writer.append(b"second\n")?;
    writer.promote()?;
    drop(writer);
    assert_eq!(lines.next_line().await?, Some("first".to_owned()));
    assert_eq!(lines.next_line().await?, Some("second".to_owned()));
    assert_eq!(lines.next_line().await?, None);
    let mut retained = Vec::new();
    snapshot.read_to_end(&mut retained)?;
    assert_eq!(retained, b"first\n");
    Ok(())
}

#[tokio::test]
async fn framed_readers_reject_truncated_pending_header() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    fs::write(home.path().join(".pendings.rollout.seek.zst"), [0; 7])?;
    assert_eq!(
        crate::open_rollout_line_reader(&path)
            .await
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::UnexpectedEof,
    );
    assert_eq!(
        open_rollout_seekable_reader(&path).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert!(
        crate::materialize_rollout_for_reference(&path)
            .await
            .is_err()
    );
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn framed_metadata_uses_the_same_authoritative_file() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let archive = path.with_extension("seek.zst");
    let pending = home.path().join(".pendings.rollout.seek.zst");
    let mut writer =
        crate::framed::FramedWriter::open(&path, std::num::NonZeroUsize::new(/*n*/ 2).unwrap())?;
    for source in [&pending, &archive] {
        writer.append(b"record\n")?;
        writer.promote()?;
        let modified = fs::metadata(source)?.modified()?;
        if source == &archive {
            fs::write(&pending, b"stale")?;
        }
        for input in [&path, &archive, &pending] {
            assert_eq!(
                crate::compression::file_modified_time(input).await?,
                Some(time::OffsetDateTime::from(modified)),
            );
            assert_eq!(
                crate::existing_rollout_path(input).await,
                Some(source.clone())
            );
        }
    }
    Ok(())
}

#[test]
fn cancellation_stops_compressed_input_and_output() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl.zst");
    let original = vec![b'x'; 1024 * 1024];
    let mut skippable = 0x184D2A50_u32.to_le_bytes().to_vec();
    skippable.extend((original.len() as u32).to_le_bytes());
    skippable.extend(&original);
    // Exercise many input reads with no output, and many output reads from little input.
    for bytes in [
        skippable,
        zstd::stream::encode_all(original.as_slice(), /*level*/ 3)?,
    ] {
        fs::write(&path, bytes)?;
        drop(open_rollout_seekable_reader(&path, || false)?);
        let checks = Cell::new(0);
        let cancelled = || checks.replace(checks.get() + 1) >= 10;
        assert!(open_rollout_seekable_reader(&path, cancelled).is_err());
        checks.set(0);
        assert!(rollout_contains_prefix(&path, original.len() as u64, cancelled).is_err());
    }
    Ok(())
}

#[test]
fn read_snapshots_preserve_bytes_and_report_cancellation() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let compressed = path.with_extension("jsonl.zst");
    let original = "first\r\nsecond 🦀\nthird\n".as_bytes();
    fs::write(
        &compressed,
        zstd::stream::encode_all(original, /*level*/ 3)?,
    )?;

    let cancelled = Cell::new(false);
    let is_cancelled = || cancelled.get();
    let mut reader = open_rollout_seekable_reader(&path, is_cancelled)?;
    // The retained snapshot must remain valid after another caller materializes the logical path.
    fs::write(&path, b"replacement\n")?;
    fs::remove_file(&compressed)?;
    reader.seek(SeekFrom::Start(7))?;
    let mut suffix = Vec::new();
    reader.read_to_end(&mut suffix)?;
    assert_eq!(suffix, original[7..]);

    let mut current = Vec::new();
    open_rollout_seekable_reader(&compressed, || false)?.read_to_end(&mut current)?;
    assert_eq!(current, b"replacement\n");

    let mut plain_snapshot = open_rollout_seekable_reader(&path, is_cancelled)?;
    fs::write(
        &compressed,
        zstd::stream::encode_all(current.as_slice(), /*level*/ 3)?,
    )?;
    fs::remove_file(&path)?;
    let mut retained = Vec::new();
    plain_snapshot.read_to_end(&mut retained)?;
    assert_eq!(retained, current);
    reader.rewind()?;
    plain_snapshot.rewind()?;
    cancelled.set(true);
    for mut reader in [reader, plain_snapshot] {
        assert_eq!(
            reader.read_to_end(&mut Vec::new()).unwrap_err().kind(),
            io::ErrorKind::Other
        );
        assert_eq!(reader.rewind().unwrap_err().kind(), io::ErrorKind::Other);
    }
    Ok(())
}

#[test]
fn prefix_bounds_support_sized_unsized_and_concatenated_frames() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl.zst");
    let first = "first 🦀\n".as_bytes();
    let second = b"second\n";
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), /*level*/ 3)?;
    encoder.set_pledged_src_size(Some(first.len() as u64))?;
    encoder.write_all(first)?;
    let sized = encoder.finish()?;
    let without_size = zstd::stream::encode_all(first, /*level*/ 3)?;
    let mut concatenated = sized.clone();
    concatenated.extend(zstd::stream::encode_all(
        second.as_slice(),
        /*level*/ 3,
    )?);

    for (compressed, expected) in [
        (sized, first.to_vec()),
        (without_size, first.to_vec()),
        (concatenated, [first, second].concat()),
    ] {
        fs::write(&path, compressed)?;
        let contains = |end| rollout_contains_prefix(&path, end, || false);
        assert!(contains(first.len() as u64)?);
        assert!(contains(expected.len() as u64)?);
        assert!(!contains(expected.len() as u64 + 1)?);
        let mut actual = Vec::new();
        open_rollout_seekable_reader(&path, || false)?.read_to_end(&mut actual)?;
        assert_eq!(actual, expected);
        assert!(!path.with_extension("").exists());
    }
    Ok(())
}
