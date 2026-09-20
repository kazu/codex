use std::fs;
use std::io::Write;

use pretty_assertions::assert_eq;

use super::*;

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
            assert_eq!(
                crate::read_rollout_records_from(input, /*start_offset*/ 0)?,
                expected.as_bytes()
            );
            for offset in 0..=expected.len() {
                if offset == 0 || expected.as_bytes()[offset - 1] == b'\n' {
                    let suffix = crate::read_rollout_records_from(input, offset as u64)?;
                    assert_eq!(suffix, expected.as_bytes()[offset..]);
                }
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
    assert!(crate::read_rollout_records_from(&path, /*start_offset*/ 0).is_err());
    assert!(crate::RolloutReverseScanner::open(&path).is_err());
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
    writer.append(b"second\n")?;
    writer.promote()?;
    drop(writer);
    assert_eq!(lines.next_line().await?, Some("first".to_owned()));
    assert_eq!(lines.next_line().await?, Some("second".to_owned()));
    assert_eq!(lines.next_line().await?, None);
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
        crate::read_rollout_records_from(&path, /*start_offset*/ 0)
            .unwrap_err()
            .kind(),
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
        assert!(rollout_contains_prefix(&path, first.len() as u64)?);
        assert!(rollout_contains_prefix(&path, expected.len() as u64)?);
        assert!(!rollout_contains_prefix(&path, expected.len() as u64 + 1)?);
        let actual = crate::read_rollout_records_from(&path, /*start_offset*/ 0)?;
        assert_eq!(actual, expected);
        assert!(!path.with_extension("").exists());
    }
    Ok(())
}
