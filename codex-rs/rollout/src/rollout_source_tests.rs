use std::fs;

use pretty_assertions::assert_eq;

use super::*;

#[tokio::test]
async fn framed_readers_preserve_logical_bytes() -> anyhow::Result<()> {
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
                    crate::read_rollout_prefix(input, offset)?,
                    Some(expected.as_bytes()[..offset].to_vec())
                );
                assert!(crate::rollout_contains_prefix(input, offset as u64)?);
            }
            assert!(!crate::rollout_contains_prefix(
                input,
                expected.len() as u64 + 1
            )?);
        }
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
    assert!(crate::read_rollout_prefix(&path, /*max_bytes*/ 100).is_err());
    assert!(crate::read_rollout_records_from(&path, /*start_offset*/ 0).is_err());
    assert!(crate::RolloutReverseScanner::open(&path).is_err());
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
async fn pending_without_a_complete_header_reads_like_an_empty_plain_rollout() -> anyhow::Result<()>
{
    // The writer creates the pending file before writing its header, so readers can observe it.
    let mut results = Vec::new();
    for (name, bytes) in [
        ("rollout.jsonl", [0; 0].as_slice()),
        (".pendings.rollout.seek.zst", &[0; 0]),
        (".pendings.rollout.seek.zst", &[0; 7]),
    ] {
        let home = tempfile::tempdir()?;
        let path = home.path().join("rollout.jsonl");
        fs::write(home.path().join(name), bytes)?;
        let mut lines = crate::open_rollout_line_reader(&path).await?;
        let mut scanner = crate::RolloutReverseScanner::open(&path)?;
        results.push((
            lines.next_line().await?,
            crate::read_rollout_records_from(&path, /*start_offset*/ 0)?,
            crate::read_rollout_records_from(&path, /*start_offset*/ 1)
                .unwrap_err()
                .kind(),
            crate::read_rollout_prefix(&path, /*max_bytes*/ 10)?,
            crate::rollout_contains_prefix(&path, /*end_byte_offset*/ 0)?,
            crate::rollout_contains_prefix(&path, /*end_byte_offset*/ 1)?,
            scanner.scan_next_rollout_line()?.is_none(),
        ));
    }
    assert_eq!(results[1], results[0]);
    assert_eq!(results[2], results[0]);
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

#[tokio::test]
async fn framed_lines_match_plain_lines_across_batches_and_invalid_utf8() -> anyhow::Result<()> {
    // Three records per frame: the lines span several batches and a short final frame.
    let records: [&[u8]; 8] = [
        b"one\n", b"two\r\n", b"\n", b"four\n", b"five\n", b"six\n", b"\xff\n", b"eight\n",
    ];
    let mut results = Vec::new();
    for framed in [false, true] {
        let home = tempfile::tempdir()?;
        let path = home.path().join("rollout.jsonl");
        if framed {
            let mut writer = crate::framed::FramedWriter::open(
                &path,
                std::num::NonZeroUsize::new(/*n*/ 3).unwrap(),
            )?;
            for record in records {
                writer.append(record)?;
                writer.promote()?;
            }
        } else {
            fs::write(&path, records.concat())?;
        }
        let mut lines = crate::open_rollout_line_reader(&path).await?;
        let mut actual = Vec::new();
        for _ in 0..9 {
            actual.push(lines.next_line().await.map_err(|error| error.kind()));
        }
        results.push(actual);
    }
    assert_eq!(results[1], results[0]);
    assert_eq!(results[0][6], Err(io::ErrorKind::InvalidData));
    Ok(())
}
