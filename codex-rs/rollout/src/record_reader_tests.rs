use std::fs;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::Duration;

use pretty_assertions::assert_eq;

use super::*;

fn compacted_record(message: &str) -> String {
    let line = RolloutLine {
        timestamp: "2026-09-21T00:00:00Z".to_owned(),
        ordinal: None,
        item: crate::RolloutItem::Compacted(Box::new(crate::CompactedItem {
            message: message.to_owned(),
            replacement_history: None,
            guardian_history: None,
            retained_context: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
        })),
    };
    format!("{}\n", serde_json::to_string(&line).unwrap())
}

#[test]
fn readers_keep_the_last_frame_when_a_writer_appends() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollout.jsonl");
    let mut writer = crate::framed::FramedWriter::open(&path, NonZeroUsize::new(2).unwrap())?;
    let records: Vec<_> = ["one", "two", "three", "four"]
        .into_iter()
        .map(compacted_record)
        .collect();
    for record in &records[..3] {
        writer.append(record.as_bytes())?;
        writer.promote()?;
    }

    let (forward, endpoint) = open_rollout_projection_reader(&path, 0, || false)?;
    let reverse = RolloutReverseScanner::open(&path)?;
    let archive = crate::framed::get_archive_path(&path);
    let (forward_archive, endpoint_archive) =
        open_rollout_projection_reader(&archive, 0, || false)?;
    let reverse_archive = RolloutReverseScanner::open(&archive)?;
    writer.append(records[3].as_bytes())?;

    for (mut forward, mut reverse, endpoint) in [
        (forward, reverse, endpoint),
        (forward_archive, reverse_archive, endpoint_archive),
    ] {
        let mut bytes = Vec::new();
        forward.read_to_end(&mut bytes)?;
        assert_eq!(bytes, records[..3].concat().as_bytes());
        assert_eq!(endpoint, bytes.len() as u64);
        let mut reversed = Vec::new();
        while let Some(outcome) = reverse.scan_next_rollout_line()? {
            if let ScanOutcome::Parsed(line) = outcome {
                reversed.push(line);
            }
        }
        assert_eq!(reversed.len(), 3);
        assert_eq!(
            serde_json::to_value(&reversed[0])?,
            serde_json::from_str::<serde_json::Value>(&records[2])?
        );
    }
    Ok(())
}

#[test]
fn opening_a_reader_waits_for_an_active_frame_edit() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollout.jsonl");
    let mut writer = crate::framed::FramedWriter::open(&path, NonZeroUsize::new(2).unwrap())?;
    writer.append(compacted_record("one").as_bytes())?;
    writer.append(compacted_record("two").as_bytes())?;
    writer.promote()?;
    let lock = crate::framed::archive_frame_lock(&path, true)?;
    let archive = crate::framed::get_archive_path(&path);
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        finished_tx
            .send(RolloutReverseScanner::open(&archive).is_ok())
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2))?;
    assert!(finished_rx.recv_timeout(Duration::from_millis(50)).is_err());
    drop(lock);
    assert!(finished_rx.recv_timeout(Duration::from_secs(2))?);
    reader.join().unwrap();
    Ok(())
}

fn record_message(outcome: ScanOutcome<RolloutLine>) -> String {
    match outcome {
        ScanOutcome::Parsed(RolloutLine {
            item: crate::RolloutItem::Compacted(item),
            ..
        }) => item.message,
        ScanOutcome::Parsed(_) => panic!("expected compacted fixture"),
        ScanOutcome::Rejected(error) => panic!("invalid compacted fixture: {error}"),
    }
}

#[test]
fn records_preserve_frozen_record_boundaries_across_formats() -> anyhow::Result<()> {
    let lines = [
        compacted_record("first").replace('\n', "\r\n"),
        "\n".to_owned(),
        "{broken}\n".to_owned(),
        compacted_record("last 🦀"),
        compacted_record("tail"),
    ];
    let body = lines.concat();
    for format in ["plain", "legacy", "pending", "framed"] {
        let home = tempfile::tempdir()?;
        let path = home.path().join("rollout.jsonl");
        match format {
            "plain" => fs::write(&path, &body)?,
            "legacy" => fs::write(
                path.with_extension("jsonl.zst"),
                zstd::stream::encode_all(body.as_bytes(), /*level*/ 3)?,
            )?,
            "pending" | "framed" => {
                let count = if format == "pending" { 6 } else { 2 };
                let mut writer =
                    crate::framed::FramedWriter::open(&path, NonZeroUsize::new(count).unwrap())?;
                for line in &lines {
                    writer.append(line.as_bytes())?;
                    writer.promote()?;
                }
            }
            _ => unreachable!(),
        }
        // Reverse scans retain byte positions inside a record. Projection reads start at records.
        for end in 0..=body.len() {
            if end == 0 || body.as_bytes()[end - 1] == b'\n' {
                assert_eq!(
                    read_rollout_records_from(&path, end as u64)?,
                    body.as_bytes()[end..]
                );
            }
            let mut actual = RolloutReverseScanner::open_at(&path, end as u64)?;
            let mut expected =
                ReverseJsonlScanner::new(std::io::Cursor::new(&body.as_bytes()[..end]))?;
            loop {
                let normalize = |value| match value {
                    ScanOutcome::Parsed(value) => Ok(serde_json::to_value(value).unwrap()),
                    ScanOutcome::Rejected(_) => Err(()),
                };
                let actual = actual.scan_next_rollout_line()?.map(normalize);
                let expected = expected.scan_next_rollout_line()?.map(normalize);
                assert_eq!(actual, expected, "{format} at {end}");
                if actual.is_none() {
                    break;
                }
            }
        }
        for start in [0, lines[0].len(), body.len()] {
            let (mut reader, end) = open_rollout_projection_reader(&path, start as u64, || false)?;
            assert_eq!(end, body.len() as u64, "{format}");
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual)?;
            assert_eq!(actual, body.as_bytes()[start..], "{format} at {start}");
        }
        assert!(open_rollout_projection_reader(&path, 0, || true).is_err());
        assert!(RolloutReverseScanner::open_at(&path, body.len() as u64 + 1).is_err());
        assert!(read_rollout_records_from(&path, body.len() as u64 + 1).is_err());
    }
    Ok(())
}

#[test]
fn reverse_records_cross_cached_windows_and_short_final_frames() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let expected: Vec<String> = (0..511)
        .map(|index| format!("{index}:{}", "x".repeat(/*n*/ 512)))
        .collect();
    let mut writer =
        crate::framed::FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 255).unwrap())?;
    for value in &expected {
        writer.append(compacted_record(value).as_bytes())?;
        writer.promote()?;
    }
    drop(writer);
    let mut scanner = RolloutReverseScanner::open(&path)?;
    let mut actual = Vec::new();
    while let Some(record) = scanner.scan_next_rollout_line()? {
        actual.push(record_message(record));
    }
    assert_eq!(actual, expected.into_iter().rev().collect::<Vec<_>>());
    Ok(())
}

#[test]
fn reverse_pending_handle_keeps_its_prefix_after_publication() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let mut writer = crate::framed::FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 2).unwrap())?;
    writer.append(compacted_record("first").as_bytes())?;
    let mut scanner = RolloutReverseScanner::open(&path)?;
    writer.append(compacted_record("second").as_bytes())?;
    writer.promote()?;
    assert_eq!(
        scanner.scan_next_rollout_line()?.map(record_message),
        Some("first".to_owned())
    );
    assert!(scanner.scan_next_rollout_line()?.is_none());
    Ok(())
}

#[test]
fn forward_defers_partial_records_and_reverse_retains_selected_files() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let legacy = path.with_extension("jsonl.zst");
    let first = compacted_record("first").replace('\n', "\r\n");
    let body = format!("{first}{}", compacted_record("last").trim_end());
    fs::write(
        &legacy,
        zstd::stream::encode_all(body.as_bytes(), /*level*/ 3)?,
    )?;
    assert_eq!(
        read_rollout_records_from(&path, /*start_offset*/ 0)?,
        first.as_bytes()
    );
    let mut retained = RolloutReverseScanner::open(&path)?;
    fs::write(&path, compacted_record("replacement"))?;
    fs::remove_file(&legacy)?;
    assert_eq!(
        retained.scan_next_rollout_line()?.map(record_message),
        Some("last".to_owned())
    );
    assert_eq!(
        retained.scan_next_rollout_line()?.map(record_message),
        Some("first".to_owned())
    );
    assert!(retained.scan_next_rollout_line()?.is_none());
    let mut plain = RolloutReverseScanner::open(&legacy)?;
    fs::write(
        &legacy,
        zstd::stream::encode_all(
            compacted_record("different fallback").as_bytes(),
            /*level*/ 3,
        )?,
    )?;
    fs::remove_file(&path)?;
    assert_eq!(
        plain.scan_next_rollout_line()?.map(record_message),
        Some("replacement".to_owned())
    );
    assert!(plain.scan_next_rollout_line()?.is_none());
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn record_suffix_lookup_avoids_decoded_temporary_writes() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let record = compacted_record(&"x".repeat(/*n*/ 8192));
    let mut writer =
        crate::framed::FramedWriter::open(&path, std::num::NonZeroUsize::new(/*n*/ 4).unwrap())?;
    for _ in 0..128 {
        writer.append(record.as_bytes())?;
        writer.promote()?;
    }
    drop(writer);
    let written = || -> anyhow::Result<u64> {
        let io = fs::read_to_string("/proc/thread-self/io")?;
        Ok(io
            .lines()
            .find_map(|line| line.strip_prefix("wchar: "))
            .unwrap()
            .parse()?)
    };
    let before = written()?;
    let mut scanner = crate::RolloutReverseScanner::open(&path)?;
    assert_eq!(
        scanner.scan_next_rollout_line()?.map(record_message),
        Some("x".repeat(/*n*/ 8192))
    );
    let copied = written()? - before;
    eprintln!(
        "record suffix lookup: {} logical bytes, {copied} bytes written",
        record.len() * 128
    );
    assert_eq!(copied, 0);
    Ok(())
}

#[test]
fn reverse_record_lookup_does_not_decode_unrequested_middle_frame() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let archive = path.with_extension("jsonl.seek.zst");
    let mut writer =
        crate::framed::FramedWriter::open(&path, std::num::NonZeroUsize::new(/*n*/ 1).unwrap())?;
    let records = [
        compacted_record("first"),
        compacted_record("middle"),
        compacted_record("last"),
    ];
    for record in &records {
        writer.append(record.as_bytes())?;
        writer.promote()?;
    }
    drop(writer);
    let mut bytes = fs::read(&archive)?;
    let middle = zstd::zstd_safe::find_frame_compressed_size(&bytes)
        .map_err(|code| anyhow::anyhow!("invalid fixture frame: {code}"))?;
    bytes[middle] ^= 0xff;
    fs::write(&archive, bytes)?;
    assert_eq!(
        read_rollout_records_from(&path, (records[0].len() + records[1].len()) as u64)?,
        records[2].as_bytes()
    );
    let mut scanner = crate::RolloutReverseScanner::open(&path)?;
    assert_eq!(
        scanner.scan_next_rollout_line()?.map(record_message),
        Some("last".to_owned())
    );
    assert!(scanner.scan_next_rollout_line().is_err());
    Ok(())
}
