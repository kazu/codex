use super::*;
use pretty_assertions::assert_eq;

fn archive_records(path: &Path) -> anyhow::Result<(usize, Vec<u8>)> {
    let mut reader = RecordReader::open(path.with_extension("seek.zst"), b"\n")?;
    let count = reader.total_records()?;
    let mut bytes = Vec::new();
    reader.records_to(/*from*/ 0, count, &mut bytes)?;
    Ok((reader.records_per_frame(), bytes))
}

#[test]
fn appends_directly_after_the_first_archive() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollout.jsonl");
    let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 255).unwrap())?;
    let mut expected = Vec::new();
    for index in 0..255 {
        let line = format!("{{\"index\":{index}}}\n");
        writer.append(line.as_bytes())?;
        expected.extend_from_slice(line.as_bytes());
    }
    writer.promote()?;
    assert!(!get_pending_path(&path)?.exists());
    for index in 255..513 {
        let line = format!("{{\"index\":{index}}}\n");
        writer.append(line.as_bytes())?;
        expected.extend_from_slice(line.as_bytes());
        assert_eq!(archive_records(&path)?, (255, expected.clone()));
        if index == 255 {
            drop(writer);
            writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 7).unwrap())?;
        }
    }
    writer.promote()?;
    assert_eq!(archive_records(&path)?, (255, expected));
    assert!(!get_pending_path(&path)?.exists());
    Ok(())
}

#[test]
fn keeps_pending_records_until_the_first_full_frame() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollout.jsonl");
    let pending = dir.path().join(".pendings.rollout.seek.zst");
    let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 3).unwrap())?;
    let mut expected = 0_u64.to_le_bytes().to_vec();
    assert_eq!(std::fs::read(&pending)?, expected);
    for _ in 0..2 {
        writer.append(b"{}\n")?;
        expected.extend_from_slice(b"{}\n");
        writer.promote()?;
        assert!(!path.with_extension("seek.zst").exists());
        assert_eq!(std::fs::read(&pending)?, expected);
        drop(writer);
        writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 3).unwrap())?;
    }
    writer.append(b"{}\n")?;
    writer.promote()?;
    assert_eq!(archive_records(&path)?, (3, b"{}\n{}\n{}\n".to_vec()));
    assert!(!pending.exists());
    Ok(())
}

#[test]
fn promotes_all_pending_records_including_the_remainder() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    for n in [1, 2, 7, 255] {
        let path = dir.path().join(format!("rollout-{n}.jsonl"));
        let mut writer = FramedWriter::open(&path, NonZeroUsize::new(n).unwrap())?;
        let mut expected = Vec::new();
        for index in 0..2 * n + 1 {
            let line = format!("{{\"index\":{index}}}\n");
            writer.append(line.as_bytes())?;
            expected.extend_from_slice(line.as_bytes());
        }
        writer.promote()?;
        assert_eq!(archive_records(&path)?, (n, expected.clone()));
        drop(writer);
        let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 9).unwrap())?;
        writer.append(b"{}\n")?;
        expected.extend_from_slice(b"{}\n");
        assert_eq!(archive_records(&path)?, (n, expected));
        assert!(!get_pending_path(&path)?.exists());
    }
    Ok(())
}

#[test]
fn stores_records_larger_than_the_compressor_chunk() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("large.jsonl");
    let line = format!("\"{}\"\n", "x".repeat(3 * 1024 * 1024));
    let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 1).unwrap())?;
    writer.append(line.as_bytes())?;
    writer.promote()?;
    writer.append(line.as_bytes())?;
    assert_eq!(
        archive_records(&path)?,
        (1, line.repeat(/*n*/ 2).into_bytes())
    );
    Ok(())
}

#[test]
fn uses_the_archive_after_pending_cleanup_fails() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollout.jsonl");
    let pending = get_pending_path(&path)?;
    let saved = dir.path().join("saved-pendings");
    let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 2).unwrap())?;
    writer.append(b"{}\n")?;
    writer.append(b"{}\n")?;
    std::fs::rename(&pending, &saved)?;
    std::fs::create_dir(&pending)?;
    assert!(writer.promote().is_err());
    writer.append(b"{\"next\":1}\n")?;
    writer.promote()?;
    assert_eq!(
        archive_records(&path)?,
        (2, b"{}\n{}\n{\"next\":1}\n".to_vec())
    );
    drop(writer);

    std::fs::remove_dir(&pending)?;
    std::fs::rename(saved, &pending)?;
    let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 9).unwrap())?;
    writer.append(b"{\"next\":2}\n")?;
    writer.promote()?;
    assert_eq!(
        archive_records(&path)?,
        (2, b"{}\n{}\n{\"next\":1}\n{\"next\":2}\n".to_vec())
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn retries_initial_compression_after_a_real_write_error() -> anyhow::Result<()> {
    const CHILD: &str = "CODEX_FRAMED_INITIAL_FAILURE_DIR";
    if let Some(dir) = std::env::var_os(CHILD) {
        let path = Path::new(&dir).join("rollout.jsonl");
        let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 2).unwrap())?;
        let pending = get_pending_path(&path)?;
        let before = std::fs::read(&pending)?;
        // Only this dedicated subprocess has its file-size limit changed.
        let mut original = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        unsafe {
            assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut original), 0);
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        }
        let limit = libc::rlimit {
            rlim_cur: 32,
            rlim_max: original.rlim_max,
        };
        unsafe {
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &limit), 0);
        }
        let result = writer.promote();
        unsafe {
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &original), 0);
        }
        assert!(result.is_err());
        assert!(!path.with_extension("seek.zst").exists());
        assert_eq!(std::fs::read(&pending)?, before);
        writer.promote()?;
        assert_eq!(archive_records(&path)?, (2, before[8..].to_vec()));
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollout.jsonl");
    let mut writer = FramedWriter::open(&path, NonZeroUsize::new(/*n*/ 2).unwrap())?;
    for index in 0..2 {
        writer.append(
            format!("{{\"index\":{index},\"text\":\"abcdefghijklmnopqrstuvwxyz0123456789\"}}\n")
                .as_bytes(),
        )?;
    }
    drop(writer);
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "framed::tests::retries_initial_compression_after_a_real_write_error",
            "--nocapture",
        ])
        .env(CHILD, dir.path())
        .output()?;
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!get_pending_path(&path)?.exists());
    Ok(())
}
