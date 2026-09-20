use pretty_assertions::assert_eq;

#[test]
fn recovery_command_recompresses_plain_zstd() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollout with spaces.seek.zst");
    let records = b"{}\n{\"index\":1}\n{\"index\":2}\n";
    let original = zstd::stream::encode_all(records.as_slice(), /*level*/ 0)?;
    std::fs::write(&path, &original)?;
    assert_cmd::Command::new(codex_utils_cargo_bin::cargo_bin("codex")?)
        .env("CODEX_HOME", dir.path())
        .args(["debug", "rollout-recovery"])
        .arg(&path)
        .args(["--records-per-frame", "2"])
        .assert()
        .success();
    let rebuilt = std::fs::read(&path)?;
    assert_ne!(rebuilt, original);
    assert_eq!(zstd::stream::decode_all(rebuilt.as_slice())?, records);
    Ok(())
}

#[test]
fn recovery_command_reports_errors_without_changing_the_archive() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("broken.seek.zst");
    for n in ["2", "0"] {
        let original = if n == "0" {
            zstd::stream::encode_all(b"{}\n".as_slice(), /*level*/ 0)?
        } else {
            b"not zstd".to_vec()
        };
        std::fs::write(&path, &original)?;
        let assertion = assert_cmd::Command::new(codex_utils_cargo_bin::cargo_bin("codex")?)
            .env("CODEX_HOME", dir.path())
            .args(["debug", "rollout-recovery"])
            .arg(&path)
            .args(["--records-per-frame", n])
            .assert()
            .failure();
        if n == "0" {
            assertion.stderr(predicates::str::contains("invalid value '0'"));
        }
        assert_eq!(std::fs::read(&path)?, original);
    }
    Ok(())
}

#[test]
fn recovery_command_help() -> anyhow::Result<()> {
    let output = std::process::Command::new(codex_utils_cargo_bin::cargo_bin("codex")?)
        .args(["debug", "rollout-recovery", "--help"])
        .output()?;
    assert!(output.status.success());
    insta::assert_snapshot!(String::from_utf8(output.stdout)?);
    Ok(())
}
