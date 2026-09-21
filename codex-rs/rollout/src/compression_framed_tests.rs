use std::num::NonZeroUsize;

use pretty_assertions::assert_eq;

use super::*;

#[tokio::test]
async fn worker_leaves_framed_storage_and_still_compresses_plain_rollouts() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let plain_uuid = Uuid::from_u128(/*v*/ 31);
    let plain_path = archived_rollout_path(home.path(), "2025-01-03T12-00-00", plain_uuid);
    write_rollout(
        &plain_path,
        ThreadId::from_string(&plain_uuid.to_string())?,
        "cold plain rollout",
    )?;
    set_old_mtime(&plain_path)?;

    let mut framed_files = Vec::new();
    for (v, records_per_frame) in [(32, 1), (33, 10)] {
        let uuid = Uuid::from_u128(v);
        let path = archived_rollout_path(home.path(), "2025-01-03T12-00-00", uuid);
        write_rollout(&path, ThreadId::from_string(&uuid.to_string())?, "framed")?;
        let lines = fs::read_to_string(&path)?;
        fs::remove_file(&path)?;
        let mut writer = crate::framed::FramedWriter::open(
            &path,
            NonZeroUsize::new(records_per_frame).unwrap(),
        )?;
        for line in lines.split_inclusive('\n') {
            writer.append(line.as_bytes())?;
            writer.promote()?;
        }
        drop(writer);
        let file = crate::existing_rollout_path(&path)
            .await
            .expect("framed storage");
        set_old_mtime(&file)?;
        framed_files.push((path, file.clone(), fs::read(&file)?));
    }

    worker::run(home.path().to_path_buf()).await?;

    assert!(!plain_path.exists());
    assert!(compressed_rollout_path(&plain_path).exists());
    for (path, file, bytes) in framed_files {
        assert_eq!(
            (
                fs::read(&file)?,
                path.exists(),
                compressed_rollout_path(&path).exists()
            ),
            (bytes, false, false),
            "{}",
            file.display()
        );
    }
    Ok(())
}
