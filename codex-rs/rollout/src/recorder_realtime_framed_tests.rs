use super::*;
use pretty_assertions::assert_eq;

#[cfg(unix)]
#[tokio::test]
async fn framed_realtime_seal_retries_directory_sync() -> std::io::Result<()> {
    for frame_size in [1, 255] {
        let home = TempDir::new()?;
        let mut config = test_config(home.path());
        config.rollout_compression = std::num::NonZeroUsize::new(frame_size);
        let recorder = RolloutRecorder::new(
            &config,
            RolloutRecorderParams::new(
                ThreadId::new(),
                None,
                None,
                SessionSource::Exec,
                None,
                "test_originator".to_string(),
                BaseInstructions::default(),
                Vec::new(),
            ),
        )
        .await?;
        recorder
            .record_canonical_items(&[agent_message_item("typed")])
            .await?;
        recorder.flush().await?;
        let record = RolloutItem::RealtimeItem(codex_protocol::realtime::RealtimeItem {
            id: "opening".to_string(),
            realtime_session_id: "voice".to_string(),
            observed_at_ms: Some(1),
            content: codex_protocol::realtime::RealtimeItemContent::RealtimeSessionStarted,
        });
        recorder
            .record_canonical_items(std::slice::from_ref(&record))
            .await?;
        recorder.persist().await?;
        let parent = recorder.rollout_path().parent().expect("rollout directory");
        let moved = home.path().join("moved");
        fs::rename(parent, &moved)?;
        let error = recorder.flush().await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound, "{error:?}");
        fs::rename(&moved, parent)?;
        recorder.flush().await?;
        if frame_size == 255 {
            let archive = crate::framed::get_archive_path(recorder.rollout_path());
            assert!(!archive.exists());
            let items: Vec<_> = (0..255).map(|_| agent_message_item("promote")).collect();
            recorder.record_canonical_items(&items).await?;
            recorder.persist().await?;
            assert!(archive.exists());
            recorder.record_canonical_items(&[record]).await?;
            recorder.persist().await?;
            fs::rename(parent, &moved)?;
            let error = recorder.flush().await.unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::NotFound, "{error:?}");
            fs::rename(&moved, parent)?;
            recorder.flush().await?;
        }
        recorder.shutdown().await?;
    }
    Ok(())
}
