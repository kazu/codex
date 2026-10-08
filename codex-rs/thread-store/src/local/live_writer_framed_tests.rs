use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use crate::AppendThreadItemsParams;
use crate::ResumeThreadParams;
use crate::ThreadMetadataPatch;
use crate::ThreadPersistenceMetadata;
use crate::ThreadStore;
use crate::local::LocalThreadStore;
use crate::local::framed_test_support::convert_to_framed;
use crate::local::test_support::test_config;
use crate::local::test_support::write_session_file;

#[tokio::test]
async fn resumed_framed_thread_appends_and_shuts_down_like_a_plain_thread() {
    let mut results = Vec::new();
    for storage in ["plain", "pending", "archive"] {
        let home = TempDir::new().expect("temp dir");
        let uuid = Uuid::from_u128(/*v*/ 501);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
        let path = write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session");
        // Callers resume with the physical path a listing or lookup handed them.
        let resume_path = if storage == "plain" {
            path.clone()
        } else {
            convert_to_framed(&path, storage).remove(0)
        };
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(resume_path),
                history: None,
                history_revision: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "test-provider".into(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .expect("resume thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![RolloutItem::EventMsg(EventMsg::UserMessage(
                    UserMessageEvent {
                        message: "appended after resume".to_string(),
                        ..Default::default()
                    },
                ))],
            })
            .await
            .expect("append items");
        store
            .pending_thread_metadata
            .stage(
                thread_id,
                ThreadMetadataPatch {
                    memory_mode: Some(ThreadMemoryMode::Disabled),
                    ..Default::default()
                },
            )
            .await
            .expect("stage pending metadata");

        store.shutdown_thread(thread_id).await.expect("shutdown");

        // The rollout still exists after shutdown, so its pending metadata must survive.
        let pending_metadata_kept = store
            .pending_thread_metadata
            .lock(thread_id)
            .await
            .is_some();
        let (items, _, parse_errors) = RolloutRecorder::load_rollout_items(&path)
            .await
            .expect("load rollout items");
        assert_eq!(path.exists(), storage == "plain", "{storage}");
        results.push((
            pending_metadata_kept,
            items.len(),
            serde_json::to_value(items.last()).expect("serialize last item"),
            parse_errors,
        ));
    }
    assert_eq!(results[1], results[0]);
    assert_eq!(results[2], results[0]);
    assert!(results[0].0);
}
