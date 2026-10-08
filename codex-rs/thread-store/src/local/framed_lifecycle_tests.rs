//! Drives one thread through the store with compression on and off and compares the outcomes.

use std::num::NonZeroUsize;

use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::ThreadPersistenceMetadata;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::local::LocalThreadStore;
use crate::local::test_support::test_config;

fn user_message(message: String) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message,
        ..Default::default()
    }))
}

fn metadata(home: &std::path::Path) -> ThreadPersistenceMetadata {
    ThreadPersistenceMetadata {
        cwd: Some(home.to_path_buf()),
        model_provider: "test-provider".to_string(),
        memory_mode: ThreadMemoryMode::Enabled,
    }
}

async fn history_len(store: &LocalThreadStore, thread_id: ThreadId, archived: bool) -> usize {
    store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: archived,
            include_history: true,
        })
        .await
        .expect("read thread")
        .history
        .expect("history")
        .items
        .len()
}

#[tokio::test]
async fn framed_thread_lives_through_the_store_like_a_plain_thread() {
    let mut results = Vec::new();
    // Three records per frame: the first session publishes an archive with a short final frame.
    for compression in [None, NonZeroUsize::new(/*n*/ 3)] {
        let home = TempDir::new().expect("temp dir");
        let mut config = test_config(home.path());
        config.rollout_compression = compression;
        let store = LocalThreadStore::new(config, /*state_db*/ None);
        let thread_id = ThreadId::new();
        store
            .create_thread(CreateThreadParams {
                creator_user_id: None,
                creator_account_id: None,
                session_id: thread_id.into(),
                thread_id,
                extra_config: None,
                forked_from_id: None,
                parent_thread_id: None,
                source: SessionSource::Cli,
                thread_source: None,
                originator: "test_originator".to_string(),
                base_instructions: BaseInstructions::default(),
                dynamic_tools: Vec::new(),
                selected_capability_roots: Vec::new(),
                multi_agent_version: None,
                history_mode: ThreadHistoryMode::Legacy,
                history_base: None,
                subagent_history_start_ordinal: None,
                initial_window_id: uuid::Uuid::now_v7().to_string(),
                runtime_workspace_roots: None,
                metadata: metadata(home.path()),
            })
            .await
            .expect("create thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: (0..4)
                    .map(|n| user_message(format!("first session {n}")))
                    .collect(),
            })
            .await
            .expect("append first session");
        store.shutdown_thread(thread_id).await.expect("shutdown");
        let after_first_session = history_len(&store, thread_id, /*archived*/ false).await;

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: None,
                history: None,
                history_revision: None,
                include_archived: false,
                metadata: metadata(home.path()),
            })
            .await
            .expect("resume thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message("second session".to_string())],
            })
            .await
            .expect("append second session");
        store.shutdown_thread(thread_id).await.expect("shutdown");
        let after_second_session = history_len(&store, thread_id, /*archived*/ false).await;

        store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("archive");
        let while_archived = history_len(&store, thread_id, /*archived*/ true).await;
        store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("unarchive");
        let after_unarchive = history_len(&store, thread_id, /*archived*/ false).await;

        store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect("delete");
        let read_after_delete = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await;
        let mut left_behind = Vec::new();
        let mut dirs = vec![home.path().join(codex_rollout::SESSIONS_SUBDIR)];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                if entry.path().is_dir() {
                    dirs.push(entry.path());
                } else {
                    left_behind.push(entry.file_name());
                }
            }
        }

        results.push((
            after_first_session,
            after_second_session,
            while_archived,
            after_unarchive,
            matches!(
                read_after_delete,
                Err(ThreadStoreError::ThreadNotFound { .. }
                    | ThreadStoreError::InvalidRequest { .. })
            ),
            left_behind,
        ));
    }
    assert_eq!(results[1], results[0]);
    assert_eq!(results[0].1, results[0].0 + 1);
}
