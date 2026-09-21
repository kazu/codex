use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::ThreadStore;
use crate::local::LocalThreadStore;
use crate::local::framed_test_support::convert_to_framed;
use crate::local::test_support::test_config;
use crate::local::test_support::write_archived_session_file;
use crate::local::test_support::write_session_file;

#[tokio::test]
async fn delete_thread_removes_framed_storage_like_a_plain_rollout() {
    for storage in ["plain", "archive", "pending", "archive and stale pending"] {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let cases = [
            (
                Uuid::from_u128(/*v*/ 401),
                write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(401))
                    .expect("session file"),
            ),
            (
                Uuid::from_u128(/*v*/ 402),
                write_archived_session_file(
                    home.path(),
                    "2025-01-03T12-00-00",
                    Uuid::from_u128(/*v*/ 402),
                )
                .expect("archived session file"),
            ),
        ];
        for (uuid, path) in cases {
            let files = if storage == "plain" {
                vec![path]
            } else {
                convert_to_framed(&path, storage)
            };
            let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");

            let deleted = store.delete_thread(DeleteThreadParams { thread_id }).await;
            let deleted_again = store.delete_thread(DeleteThreadParams { thread_id }).await;

            assert_eq!(
                (
                    deleted.is_ok(),
                    files.iter().filter(|file| file.exists()).count(),
                    matches!(deleted_again, Err(ThreadStoreError::ThreadNotFound { .. })),
                ),
                (true, 0, true),
                "{storage} {uuid}"
            );
        }
    }
}
