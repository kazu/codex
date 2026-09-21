use codex_protocol::ThreadId;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use crate::ArchiveThreadParams;
use crate::ThreadStore;
use crate::local::LocalThreadStore;
use crate::local::framed_test_support::convert_to_framed;
use crate::local::test_support::test_config;
use crate::local::test_support::write_session_file;

const STORAGES: [&str; 4] = ["plain", "pending", "archive", "archive and stale pending"];

fn files_in(dir: &std::path::Path) -> Vec<String> {
    let mut names = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            if entry.path().is_dir() {
                dirs.push(entry.path());
            } else {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    names.sort();
    names
}

#[tokio::test]
async fn archive_and_unarchive_move_every_framed_file_like_a_plain_rollout() {
    let mut results = Vec::new();
    for storage in STORAGES {
        let home = TempDir::new().expect("temp dir");
        let uuid = Uuid::from_u128(/*v*/ 601);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
        let path = write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session");
        let mut file_names = vec![path.file_name().unwrap().to_string_lossy().into_owned()];
        if storage != "plain" {
            file_names = convert_to_framed(&path, storage)
                .iter()
                .map(|file| file.file_name().unwrap().to_string_lossy().into_owned())
                .collect();
        }
        file_names.sort();
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
        let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);

        let archive_result = store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await;
        let after_archive = (files_in(&sessions), files_in(&archived));
        let unarchive_result = store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await;
        let after_unarchive = (files_in(&sessions), files_in(&archived));
        let (items, _, parse_errors) = RolloutRecorder::load_rollout_items(&path)
            .await
            .expect("load restored rollout");

        assert_eq!(
            (after_archive, after_unarchive),
            ((Vec::new(), file_names.clone()), (file_names, Vec::new())),
            "{storage}"
        );
        assert_eq!(path.parent().map(std::path::Path::exists), Some(true));
        results.push((
            archive_result.is_ok(),
            unarchive_result.is_ok(),
            items.len(),
            parse_errors,
        ));
    }
    for result in &results[1..] {
        assert_eq!(result, &results[0]);
    }
    assert_eq!((results[0].0, results[0].1), (true, true));
}

#[tokio::test]
async fn failed_framed_archive_restores_the_files_already_moved() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 602);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session");
    let files = convert_to_framed(&path, "archive and stale pending");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    // The stale pending file moves first. A non-empty directory at the archive file's
    // destination then makes that rename fail, so the pending file has to be moved back.
    let blocked = home
        .path()
        .join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR)
        .join(files[0].file_name().unwrap());
    std::fs::create_dir_all(blocked.join("occupied")).expect("block destination");

    let result = store
        .archive_thread(ArchiveThreadParams { thread_id })
        .await;

    assert!(result.is_err());
    assert!(files.iter().all(|file| file.is_file()));
}
