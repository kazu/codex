use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::UserMessageEvent;
use pretty_assertions::assert_eq;
use uuid::Uuid;

use crate::RolloutItem;
use crate::RolloutLine;
use crate::ThreadSortKey;
use crate::compression::RolloutFile;
use crate::framed::FramedWriter;

fn rollout_lines(thread_id: ThreadId, cwd: &Path, message: &str) -> Vec<String> {
    [
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                cwd: cwd.to_path_buf(),
                originator: "test".to_string(),
                cli_version: "test".to_string(),
                source: SessionSource::Cli,
                ..Default::default()
            },
            git: None,
        }),
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: message.to_string(),
            ..Default::default()
        })),
    ]
    .into_iter()
    .map(|item| {
        let line = RolloutLine {
            timestamp: "2025-01-03T12:00:01Z".to_string(),
            ordinal: None,
            item,
        };
        format!("{}\n", serde_json::to_string(&line).unwrap())
    })
    .collect()
}

/// Writes the rollout as framed storage and returns the physical file readers must resolve to.
fn write_framed(path: &Path, lines: &[String], storage: &str) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(path.parent().unwrap())?;
    let records_per_frame = if storage == "pending" { 10 } else { 1 };
    let mut writer = FramedWriter::open(path, NonZeroUsize::new(records_per_frame).unwrap())?;
    for line in lines {
        writer.append(line.as_bytes())?;
        writer.promote()?;
    }
    let pending = crate::framed::get_pending_path(path)?;
    if storage == "archive and stale pending" {
        fs::write(
            &pending,
            [0u64.to_le_bytes().as_slice(), b"stale\n"].concat(),
        )?;
    }
    Ok(if storage == "pending" {
        pending
    } else {
        crate::framed::get_archive_path(path)
    })
}

#[tokio::test]
async fn framed_storage_is_discovered_as_one_logical_rollout() -> anyhow::Result<()> {
    for storage in ["pending", "archive", "archive and stale pending"] {
        let home = tempfile::tempdir()?;
        let uuid = Uuid::from_u128(/*v*/ 21);
        let thread_id = ThreadId::from_string(&uuid.to_string())?;
        let file_name = format!("rollout-2025-01-03T12-00-00-{uuid}.jsonl");
        let path = home.path().join("sessions/2025/01/03").join(&file_name);
        let lines = rollout_lines(thread_id, home.path(), "targeted search term");
        let physical = write_framed(&path, &lines, storage)?;

        let rollout_file = RolloutFile::from_path(physical.clone()).expect("framed rollout file");
        assert_eq!(
            (
                rollout_file.path(),
                rollout_file.plain_file_name(),
                // The cold compression worker and the search walkers skip or decode on this.
                rollout_file.is_compressed(),
            ),
            (physical.as_path(), file_name.as_str(), true),
            "{storage}"
        );
        let stale = crate::framed::get_pending_path(&path)?;
        if stale != physical {
            assert_eq!(RolloutFile::from_path(stale), None, "{storage}");
        }
        assert_eq!(
            crate::rollout_id_from_path(&physical),
            crate::rollout_id_from_path(&path),
            "{storage}"
        );
        assert!(crate::rollout_id_from_path(&physical).is_some());

        assert_eq!(
            crate::find_thread_path_by_id_str(
                home.path(),
                &uuid.to_string(),
                /*state_db_ctx*/ None
            )
            .await?,
            Some(physical.clone()),
            "{storage}"
        );
        let page = crate::get_threads(
            home.path(),
            /*page_size*/ 10,
            /*cursor*/ None,
            ThreadSortKey::UpdatedAt,
            &[],
            /*model_providers*/ None,
            /*cwd_filters*/ None,
            "test-provider",
        )
        .await?;
        assert_eq!(
            page.items
                .iter()
                .map(|item| (item.path.clone(), item.thread_id))
                .collect::<Vec<_>>(),
            vec![(physical.clone(), Some(thread_id))],
            "{storage}"
        );

        let matches = crate::search_rollout_matches(
            Path::new("missing-rg-for-test"),
            home.path(),
            /*archived*/ false,
            "search term",
        )
        .await?;
        assert_eq!(
            matches.into_iter().collect::<Vec<_>>(),
            vec![(path.clone(), Some("targeted search term".to_string()))],
            "{storage}"
        );

        // A legacy zstd file, then a plain file, takes precedence over framed storage.
        let legacy = path.with_extension("jsonl.zst");
        fs::write(
            &legacy,
            zstd::stream::encode_all(lines.concat().as_bytes(), /*level*/ 3)?,
        )?;
        assert_eq!(RolloutFile::from_path(physical.clone()), None, "{storage}");
        assert_eq!(
            crate::find_thread_path_by_id_str(
                home.path(),
                &uuid.to_string(),
                /*state_db_ctx*/ None
            )
            .await?,
            Some(legacy.clone()),
            "{storage}"
        );
        fs::write(&path, lines.concat())?;
        assert_eq!(RolloutFile::from_path(physical), None, "{storage}");
        assert_eq!(RolloutFile::from_path(legacy), None, "{storage}");
    }
    Ok(())
}

#[test]
fn logical_path_of_framed_changes_only_framed_names() {
    let dir = Path::new("/tmp/sessions/2025/01/03");
    let paths = [
        "rollout-a.jsonl",
        "rollout-a.jsonl.zst",
        "rollout-a.seek.zst",
        ".pendings.rollout-a.seek.zst",
    ]
    .map(|name| crate::logical_path_of_framed(&dir.join(name)));

    assert_eq!(
        paths,
        [
            dir.join("rollout-a.jsonl"),
            dir.join("rollout-a.jsonl.zst"),
            dir.join("rollout-a.jsonl"),
            dir.join("rollout-a.jsonl"),
        ]
    );
}
