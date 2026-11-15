use std::num::NonZeroUsize;

use pretty_assertions::assert_eq;

use super::*;

#[tokio::test]
async fn reconcile_search_metadata_matches_plain_and_framed_paths() -> anyhow::Result<()> {
    let mut titles = Vec::new();
    for storage in ["plain", "pending", "archive"] {
        let home = TempDir::new()?;
        let uuid = Uuid::from_u128(9014);
        let thread_id = ThreadId::from_string(&uuid.to_string())?;
        let path = write_session_file(home.path(), "2025-01-03T15-00-00", uuid)?;
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            codex_state::SqliteConfig::new_for_testing(home.path().abs()),
            config.model_provider_id.clone(),
        )
        .await?;
        let mut metadata = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            path.clone(),
            chrono::Utc::now(),
            SessionSource::Cli,
        )
        .with_model_provider(Some(config.model_provider_id.clone()))
        .with_cwd(home.path().to_path_buf())
        .build(&config.model_provider_id);
        metadata.title = "stale title".to_owned();
        metadata.first_user_message = Some(metadata.title.clone());
        metadata.preview = metadata.first_user_message.clone();
        runtime.upsert_thread(&metadata).await?;
        if storage != "plain" {
            convert_to_framed(&path, storage)?;
        }
        let stored = crate::existing_rollout_path(&path).await.unwrap();
        crate::state_db::reconcile_rollout_from_disk(
            &runtime,
            &stored,
            &config.model_provider_id,
            Some(false),
            Some(home.path()),
        )
        .await;
        let updated = runtime.get_thread(thread_id).await?.unwrap();
        assert_eq!(updated.rollout_path, path);
        titles.push(updated.title);
        metadata.rollout_path = home.path().join("another-rollout.jsonl");
        runtime.upsert_thread(&metadata).await?;
        crate::state_db::reconcile_rollout_from_disk(
            &runtime,
            &stored,
            &config.model_provider_id,
            Some(false),
            Some(home.path()),
        )
        .await;
        let unchanged = runtime.get_thread(thread_id).await?.unwrap();
        assert_eq!(unchanged.rollout_path, metadata.rollout_path);
        assert_eq!(unchanged.title, "stale title");
    }
    assert_eq!(titles[0], "Hello from user");
    assert_eq!(titles[1], titles[0]);
    assert_eq!(titles[2], titles[0]);
    Ok(())
}

/// Replaces a plain rollout with framed storage; `"archive"` uses three records per frame.
fn convert_to_framed(path: &Path, storage: &str) -> anyhow::Result<()> {
    let bytes = fs::read(path)?;
    let archive = crate::framed::get_archive_path(path);
    let pending = crate::framed::get_pending_path(path)?;
    if storage != "pending" {
        fs::write(
            &archive,
            zstd::stream::encode_all(bytes.as_slice(), /*level*/ 3)?,
        )?;
        crate::recovery(&archive, NonZeroUsize::new(/*n*/ 3).unwrap())?;
    }
    if storage != "archive" {
        let body = if storage == "pending" {
            bytes.as_slice()
        } else {
            b"stale\n"
        };
        fs::write(&pending, [0u64.to_le_bytes().as_slice(), body].concat())?;
    }
    fs::remove_file(path)?;
    Ok(())
}

fn read_logical_lines(path: &Path) -> anyhow::Result<Vec<(Option<u64>, String)>> {
    let bytes = crate::read_rollout_records_from(path, /*start_offset*/ 0)?;
    Ok(std::str::from_utf8(&bytes)?
        .lines()
        .map(|line| match crate::parse_rollout_line(line) {
            Ok(RolloutLine {
                ordinal,
                item: RolloutItem::EventMsg(EventMsg::AgentMessage(event)),
                ..
            }) => (ordinal, event.message),
            Ok(line) => (line.ordinal, "other".to_string()),
            Err(_) => (None, "rejected".to_string()),
        })
        .collect())
}

#[tokio::test]
async fn resume_appends_to_framed_storage_like_a_plain_rollout() -> anyhow::Result<()> {
    for compression in [None, NonZeroUsize::new(/*n*/ 3)] {
        for broken_tail in [false, true] {
            let mut results = Vec::new();
            for storage in ["plain", "pending", "archive", "archive and stale pending"] {
                let home = TempDir::new()?;
                let mut config = test_config(home.path());
                config.rollout_compression = compression;
                let path = home.path().join("rollout.jsonl");
                write_paginated_rollout(&path, ThreadId::default(), &[1, 2, 3])?;
                if broken_tail {
                    let mut file = File::options().append(true).open(&path)?;
                    file.write_all(b"{broken}\n")?;
                }
                if storage != "plain" {
                    convert_to_framed(&path, storage)?;
                }

                for message in ["first resumed", "second resumed"] {
                    let resumed =
                        RolloutRecorder::new(&config, RolloutRecorderParams::resume(path.clone()))
                            .await?;
                    resumed
                        .record_canonical_items(&[agent_message_item(message)])
                        .await?;
                    resumed.flush().await?;
                    resumed.shutdown().await?;
                }
                crate::append_rollout_item_to_path(&path, &agent_message_item("unloaded append"))
                    .await?;

                // Framed storage is appended in place and never written back as plain JSONL.
                assert_eq!(path.exists(), storage == "plain", "{storage}");
                results.push(read_logical_lines(&path)?);
            }
            for result in &results[1..] {
                assert_eq!(result, &results[0], "{compression:?} {broken_tail}");
            }
            assert_eq!(
                results[0].last(),
                Some(&(Some(6), "unloaded append".to_string()))
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn resumed_pending_publishes_only_while_compression_is_enabled() -> anyhow::Result<()> {
    for (compression, published) in [(NonZeroUsize::new(/*n*/ 3), true), (None, false)] {
        let home = TempDir::new()?;
        let mut config = test_config(home.path());
        config.rollout_compression = compression;
        let path = home.path().join("rollout.jsonl");
        // More records than the default frame size: a disabled configuration must not publish.
        let ordinals = (1..=300).collect::<Vec<u64>>();
        write_paginated_rollout(&path, ThreadId::default(), &ordinals)?;
        convert_to_framed(&path, "pending")?;

        let resumed =
            RolloutRecorder::new(&config, RolloutRecorderParams::resume(path.clone())).await?;
        resumed
            .record_canonical_items(&[agent_message_item("third record")])
            .await?;
        resumed.flush().await?;
        resumed.shutdown().await?;

        assert_eq!(
            (
                crate::framed::get_archive_path(&path).exists(),
                crate::framed::get_pending_path(&path)?.exists(),
                path.exists(),
            ),
            (published, !published, false),
            "{compression:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn referencing_framed_storage_leaves_it_in_place() -> anyhow::Result<()> {
    for storage in ["pending", "archive"] {
        let home = TempDir::new()?;
        let path = home.path().join("rollout.jsonl");
        write_paginated_rollout(&path, ThreadId::default(), &[1, 2, 3])?;
        convert_to_framed(&path, storage)?;
        let physical = crate::existing_rollout_path(&path)
            .await
            .expect("framed storage");
        let bytes = fs::read(&physical)?;

        let referenced = crate::materialize_rollout_for_reference(&path).await?;

        assert_eq!(
            (referenced, fs::read(&physical)?, path.exists()),
            (physical, bytes, false),
            "{storage}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn resume_repairs_an_unterminated_pending_tail_like_a_plain_rollout() -> anyhow::Result<()> {
    let mut results = Vec::new();
    for storage in ["plain", "pending"] {
        let home = TempDir::new()?;
        let config = test_config(home.path());
        let path = home.path().join("rollout.jsonl");
        write_paginated_rollout(&path, ThreadId::default(), &[1])?;
        // Drop the final newline, as a crash between the record and its terminator leaves it.
        let bytes = fs::read(&path)?;
        fs::write(&path, &bytes[..bytes.len() - 1])?;
        if storage == "pending" {
            convert_to_framed(&path, storage)?;
        }

        let resumed =
            RolloutRecorder::new(&config, RolloutRecorderParams::resume(path.clone())).await?;
        resumed
            .record_canonical_items(&[agent_message_item("after repair")])
            .await?;
        resumed.flush().await?;
        resumed.shutdown().await?;

        results.push(read_logical_lines(&path)?);
    }
    assert_eq!(results[1], results[0]);
    assert_eq!(
        results[0].last(),
        Some(&(Some(2), "after repair".to_string()))
    );
    Ok(())
}
