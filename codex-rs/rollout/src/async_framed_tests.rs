use super::AsyncFramedWriter;
use crate::WriterLockCoordinator;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn join_failure_after_publication_leaves_writer_unavailable() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let path = home.path().join("rollout.jsonl");
    let coordinator = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::new();
    let mut writer = AsyncFramedWriter::open(
        path.clone(),
        std::num::NonZeroUsize::new(/*n*/ 1).unwrap(),
        Some(Arc::new(coordinator.acquire(thread_id)?)),
    )
    .await?;
    writer.append("{}\n".to_owned()).await?;
    let error = writer
        .run(|writer| {
            writer.promote()?;
            panic!("injected failure after archive publication");
        })
        .await
        .expect_err("a blocking panic must propagate");
    assert!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<tokio::task::JoinError>())
            .is_some_and(tokio::task::JoinError::is_panic)
    );
    assert!(!writer.is_available());
    assert!(writer.promote().await.is_err());
    assert!(
        writer
            .append("{\"unexpected\":true}\n".to_owned())
            .await
            .is_err()
    );
    assert!(
        matches!(coordinator.acquire(thread_id), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    drop(writer);
    let _guard = coordinator.acquire(thread_id)?;
    let records =
        zstd::stream::decode_all(std::fs::File::open(path.with_extension("jsonl.seek.zst"))?)?;
    assert_eq!(records, b"{}\n");
    Ok(())
}

#[tokio::test]
async fn cancelled_wait_keeps_writer_lock_until_blocking_io_finishes() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let coordinator = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::new();
    let guard = Arc::new(coordinator.acquire(thread_id)?);
    let path = home.path().join("rollout.jsonl");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        let mut writer = AsyncFramedWriter::open(
            path,
            std::num::NonZeroUsize::new(/*n*/ 2).unwrap(),
            Some(guard.clone()),
        )
        .await?;
        let result = writer
            .run(move |writer| {
                let _ = started_tx.send(());
                release_rx.recv().map_err(std::io::Error::other)?;
                writer.append(b"{}\n")
            })
            .await;
        drop(guard);
        result
    });
    started_rx.await?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let held_during_io = matches!(coordinator.acquire(thread_id), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock);
    release_tx.send(())?;
    assert!(
        held_during_io,
        "blocking I/O must retain the writer lock after cancellation"
    );
    tokio::time::timeout(Duration::from_secs(/*secs*/ 5), async {
        loop {
            if coordinator.acquire(thread_id).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 1)).await;
        }
    })
    .await?;
    Ok(())
}
