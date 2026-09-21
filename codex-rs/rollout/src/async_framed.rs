use crate::framed::FramedWriter;
use crate::ordinal::RolloutOrdinalState;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// Moves the retained writer into bounded blocking operations without copying records.
pub(crate) struct AsyncFramedWriter {
    writer: Option<FramedWriter>,
    writer_lock: Option<Arc<crate::WriterLockGuard>>,
}

impl AsyncFramedWriter {
    pub(crate) async fn open(
        path: PathBuf,
        frame_size: NonZeroUsize,
        writer_lock: Option<Arc<crate::WriterLockGuard>>,
    ) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let parent = path
                .parent()
                .ok_or_else(|| io::Error::other("rollout path has no parent"))?;
            std::fs::create_dir_all(parent)?;
            let writer = FramedWriter::open(&path, frame_size)?;
            Ok(Self {
                writer: Some(writer),
                writer_lock,
            })
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Opens existing framed storage to append in place, with its frame size and the ordinal
    /// state for the next record. Returns `None` unless [`crate::existing_rollout_path`]
    /// resolves to an archive or pending file; plain and legacy zstd rollouts keep their own
    /// open path. A pending file publishes at `frame_size`; with compression disabled
    /// (`None`) it keeps receiving records and is never published as an archive.
    pub(crate) async fn open_existing(
        path: &Path,
        frame_size: Option<NonZeroUsize>,
        writer_lock: Option<Arc<crate::WriterLockGuard>>,
    ) -> io::Result<Option<(PathBuf, Self, NonZeroUsize, RolloutOrdinalState)>> {
        if crate::rollout_source::existing_framed_path(path)
            .await
            .is_none()
        {
            return Ok(None);
        }
        let path = crate::plain_rollout_path(path);
        tokio::task::spawn_blocking(move || {
            let (writer, records) =
                FramedWriter::open_with_records(&path, frame_size.unwrap_or(NonZeroUsize::MAX))?;
            let ordinal_state = crate::rollout_source::ordinal_state_for_framed(&path, records)?;
            let frame_size = writer.records_per_frame();
            let writer = Self {
                writer: Some(writer),
                writer_lock,
            };
            Ok(Some((path, writer, frame_size, ordinal_state)))
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn append(&mut self, line: String) -> io::Result<()> {
        self.run(move |writer| writer.append(line.as_bytes())).await
    }

    pub(crate) async fn promote(&mut self) -> io::Result<()> {
        self.run(FramedWriter::promote).await
    }

    pub(crate) fn is_available(&self) -> bool {
        self.writer.is_some()
    }

    async fn run(
        &mut self,
        operation: impl FnOnce(&mut FramedWriter) -> io::Result<()> + Send + 'static,
    ) -> io::Result<()> {
        let mut writer = self.writer.take().ok_or_else(|| {
            io::Error::other("rollout writer unavailable after blocking task failure")
        })?;
        let writer_lock = self.writer_lock.clone();
        let (writer, result) = tokio::task::spawn_blocking(move || {
            let _writer_lock = writer_lock;
            let result = operation(&mut writer);
            (writer, result)
        })
        .await
        .map_err(io::Error::other)?;
        self.writer = Some(writer);
        result
    }
}

#[cfg(test)]
#[path = "async_framed_tests.rs"]
mod tests;
