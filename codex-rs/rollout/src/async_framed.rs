use crate::framed::FramedWriter;
use std::io;
use std::num::NonZeroUsize;
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
