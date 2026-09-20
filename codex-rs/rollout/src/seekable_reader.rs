//! Reads rollout bytes and positions independently of their plain or compressed representation.
//!
//! Offsets always address the original JSONL bytes. Readers retain an open file, or an anonymous
//! decoded snapshot, so a concurrent compression cannot invalidate an in-progress scan.

use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use crate::plain_rollout_path;

pub(crate) fn get_rollout_paths(path: &Path) -> io::Result<[PathBuf; 4]> {
    let plain = plain_rollout_path(path);
    let legacy = crate::compression::compressed_rollout_path(&plain);
    let archive = plain.with_extension("seek.zst");
    let pending = crate::framed::get_pending_path(&plain)?;
    Ok([plain, legacy, archive, pending])
}

pub(crate) enum RolloutReader {
    Plain(File),
    Compressed(File),
    Pending(File),
}

impl RolloutReader {
    fn open(path: &Path) -> io::Result<Self> {
        for attempt in 0..4 {
            match Self::open_once(path) {
                Ok((reader, _)) => return Ok(reader),
                Err(err) if err.kind() == io::ErrorKind::NotFound && attempt < 3 => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("the final open attempt returns its error")
    }

    pub(crate) fn open_once(path: &Path) -> io::Result<(Self, PathBuf)> {
        let [plain_path, legacy_path, archive_path, pending_path] = get_rollout_paths(path)?;
        match File::open(&plain_path) {
            Ok(file) => return Ok((Self::Plain(file), plain_path)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        for compressed_path in [legacy_path, archive_path] {
            match File::open(&compressed_path) {
                Ok(file) => return Ok((Self::Compressed(file), compressed_path)),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        let mut file = File::open(&pending_path)?;
        crate::framed::read_start(&mut file)?;
        Ok((Self::Pending(file), pending_path))
    }

    pub(crate) fn file(&self) -> &File {
        match self {
            Self::Plain(file) | Self::Compressed(file) | Self::Pending(file) => file,
        }
    }

    pub(crate) fn into_read(self) -> io::Result<Box<dyn Read + Send>> {
        match self {
            Self::Plain(file) | Self::Pending(file) => Ok(Box::new(file)),
            Self::Compressed(file) => Ok(Box::new(zstd::stream::read::Decoder::new(file)?)),
        }
    }
}

/// Reads at most `max_bytes` decoded rollout bytes without materializing the durable file.
///
/// Returns `None` for nonregular files. This retains an open representation while reading, so
/// compression or materialization cannot invalidate the read after resolution.
pub fn read_rollout_prefix(path: &Path, max_bytes: usize) -> io::Result<Option<Vec<u8>>> {
    if std::fs::metadata(path).is_ok_and(|metadata| !metadata.is_file()) {
        return Ok(None);
    }
    let source = RolloutReader::open(path)?;
    if !source.file().metadata()?.is_file() {
        return Ok(None);
    }
    let reader = source.into_read()?;
    let mut bytes = Vec::new();
    reader.take(max_bytes as u64).read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

/// Opens the original JSONL bytes for blocking offset reads without changing the rollout on disk.
///
/// Compressed files are decoded and pending bodies copied into an anonymous temporary file,
/// keeping memory use bounded and preserving read-only access to the Codex home. Plain files retain
/// their existing seek fast path.
pub fn open_rollout_seekable_reader(path: &Path) -> io::Result<File> {
    match RolloutReader::open(path)? {
        RolloutReader::Plain(file) => Ok(file),
        source @ (RolloutReader::Compressed(_) | RolloutReader::Pending(_)) => {
            let mut decoded = tempfile::tempfile()?;
            io::copy(&mut source.into_read()?, &mut decoded)?;
            decoded.rewind()?;
            Ok(decoded)
        }
    }
}

/// Checks a frozen prefix's byte bound using a blocking read of the logical JSONL representation.
///
/// A known first-frame size is a lower bound even for concatenated zstd frames. New compressed
/// rollouts include that size, so ordinary lineage validation only reads the header. Older frames
/// without a size, and prefixes extending beyond the first frame, are decoded up to the bound.
pub fn rollout_contains_prefix(path: &Path, end_byte_offset: u64) -> io::Result<bool> {
    match RolloutReader::open(path)? {
        RolloutReader::Plain(file) => Ok(end_byte_offset <= file.metadata()?.len()),
        RolloutReader::Pending(mut file) => Ok(end_byte_offset
            <= file
                .metadata()?
                .len()
                .saturating_sub(file.stream_position()?)),
        RolloutReader::Compressed(mut file) => {
            // A zstd frame header occupies at most 18 bytes.
            let mut header = [0; 18];
            let read = file.read(&mut header)?;
            if zstd::zstd_safe::get_frame_content_size(&header[..read])
                .ok()
                .flatten()
                .is_some_and(|size| end_byte_offset <= size)
            {
                return Ok(true);
            }
            file.rewind()?;
            let mut prefix = zstd::stream::read::Decoder::new(file)?.take(end_byte_offset);
            Ok(io::copy(&mut prefix, &mut io::sink())? == end_byte_offset)
        }
    }
}

#[cfg(test)]
#[path = "seekable_reader_tests.rs"]
mod tests;
