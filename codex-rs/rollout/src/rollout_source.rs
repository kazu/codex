//! Resolves framed archive and pending storage next to the plain and legacy zstd representations.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use tokio::io::AsyncBufReadExt;

use crate::compression::RolloutLineReaderInner;
use crate::framed::StoredRecords;
use crate::ordinal::RolloutOrdinalState;
use crate::ordinal::ordinal_state_for_rollout;
use crate::plain_rollout_path;

pub(crate) fn get_rollout_paths(path: &Path) -> io::Result<[PathBuf; 4]> {
    let plain = plain_rollout_path(path);
    let legacy = plain.with_extension("jsonl.zst");
    let archive = crate::framed::get_archive_path(&plain);
    let pending = crate::framed::get_pending_path(&plain)?;
    Ok([plain, legacy, archive, pending])
}

/// Returns the archive and pending files that can hold a rollout besides its plain and legacy
/// zstd files, for callers that move or delete every file of a rollout.
pub fn framed_rollout_paths(path: &Path) -> Vec<PathBuf> {
    get_rollout_paths(path)
        .map(|[_, _, archive, pending]| vec![archive, pending])
        .unwrap_or_default()
}

/// Blocking form of [`crate::existing_rollout_path`] for callers that stat a recorder's logical
/// path; returns `path` itself when no representation exists.
///
/// A pending file that holds only its start header has no records, so it is passed over the
/// way an empty plain file fails a caller's size check.
pub fn stored_rollout_path(path: &Path) -> PathBuf {
    get_rollout_paths(path)
        .ok()
        .and_then(|[plain, legacy, archive, pending]| {
            [plain, legacy, archive]
                .into_iter()
                .find(|candidate| candidate.is_file())
                .or_else(|| {
                    std::fs::metadata(&pending)
                        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 8)
                        .then_some(pending)
                })
        })
        .unwrap_or_else(|| path.to_path_buf())
}

/// Returns the logical `.jsonl` path for an archive or pending file and any other path
/// unchanged, to compare a listed rollout path with the logical path a live recorder reports.
pub fn logical_path_of_framed(path: &Path) -> PathBuf {
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(crate::framed::is_framed_file_name)
    {
        return plain_rollout_path(path);
    }
    path.to_path_buf()
}

/// Returns whether a legacy zstd, archive, or pending file is superseded by an existing file
/// earlier in the resolution order, so directory walks report one file per rollout.
pub(crate) fn is_hidden_by_sibling(path: &Path) -> bool {
    get_rollout_paths(path).is_ok_and(|candidates| {
        candidates
            .iter()
            .take_while(|candidate| *candidate != path)
            .any(|candidate| candidate.exists())
    })
}

/// Returns the archive or pending file when [`crate::existing_rollout_path`] resolves to one,
/// that is when no plain or legacy zstd file takes precedence.
pub(crate) async fn existing_framed_path(path: &Path) -> Option<PathBuf> {
    let resolved = crate::existing_rollout_path(path).await?;
    crate::framed::is_framed_file_name(resolved.file_name()?.to_str()?).then_some(resolved)
}

/// Continues [`crate::existing_rollout_path`] resolution with the archive, then the pending file.
pub(crate) async fn existing_framed_with_metadata(
    path: &Path,
) -> Option<(PathBuf, std::fs::Metadata)> {
    let [_, _, archive, pending] = get_rollout_paths(path).ok()?;
    for candidate in [archive, pending] {
        if let Ok(metadata) = tokio::fs::metadata(&candidate).await
            && metadata.is_file()
        {
            return Some((candidate, metadata));
        }
    }
    None
}

pub(crate) enum RolloutSource {
    Framed(File),
    /// Positioned after the eight-byte start header.
    Pending(File),
}

impl RolloutSource {
    /// Opens framed or pending storage when no plain or legacy zstd file takes precedence.
    pub(crate) fn open_framed(path: &Path) -> io::Result<Option<Self>> {
        let [plain_path, legacy_path, archive_path, pending_path] = get_rollout_paths(path)?;
        if plain_path.exists() || legacy_path.exists() {
            return Ok(None);
        }
        Ok(Self::open_fork(archive_path, pending_path)?.map(|(source, _)| source))
    }

    // Publication persists the archive before removing the pending file, so the second archive
    // attempt covers a publication between the first two opens.
    fn open_fork(
        archive_path: PathBuf,
        pending_path: PathBuf,
    ) -> io::Result<Option<(Self, PathBuf)>> {
        for attempt in 0..2 {
            match File::open(&archive_path) {
                Ok(file) => return Ok(Some((Self::Framed(file), archive_path))),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            if attempt == 0 {
                match File::open(&pending_path) {
                    Ok(mut file) => {
                        // The writer creates the file before its header; until the header is
                        // complete the rollout has no records, like an empty plain file.
                        if file.metadata()?.len() < 8 {
                            file.seek(SeekFrom::End(0))?;
                        } else {
                            crate::framed::read_start(&mut file)?;
                        }
                        return Ok(Some((Self::Pending(file), pending_path)));
                    }
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(None)
    }
}

/// Opens the line reader and metrics format label for a path that
/// [`crate::existing_rollout_path`] resolved to an archive or pending file; `None` leaves every
/// other name to the caller without touching the file system.
pub(crate) async fn open_framed_lines(
    path: &Path,
) -> Option<io::Result<(RolloutLineReaderInner, &'static str)>> {
    if !crate::framed::is_framed_file_name(path.file_name()?.to_str()?) {
        return None;
    }
    let path = path.to_owned();
    let opened = tokio::task::spawn_blocking(move || {
        let [_, _, archive_path, pending_path] = get_rollout_paths(&path)?;
        match RolloutSource::open_fork(archive_path, pending_path)? {
            Some((RolloutSource::Framed(file), path)) => {
                let reader = seekzstdsep::RecordReader::from_file(path, file, b"\n")
                    .map_err(io::Error::other)?;
                let lines = FramedLines {
                    max_batch_len: reader.records_per_frame(),
                    batch_len: FIRST_BATCH_LEN.min(reader.records_per_frame()),
                    records: Some(reader.into_records()),
                    batch: VecDeque::new(),
                };
                Ok((RolloutLineReaderInner::Framed(Box::new(lines)), "zstd"))
            }
            Some((RolloutSource::Pending(file), _)) => {
                let file = tokio::fs::File::from_std(file);
                let lines = tokio::io::BufReader::new(file).lines();
                Ok((RolloutLineReaderInner::Plain(lines), "plain"))
            }
            None => Err(io::ErrorKind::NotFound.into()),
        }
    })
    .await;
    Some(opened.map_err(io::Error::other).and_then(|opened| opened))
}

// Head reads take a few lines, so the first batch is small; full reads double it up to one
// frame's record count.
const FIRST_BATCH_LEN: usize = 8;

/// Lines of a framed archive, decoded record by record in batches so the blocking pool is
/// entered once per batch rather than once per line.
pub(crate) struct FramedLines {
    records: Option<seekzstdsep::RecordIter>,
    batch_len: usize,
    max_batch_len: usize,
    batch: VecDeque<io::Result<String>>,
}

impl FramedLines {
    pub(crate) fn into_blocking_lines(
        self,
    ) -> io::Result<impl Iterator<Item = io::Result<String>> + Send> {
        let records = self
            .records
            .ok_or_else(|| io::Error::other("compressed rollout reader is busy"))?;
        Ok(self.batch.into_iter().chain(records.map(|record| {
            record
                .map_err(io::Error::other)
                .and_then(decode_record_line)
        })))
    }

    /// Yields what `BufRead::lines` yields for the same bytes.
    pub(crate) async fn next_line(&mut self) -> io::Result<Option<String>> {
        if self.batch.is_empty() {
            let Some(mut records) = self.records.take() else {
                return Err(io::Error::other("compressed rollout reader is busy"));
            };
            let batch_len = self.batch_len;
            let (batch, records) = tokio::task::spawn_blocking(move || {
                let mut batch = VecDeque::with_capacity(batch_len);
                for record in records.by_ref().take(batch_len) {
                    let line = record
                        .map_err(io::Error::other)
                        .and_then(decode_record_line);
                    let failed = line.is_err();
                    batch.push_back(line);
                    if failed {
                        break;
                    }
                }
                (batch, records)
            })
            .await
            .map_err(io::Error::other)?;
            self.batch = batch;
            self.records = Some(records);
            self.batch_len = batch_len.saturating_mul(2).min(self.max_batch_len);
        }
        self.batch.pop_front().transpose()
    }
}

fn decode_record_line(mut record: Vec<u8>) -> io::Result<String> {
    if record.ends_with(b"\n") {
        record.pop();
        if record.ends_with(b"\r") {
            record.pop();
        }
    }
    String::from_utf8(record).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        )
    })
}

/// Applies [`crate::ordinal::ordinal_state_for_rollout`] to framed or pending storage.
///
/// That function reads the first record and scans back to the last record that parses. An
/// archive supplies exactly those records, fetched by record, so no frame in between is decoded.
pub(crate) fn ordinal_state_for_framed(
    path: &Path,
    records: StoredRecords,
) -> io::Result<RolloutOrdinalState> {
    let reader = match records {
        StoredRecords::Archive(reader) => *reader,
        StoredRecords::Pending(bytes) => {
            return ordinal_state_for_rollout(&mut io::Cursor::new(bytes), path);
        }
    };
    // The two ends of the iterator never yield the same record twice.
    let mut records = reader.into_records();
    let mut bytes = Vec::new();
    for record in records.by_ref() {
        let record = record.map_err(io::Error::other)?;
        bytes.extend_from_slice(&record);
        if !record.iter().all(u8::is_ascii_whitespace) {
            break;
        }
    }
    let mut tail = Vec::new();
    while let Some(record) = records.next_back() {
        let record = record.map_err(io::Error::other)?;
        let parsed = crate::parse_rollout_line_bytes(&record).is_ok();
        tail.push(record);
        if parsed {
            break;
        }
    }
    for record in tail.iter().rev() {
        bytes.extend_from_slice(record);
    }
    ordinal_state_for_rollout(&mut io::Cursor::new(bytes), path)
}

/// [`crate::read_rollout_prefix`] for framed or pending storage; the outer `None` leaves the
/// other formats to the caller.
pub(crate) fn read_framed_prefix(
    path: &Path,
    max_bytes: usize,
) -> io::Result<Option<Option<Vec<u8>>>> {
    let (file, framed) = match RolloutSource::open_framed(path)? {
        Some(RolloutSource::Framed(file)) => (file, true),
        Some(RolloutSource::Pending(file)) => (file, false),
        None => return Ok(None),
    };
    if !file.metadata()?.is_file() {
        return Ok(Some(None));
    }
    let reader: Box<dyn Read> = if framed {
        Box::new(zstd::stream::read::Decoder::new(file)?)
    } else {
        Box::new(file)
    };
    let mut bytes = Vec::new();
    reader.take(max_bytes as u64).read_to_end(&mut bytes)?;
    Ok(Some(Some(bytes)))
}

/// [`crate::rollout_contains_prefix`] for framed or pending storage; `None` leaves the other
/// formats to the caller.
pub(crate) fn framed_contains_prefix(
    path: &Path,
    end_byte_offset: u64,
) -> io::Result<Option<bool>> {
    let len = match RolloutSource::open_framed(path)? {
        Some(RolloutSource::Framed(mut file)) => zeekstd::SeekTable::from_seekable(&mut file)
            .map_err(io::Error::other)?
            .size_decomp(),
        Some(RolloutSource::Pending(file)) => Shifted::new(file)?.seek(SeekFrom::End(0))?,
        None => return Ok(None),
    };
    Ok(Some(end_byte_offset <= len))
}

/// Presents a file from its current position as byte zero, hiding the pending start header.
pub(crate) struct Shifted {
    file: File,
    shift: u64,
}

impl Shifted {
    pub(crate) fn new(mut file: File) -> io::Result<Self> {
        let shift = file.stream_position()?;
        Ok(Self { file, shift })
    }
}

impl Read for Shifted {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for Shifted {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let pos = match pos {
            SeekFrom::Start(offset) => SeekFrom::Start(
                offset
                    .checked_add(self.shift)
                    .ok_or_else(|| io::Error::other("rollout byte offset overflow"))?,
            ),
            SeekFrom::End(_) | SeekFrom::Current(_) => pos,
        };
        self.file.seek(pos)?.checked_sub(self.shift).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before the start of the rollout records",
            )
        })
    }
}

#[cfg(test)]
#[path = "rollout_source_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "framed_discovery_tests.rs"]
mod discovery_tests;
