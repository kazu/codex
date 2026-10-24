//! Record access with byte coordinates retained only for existing history checkpoints.

use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use seekzstdsep::RecordReader;
use zeekstd::SeekTable;

use crate::ReverseJsonlScanner;
use crate::RolloutLine;
use crate::ScanOutcome;
use crate::framed::archive_frame_lock;
use crate::rollout_source::RolloutSource;
use crate::rollout_source::Shifted;
use crate::seekable_reader::CancellableReader;
use crate::seekable_reader::open_plain_or_legacy_file;

fn lock_existing_archive(path: &Path) -> io::Result<Option<File>> {
    match archive_frame_lock(path, false) {
        Ok(lock) => Ok(Some(lock)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(crate) fn read_rollout_records_from(path: &Path, start_offset: u64) -> io::Result<Vec<u8>> {
    let (mut reader, _) = open_rollout_projection_reader(path, start_offset, || false)?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |last| last + 1);
    bytes.truncate(complete);
    Ok(bytes)
}

/// Opens a bounded forward projection stream at a complete-record byte checkpoint.
///
/// The returned endpoint is captured when the representation is opened. Framed files are decoded
/// one physical record at a time; other representations retain an open seekable snapshot.
pub fn open_rollout_projection_reader<'a>(
    path: &Path,
    start_offset: u64,
    is_cancelled: impl Fn() -> bool + 'a,
) -> io::Result<(Box<dyn Read + 'a>, u64)> {
    if is_cancelled() {
        return Err(io::Error::other("rollout read cancelled"));
    }
    let frame_lock = lock_existing_archive(path)?;
    let file = match RolloutSource::open_framed(path)? {
        Some(RolloutSource::Framed(file)) => {
            let mut source = FramedRecords::open(path, file)?;
            drop(frame_lock);
            let end = source.table.size_decomp();
            let (next, skipped) = source.locate(start_offset)?;
            if skipped != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "rollout checkpoint is inside a record",
                ));
            }
            let total = source.total;
            let reader = FramedForwardReader {
                source,
                next,
                total,
                record: Vec::new(),
                position: 0,
            };
            return Ok((Box::new(CancellableReader::new(reader, is_cancelled)), end));
        }
        Some(RolloutSource::Pending(file)) => file,
        None => open_plain_or_legacy_file(path, &is_cancelled)?,
    };
    let mut reader = Shifted::new(file)?;
    let end = reader.seek(SeekFrom::End(0))?;
    if start_offset > end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rollout checkpoint is past the file",
        ));
    }
    reader.seek(SeekFrom::Start(start_offset))?;
    Ok((Box::new(CancellableReader::new(reader, is_cancelled)), end))
}

struct FramedForwardReader {
    source: FramedRecords,
    next: usize,
    total: usize,
    record: Vec<u8>,
    position: usize,
}

impl Read for FramedForwardReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while self.position == self.record.len() {
            if self.next == self.total {
                return Ok(0);
            }
            self.record = self.source.record(self.next)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "missing rollout record")
            })?;
            self.next += 1;
            self.position = 0;
        }
        let count = output.len().min(self.record.len() - self.position);
        output[..count].copy_from_slice(&self.record[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

/// Reads rollout records backwards without materializing framed or pending histories.
pub struct RolloutReverseScanner(ReverseSource);

enum ReverseSource {
    Jsonl(ReverseJsonlScanner<Shifted>),
    Framed {
        source: Box<FramedRecords>,
        next: usize,
        /// Bytes of record `next` before an end position inside that record, read first.
        partial: usize,
    },
}

impl RolloutReverseScanner {
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_end_cancellable(path, /*end*/ None, || false)
    }

    /// Reads only the prefix captured by an existing history's exclusive record-boundary byte endpoint.
    pub fn open_at(path: &Path, end_byte_offset: u64) -> io::Result<Self> {
        Self::open_with_end_cancellable(path, Some(end_byte_offset), || false)
    }

    pub fn open_with_end_cancellable(
        path: &Path,
        end: Option<u64>,
        is_cancelled: impl Fn() -> bool,
    ) -> io::Result<Self> {
        if is_cancelled() {
            return Err(io::Error::other("rollout read cancelled"));
        }
        let frame_lock = lock_existing_archive(path)?;
        let file = match RolloutSource::open_framed(path)? {
            Some(RolloutSource::Framed(file)) => {
                let mut source = FramedRecords::open(path, file)?;
                drop(frame_lock);
                let (next, partial) = source.locate(end.unwrap_or(source.table.size_decomp()))?;
                if is_cancelled() {
                    return Err(io::Error::other("rollout read cancelled"));
                }
                return Ok(Self(ReverseSource::Framed {
                    source: Box::new(source),
                    next,
                    partial,
                }));
            }
            Some(RolloutSource::Pending(file)) => file,
            None => open_plain_or_legacy_file(path, &is_cancelled)?,
        };
        if is_cancelled() {
            return Err(io::Error::other("rollout read cancelled"));
        }
        let reader = Shifted::new(file)?;
        Ok(Self(ReverseSource::Jsonl(match end {
            Some(end) => ReverseJsonlScanner::new_at(reader, end)?,
            None => ReverseJsonlScanner::new(reader)?,
        })))
    }

    /// Uses the canonical decoder, including compatibility with older persisted rollout items.
    pub fn scan_next_rollout_line(&mut self) -> io::Result<Option<ScanOutcome<RolloutLine>>> {
        let (source, next, partial) = match &mut self.0 {
            ReverseSource::Jsonl(scanner) => return scanner.scan_next_rollout_line(),
            ReverseSource::Framed {
                source,
                next,
                partial,
            } => (source, next, partial),
        };
        while *next > 0 || *partial > 0 {
            let len = std::mem::take(partial);
            if len == 0 {
                *next -= 1;
            }
            let record = source.record(*next)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "missing rollout record")
            })?;
            let record = if len == 0 {
                &record[..]
            } else {
                &record[..len]
            };
            if !record.iter().all(u8::is_ascii_whitespace) {
                return Ok(Some(match crate::parse_rollout_line_bytes(record) {
                    Ok(value) => ScanOutcome::Parsed(value),
                    Err(error) => ScanOutcome::Rejected(error),
                }));
            }
        }
        Ok(None)
    }
}

struct FramedRecords {
    reader: RecordReader,
    table: SeekTable,
    total: usize,
    last_frame_start: usize,
    last_frame: Vec<Vec<u8>>,
}

impl FramedRecords {
    fn open(path: &Path, mut file: File) -> io::Result<Self> {
        let table = SeekTable::from_seekable(&mut file).map_err(io::Error::other)?;
        let mut reader =
            RecordReader::from_file(path.to_owned(), file, b"\n").map_err(io::Error::other)?;
        let total = reader.total_records().map_err(io::Error::other)?;
        let last_frame_start = if table.num_frames() == 0 {
            0
        } else {
            usize::try_from(table.num_frames() - 1)
                .map_err(io::Error::other)?
                .checked_mul(reader.records_per_frame())
                .ok_or_else(|| io::Error::other("rollout record index overflow"))?
        };
        let last_frame_len = total
            .checked_sub(last_frame_start)
            .ok_or_else(|| io::Error::other("rollout seek table exceeds record count"))?;
        let mut last_frame = Vec::with_capacity(last_frame_len);
        for index in last_frame_start..total {
            last_frame.push(
                reader
                    .record(index)
                    .map_err(io::Error::other)?
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "missing rollout record")
                    })?,
            );
        }
        Ok(Self {
            reader,
            table,
            total,
            last_frame_start,
            last_frame,
        })
    }

    fn record(&mut self, index: usize) -> io::Result<Option<Vec<u8>>> {
        if index >= self.last_frame_start {
            return Ok(self.last_frame.get(index - self.last_frame_start).cloned());
        }
        self.reader.record(index).map_err(io::Error::other)
    }

    /// Translates a persisted byte position into a physical record index and the bytes of that
    /// record before the position (zero at a record boundary).
    /// Physical records include blank/rejected lines and are not rollout ordinals.
    fn locate(&mut self, offset: u64) -> io::Result<(usize, usize)> {
        if offset > self.table.size_decomp() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rollout checkpoint is past the file",
            ));
        }
        if offset == self.table.size_decomp() {
            return Ok((self.total, 0));
        }
        let frame = self.table.frame_index_decomp(offset);
        let mut remaining = offset
            - self
                .table
                .frame_start_decomp(frame)
                .map_err(io::Error::other)?;
        let mut index = usize::try_from(frame)
            .map_err(io::Error::other)?
            .checked_mul(self.reader.records_per_frame())
            .ok_or_else(|| io::Error::other("rollout record index overflow"))?;
        while remaining > 0 {
            let record = self.record(index)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "missing rollout record")
            })?;
            if remaining < record.len() as u64 {
                break;
            }
            remaining -= record.len() as u64;
            index = index
                .checked_add(/*rhs*/ 1)
                .ok_or_else(|| io::Error::other("rollout record index overflow"))?;
        }
        Ok((index, remaining as usize))
    }
}

#[cfg(test)]
#[path = "record_reader_tests.rs"]
mod tests;
