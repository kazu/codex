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
use crate::rollout_source::RolloutSource;
use crate::rollout_source::Shifted;

/// Reads the newline-terminated suffix after an existing projection's record-boundary byte checkpoint.
/// Framed payloads are retrieved by physical record; a partial final JSONL line is deferred.
pub fn read_rollout_records_from(path: &Path, start_offset: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let file = match RolloutSource::open_framed(path)? {
        Some(RolloutSource::Framed(file)) => {
            let mut source = FramedRecords::open(path, file)?;
            let (index, skipped) = source.locate(start_offset)?;
            let total = source.reader.total_records().map_err(io::Error::other)?;
            let count = total
                .checked_sub(index)
                .ok_or_else(|| io::Error::other("rollout record index is past the file"))?;
            if count > 0 {
                source
                    .reader
                    .records_to(index, count, &mut bytes)
                    .map_err(io::Error::other)?;
            }
            if skipped > 0 {
                bytes.drain(..skipped);
            }
            None
        }
        Some(RolloutSource::Pending(file)) => Some(file),
        None => Some(crate::open_rollout_seekable_reader(path)?),
    };
    if let Some(file) = file {
        let mut reader = Shifted::new(file)?;
        let count = reader
            .seek(SeekFrom::End(0))?
            .checked_sub(start_offset)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "rollout checkpoint is past the file",
                )
            })?;
        bytes.resize(
            usize::try_from(count).map_err(io::Error::other)?,
            /*value*/ 0,
        );
        reader.seek(SeekFrom::Start(start_offset))?;
        reader.read_exact(&mut bytes)?;
    }
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(/*default*/ 0, |last| last + 1);
    bytes.truncate(complete);
    Ok(bytes)
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
        Self::open_with_end(path, /*end*/ None)
    }

    /// Reads only the prefix captured by an existing history's exclusive record-boundary byte endpoint.
    pub fn open_at(path: &Path, end_byte_offset: u64) -> io::Result<Self> {
        Self::open_with_end(path, Some(end_byte_offset))
    }

    fn open_with_end(path: &Path, end: Option<u64>) -> io::Result<Self> {
        let file = match RolloutSource::open_framed(path)? {
            Some(RolloutSource::Framed(file)) => {
                let mut source = FramedRecords::open(path, file)?;
                let (next, partial) = source.locate(end.unwrap_or(source.table.size_decomp()))?;
                return Ok(Self(ReverseSource::Framed {
                    source: Box::new(source),
                    next,
                    partial,
                }));
            }
            Some(RolloutSource::Pending(file)) => file,
            None => crate::open_rollout_seekable_reader(path)?,
        };
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
            let record = source
                .reader
                .record(*next)
                .map_err(io::Error::other)?
                .ok_or_else(|| {
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
}

impl FramedRecords {
    fn open(path: &Path, mut file: File) -> io::Result<Self> {
        let table = SeekTable::from_seekable(&mut file).map_err(io::Error::other)?;
        let reader =
            RecordReader::from_file(path.to_owned(), file, b"\n").map_err(io::Error::other)?;
        Ok(Self { reader, table })
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
            let total = self.reader.total_records().map_err(io::Error::other)?;
            return Ok((total, 0));
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
            let record = self
                .reader
                .record(index)
                .map_err(io::Error::other)?
                .ok_or_else(|| {
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
