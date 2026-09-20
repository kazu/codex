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
use crate::seekable_reader::RolloutReader;

/// Reads the newline-terminated suffix after an existing projection's record-boundary byte checkpoint.
/// Framed payloads are retrieved by physical record; a partial final JSONL line is deferred.
pub fn read_rollout_records_from(path: &Path, start_offset: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match RolloutReader::open(path)? {
        RolloutReader::Framed(file) => {
            let mut source = FramedRecords::open(path, file)?;
            let index = source.locate(start_offset)?;
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
        }
        RolloutReader::Plain(mut file) | RolloutReader::Pending(mut file) => {
            let start = file
                .stream_position()?
                .checked_add(start_offset)
                .ok_or_else(|| io::Error::other("rollout byte offset overflow"))?;
            let count = file.metadata()?.len().checked_sub(start).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "rollout checkpoint is past the file",
                )
            })?;
            bytes.resize(
                usize::try_from(count).map_err(io::Error::other)?,
                /*value*/ 0,
            );
            file.seek(SeekFrom::Start(start))?;
            file.read_exact(&mut bytes)?;
        }
        source @ RolloutReader::Compressed(_) => {
            let mut reader = source.into_read()?;
            if io::copy(&mut reader.by_ref().take(start_offset), &mut io::sink())? != start_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "rollout checkpoint is past the file",
                ));
            }
            reader.read_to_end(&mut bytes)?;
        }
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
    Jsonl(ReverseJsonlScanner<File>),
    Framed {
        source: Box<FramedRecords>,
        next: usize,
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
        let mut file = match RolloutReader::open(path)? {
            RolloutReader::Framed(file) => {
                let mut source = FramedRecords::open(path, file)?;
                let next = source.locate(end.unwrap_or(source.table.size_decomp()))?;
                return Ok(Self(ReverseSource::Framed {
                    source: Box::new(source),
                    next,
                }));
            }
            RolloutReader::Plain(file) | RolloutReader::Pending(file) => file,
            source @ RolloutReader::Compressed(_) => {
                let mut file = tempfile::tempfile()?;
                io::copy(&mut source.into_read()?, &mut file)?;
                file.rewind()?;
                file
            }
        };
        let start = file.stream_position()?;
        let end = match end {
            Some(end) => start
                .checked_add(end)
                .ok_or_else(|| io::Error::other("rollout byte offset overflow"))?,
            None => file.metadata()?.len(),
        };
        Ok(Self(ReverseSource::Jsonl(
            ReverseJsonlScanner::new_between(file, start, end)?,
        )))
    }

    /// Uses the canonical decoder, including compatibility with older persisted rollout items.
    pub fn scan_next_rollout_line(&mut self) -> io::Result<Option<ScanOutcome<RolloutLine>>> {
        let (source, next) = match &mut self.0 {
            ReverseSource::Jsonl(scanner) => return scanner.scan_next_rollout_line(),
            ReverseSource::Framed { source, next } => (source, next),
        };
        while *next > 0 {
            *next -= 1;
            let record = source
                .reader
                .record(*next)
                .map_err(io::Error::other)?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "missing rollout record")
                })?;
            if !record.iter().all(u8::is_ascii_whitespace) {
                return Ok(Some(
                    match serde_json::from_slice(&record).and_then(crate::decode_rollout_line) {
                        Ok(value) => ScanOutcome::Parsed(value),
                        Err(error) => ScanOutcome::Rejected(error),
                    },
                ));
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

    /// Translates a persisted record-boundary byte position into a physical record index.
    /// Physical records include blank/rejected lines and are not rollout ordinals.
    fn locate(&mut self, offset: u64) -> io::Result<usize> {
        if offset > self.table.size_decomp() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rollout checkpoint is past the file",
            ));
        }
        if offset == self.table.size_decomp() {
            return self.reader.total_records().map_err(io::Error::other);
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "rollout checkpoint is inside a record",
                ));
            }
            remaining -= record.len() as u64;
            index = index
                .checked_add(/*rhs*/ 1)
                .ok_or_else(|| io::Error::other("rollout record index overflow"))?;
        }
        Ok(index)
    }
}

#[cfg(test)]
#[path = "record_reader_tests.rs"]
mod tests;
