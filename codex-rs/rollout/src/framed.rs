//! Records start in `.pendings.<archive filename>` after an eight-byte LE start
//! position (zero). Once the first archive is published, it is authoritative;
//! subsequent records are appended directly, including a short final frame.
//! The caller owns the rollout's existing writer guard. No power-loss guarantee.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use seekzstdsep::RecordReader;

pub(crate) struct FramedWriter {
    path: PathBuf,
    file: File,
    records_per_frame: NonZeroUsize,
    // None means the archive is published and receives records directly.
    pending_records: Option<usize>,
}

/// The stored records as [`FramedWriter::open_with_records`] left them after opening.
pub(crate) enum StoredRecords {
    Archive(RecordReader),
    /// The pending file's JSONL bytes, without its start header.
    Pending(Vec<u8>),
}

impl FramedWriter {
    pub(crate) fn open(path: &Path, records_per_frame: NonZeroUsize) -> io::Result<Self> {
        Ok(Self::open_with_records(path, records_per_frame)?.0)
    }

    /// Also hands back what opening had to read, so a resume derives its ordinal state without
    /// opening the storage a second time.
    pub(crate) fn open_with_records(
        path: &Path,
        records_per_frame: NonZeroUsize,
    ) -> io::Result<(Self, StoredRecords)> {
        let archive = get_archive_path(path);
        let (file, records_per_frame, pending_records, records) =
            match OpenOptions::new().read(true).write(true).open(&archive) {
                Ok(file) => {
                    let reader = RecordReader::from_file(archive, file.try_clone()?, b"\n")
                        .map_err(io::Error::other)?;
                    let records_per_frame = NonZeroUsize::new(reader.records_per_frame())
                        .ok_or_else(|| io::Error::other("empty rollout archive"))?;
                    (
                        file,
                        records_per_frame,
                        None,
                        StoredRecords::Archive(reader),
                    )
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let mut file = OpenOptions::new()
                        .read(true)
                        .append(true)
                        .create(true)
                        .open(get_pending_path(path)?)?;
                    let len = file.metadata()?.len();
                    if len == 0 {
                        write_start(&mut file, /*start*/ 0)?;
                    } else if len > 8 {
                        // A resumed pending file gets the same tail repair as a plain rollout.
                        crate::recorder::ensure_rollout_is_newline_terminated(&mut file)?;
                    }

                    file.rewind()?;
                    read_start(&mut file)?;
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes)?;
                    let count = bytes.iter().filter(|byte| **byte == b'\n').count();
                    (
                        file,
                        records_per_frame,
                        Some(count),
                        StoredRecords::Pending(bytes),
                    )
                }
                Err(error) => return Err(error),
            };

        let writer = Self {
            path: path.to_owned(),
            file,
            records_per_frame,
            pending_records,
        };
        Ok((writer, records))
    }

    /// The archive's frame size once published; until then the size it will publish with.
    pub(crate) fn records_per_frame(&self) -> NonZeroUsize {
        self.records_per_frame
    }

    /// The caller supplies one serialized record including its newline.
    /// Failed writes may leave partial data; recovery must be explicitly requested.
    pub(crate) fn append(&mut self, line: &[u8]) -> io::Result<()> {
        if let Some(count) = &mut self.pending_records {
            self.file.write_all(line)?;
            *count += 1;
            return Ok(());
        }

        // Every frame before the last holds exactly `records_per_frame` records: the archive is
        // published with that limit and only ever extended here, so the count is trusted and
        // earlier frames are not decoded on each append.
        seekzstdsep::append_records_with_opts(
            &mut self.file,
            line,
            b"\n",
            seekzstdsep::OnMissingSeparator::Refuse,
            seekzstdsep::CompressOptions::default().level,
            Some(self.records_per_frame.get()),
            /*trust_records_per_frame*/ true,
        )
        .map_err(|error| {
            io::Error::other(error.context(format!(
                "compressed rollout append failed; stop all writers, then run \
                 `codex debug rollout-recovery <ARCHIVE> --records-per-frame {}`. Archive: {}",
                self.records_per_frame,
                get_archive_path(&self.path).display(),
            )))
        })
    }

    /// Publishes the first archive once N records are available; later calls do nothing.
    pub(crate) fn promote(&mut self) -> io::Result<()> {
        if self
            .pending_records
            .is_none_or(|count| count < self.records_per_frame.get())
        {
            return Ok(());
        }

        self.file.rewind()?;
        read_start(&mut self.file)?;
        let size: usize = self
            .file
            .metadata()?
            .len()
            .try_into()
            .map_err(io::Error::other)?;

        self.file = write_archive(
            &get_archive_path(&self.path),
            &mut self.file,
            size,
            self.records_per_frame,
        )?;
        self.pending_records = None;
        std::fs::remove_file(get_pending_path(&self.path)?)?;
        Ok(())
    }
}

/// Rebuilds a seekable archive after all writers have been stopped. Takes no lock.
/// Decode or compression errors stop recovery without replacing the source.
/// This does not recover lost records or salvage a partially decoded prefix.
pub fn recovery(path: &Path, records_per_frame: NonZeroUsize) -> io::Result<()> {
    let bytes = zstd::stream::decode_all(File::open(path)?)?;
    if !bytes.ends_with(b"\n") {
        return Err(io::Error::other(
            "recovery requires complete newline-terminated records",
        ));
    }

    write_archive(path, bytes.as_slice(), bytes.len(), records_per_frame)?;
    Ok(())
}

fn write_archive(
    path: &Path,
    input: impl Read,
    size: usize,
    records_per_frame: NonZeroUsize,
) -> io::Result<File> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut output = tempfile::NamedTempFile::new_in(parent)?;
    seekzstdsep::convert_to_seekable_zst_reader_with_opts(
        input,
        &mut output,
        size.max(32 * 1024),
        /*is_same_separator_cnt*/ true,
        b"\n",
        /*limit_multiplier*/ Some(1),
        Some(seekzstdsep::CompressOptions {
            max_of_separator: Some(records_per_frame.get()),
            ..Default::default()
        }),
    )
    .map_err(io::Error::other)?;
    output.persist(path).map_err(|error| error.error)
}

fn write_start(writer: &mut impl Write, start: u64) -> io::Result<()> {
    writer.write_all(&start.to_le_bytes())
}

pub(crate) fn read_start(reader: &mut impl Read) -> io::Result<u64> {
    let mut header = [0; 8];
    reader.read_exact(&mut header)?;
    Ok(u64::from_le_bytes(header))
}

const ARCHIVE_SUFFIX: &str = ".jsonl.seek.zst";
const PENDING_PREFIX: &str = ".pendings.";

/// Returns whether a file name is a framed archive or its pending file.
pub(crate) fn is_framed_file_name(name: &str) -> bool {
    name.ends_with(ARCHIVE_SUFFIX)
}

/// Returns the logical `.jsonl` file name for an archive or pending file name.
pub(crate) fn get_plain_file_name(name: &str) -> Option<String> {
    let stem = strip_pending_prefix(name.strip_suffix(ARCHIVE_SUFFIX)?);
    Some(format!("{stem}.jsonl"))
}

/// Returns a file name without the pending prefix, so the name starts with `rollout-` again.
pub(crate) fn strip_pending_prefix(name: &str) -> &str {
    name.strip_prefix(PENDING_PREFIX).unwrap_or(name)
}

pub(crate) fn get_archive_path(path: &Path) -> PathBuf {
    path.with_extension(&ARCHIVE_SUFFIX[1..])
}

pub(crate) fn get_pending_path(path: &Path) -> io::Result<PathBuf> {
    let archive = get_archive_path(path);
    let filename = archive.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "rollout path must name a file")
    })?;
    let mut name = std::ffi::OsString::from(PENDING_PREFIX);
    name.push(filename);
    Ok(archive.with_file_name(name))
}

#[cfg(test)]
#[path = "framed_tests.rs"]
mod tests;
