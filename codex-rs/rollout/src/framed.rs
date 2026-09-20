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

impl FramedWriter {
    pub(crate) fn open(path: &Path, records_per_frame: NonZeroUsize) -> io::Result<Self> {
        let archive = path.with_extension("seek.zst");
        let (file, records_per_frame, pending_records) =
            match OpenOptions::new().read(true).write(true).open(&archive) {
                Ok(file) => {
                    let reader = RecordReader::from_file(archive, file.try_clone()?, b"\n")
                        .map_err(io::Error::other)?;
                    let records_per_frame = NonZeroUsize::new(reader.records_per_frame())
                        .ok_or_else(|| io::Error::other("empty rollout archive"))?;
                    (file, records_per_frame, None)
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let mut file = OpenOptions::new()
                        .read(true)
                        .append(true)
                        .create(true)
                        .open(get_pending_path(path)?)?;
                    if file.metadata()?.len() == 0 {
                        write_start(&mut file, /*start*/ 0)?;
                    }

                    file.rewind()?;
                    read_start(&mut file)?;
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes)?;
                    let count = bytes.iter().filter(|byte| **byte == b'\n').count();
                    (file, records_per_frame, Some(count))
                }
                Err(error) => return Err(error),
            };

        Ok(Self {
            path: path.to_owned(),
            file,
            records_per_frame,
            pending_records,
        })
    }

    /// The caller supplies one serialized record including its newline.
    /// Failed writes may leave partial data; compressed append recovery is deferred.
    pub(crate) fn append(&mut self, line: &[u8]) -> io::Result<()> {
        if let Some(count) = &mut self.pending_records {
            self.file.write_all(line)?;
            *count += 1;
            return Ok(());
        }

        // TODO: Add compressed-append failure handling here before retrying writes.
        seekzstdsep::append_records(
            &mut self.file,
            line,
            b"\n",
            seekzstdsep::OnMissingSeparator::Refuse,
            seekzstdsep::CompressOptions::default().level,
            Some(self.records_per_frame.get()),
        )
        .map_err(io::Error::other)
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

        let parent = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut output = tempfile::NamedTempFile::new_in(parent)?;
        seekzstdsep::convert_to_seekable_zst_reader_with_opts(
            &mut self.file,
            &mut output,
            size.max(32 * 1024),
            /*is_same_separator_cnt*/ true,
            b"\n",
            /*limit_multiplier*/ Some(1),
            Some(seekzstdsep::CompressOptions {
                max_of_separator: Some(self.records_per_frame.get()),
                ..Default::default()
            }),
        )
        .map_err(io::Error::other)?;

        self.file = output
            .persist(self.path.with_extension("seek.zst"))
            .map_err(|error| error.error)?;
        self.pending_records = None;
        std::fs::remove_file(get_pending_path(&self.path)?)?;
        Ok(())
    }
}

fn write_start(writer: &mut impl Write, start: u64) -> io::Result<()> {
    writer.write_all(&start.to_le_bytes())
}

fn read_start(reader: &mut impl Read) -> io::Result<u64> {
    let mut header = [0; 8];
    reader.read_exact(&mut header)?;
    Ok(u64::from_le_bytes(header))
}

fn get_pending_path(path: &Path) -> io::Result<PathBuf> {
    let archive = path.with_extension("seek.zst");
    let filename = archive.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "rollout path must name a file")
    })?;
    let mut name = std::ffi::OsString::from(".pendings.");
    name.push(filename);
    Ok(archive.with_file_name(name))
}

#[cfg(test)]
#[path = "framed_tests.rs"]
mod tests;
