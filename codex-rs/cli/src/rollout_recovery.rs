use std::num::NonZeroUsize;
use std::path::PathBuf;

#[derive(Debug, clap::Parser)]
pub(crate) struct RecoveryCommand {
    /// Archive to rebuild. Stop all writers first; no lock is acquired.
    #[arg(value_name = "ARCHIVE")]
    pub archive: PathBuf,

    /// Maximum records per frame in the rebuilt archive.
    #[arg(long, default_value = "255")]
    pub records_per_frame: NonZeroUsize,
}
