use std::path::Path;
use std::path::PathBuf;

/// Replaces a plain rollout file with framed storage and returns the files that now exist.
///
/// `storage` is `"archive"`, `"pending"`, or `"archive and stale pending"`.
pub(super) fn convert_to_framed(plain_path: &Path, storage: &str) -> Vec<PathBuf> {
    let [archive, pending] = codex_rollout::framed_rollout_paths(plain_path)
        .try_into()
        .expect("archive and pending paths");
    let bytes = std::fs::read(plain_path).expect("read plain rollout");
    let mut files = Vec::new();
    if storage != "pending" {
        let compressed = zstd::stream::encode_all(bytes.as_slice(), /*level*/ 3).expect("compress");
        std::fs::write(&archive, compressed).expect("write archive");
        codex_rollout::recovery(&archive, std::num::NonZeroUsize::new(/*n*/ 2).unwrap())
            .expect("frame existing records");
        files.push(archive);
    }
    if storage != "archive" {
        let body = if storage == "pending" {
            bytes.as_slice()
        } else {
            b"stale\n"
        };
        std::fs::write(&pending, [0u64.to_le_bytes().as_slice(), body].concat())
            .expect("write pending");
        files.push(pending);
    }
    std::fs::remove_file(plain_path).expect("remove plain rollout");
    files
}
