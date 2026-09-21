//! Keeps a framed rollout's archive and pending file together when its rollout is moved.

use std::path::Path;
use std::path::PathBuf;

/// Adds the rollout's other framed file, when one exists beside `source`, to the move list of a
/// caller that moves `source` into `destination_dir`.
///
/// A pending file can remain next to its archive when publication stopped between persisting the
/// archive and removing the pending file. It is never read once the archive exists, but it must
/// travel with the rollout so the source directory is left clean and delete finds it.
pub(super) fn push_framed_sibling_moves(
    moves: &mut Vec<(PathBuf, PathBuf)>,
    source: &Path,
    destination_dir: &Path,
) {
    for sibling in codex_rollout::framed_rollout_paths(source) {
        if sibling != source
            && sibling.is_file()
            && let Some(file_name) = sibling.file_name()
        {
            let destination = destination_dir.join(file_name);
            moves.push((sibling, destination));
        }
    }
}

/// Removes the archive and pending file of a rollout a caller is discarding by its logical path.
///
/// A framed recorder reports the logical `.jsonl` path, so removing that path alone leaves the
/// files it actually wrote behind. Missing files are not an error, as for the plain removal.
pub(super) async fn remove_framed_files(rollout_path: &Path) {
    for framed_path in codex_rollout::framed_rollout_paths(rollout_path) {
        let _ = tokio::fs::remove_file(framed_path).await;
    }
}

#[cfg(test)]
#[path = "framed_moves_tests.rs"]
mod tests;
