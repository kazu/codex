use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::local::framed_test_support::convert_to_framed;
use crate::local::test_support::write_session_file;

#[tokio::test]
async fn remove_framed_files_discards_what_a_framed_recorder_wrote() {
    for storage in ["pending", "archive", "archive and stale pending"] {
        let home = TempDir::new().expect("temp dir");
        let path = write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(701))
            .expect("session file");
        let other = write_session_file(home.path(), "2025-01-03T12-00-01", Uuid::from_u128(702))
            .expect("other session file");
        let files = convert_to_framed(&path, storage);
        let other_files = convert_to_framed(&other, storage);

        remove_framed_files(&path).await;
        // Removing an already removed rollout is not an error.
        remove_framed_files(&path).await;

        assert_eq!(
            (
                files.iter().filter(|file| file.exists()).count(),
                other_files.iter().filter(|file| file.exists()).count(),
            ),
            (0, other_files.len()),
            "{storage}"
        );
    }
}
