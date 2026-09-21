use pretty_assertions::assert_eq;

use super::rollout_path_is_resumable;

#[test]
fn framed_rollout_is_resumable_through_its_logical_path() {
    let results = [
        "rollout.jsonl",
        "rollout.jsonl.seek.zst",
        ".pendings.rollout.jsonl.seek.zst",
    ]
    .map(|name| {
        let home = tempfile::tempdir().expect("temp dir");
        let logical_path = home.path().join("rollout.jsonl");
        let missing = rollout_path_is_resumable(&logical_path);
        std::fs::write(home.path().join(name), b"stored records\n").expect("write rollout");
        (missing, rollout_path_is_resumable(&logical_path))
    });

    assert_eq!(results, [(false, true); 3]);
}

#[test]
fn rollout_without_records_is_not_resumable_in_either_format() {
    // A crash after creating the file leaves an empty plain file or a header-only pending file.
    let results = [
        ("rollout.jsonl", [0u8; 0].as_slice()),
        (".pendings.rollout.jsonl.seek.zst", &[0u8; 8]),
    ]
    .map(|(name, bytes)| {
        let home = tempfile::tempdir().expect("temp dir");
        std::fs::write(home.path().join(name), bytes).expect("write rollout");
        rollout_path_is_resumable(&home.path().join("rollout.jsonl"))
    });

    assert_eq!(results, [false, false]);
}
