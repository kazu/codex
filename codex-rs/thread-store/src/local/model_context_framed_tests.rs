use pretty_assertions::assert_eq;

use super::*;

#[tokio::test]
async fn replays_framed_lineage_like_plain_lineage() {
    let home = TempDir::new().expect("temp dir");
    let root_uuid = Uuid::from_u128(/*v*/ 3001);
    let root_id = ThreadId::from_string(&root_uuid.to_string()).expect("root id");
    let root_path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-02-00",
        root_uuid,
        [
            user_message("root before checkpoint"),
            compacted("root checkpoint", Some(Vec::new())),
            turn_started("root-excluded"),
            user_message("root after cutoff"),
        ],
    );
    let child_uuid = Uuid::from_u128(/*v*/ 3002);
    let child_id = ThreadId::from_string(&child_uuid.to_string()).expect("child id");
    let child_path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-02-01",
        child_uuid,
        [
            turn_started("child-turn"),
            user_message("child local"),
            turn_context(home.path(), "child-turn"),
            turn_complete("child-turn"),
        ],
    );
    set_history_base(
        child_path.as_path(),
        history_position(
            root_path.as_path(),
            root_id,
            /*end_ordinal_exclusive*/ 3,
        ),
    );
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let params = || LoadThreadHistoryParams {
        thread_id: child_id,
        include_archived: false,
    };

    let plain = store
        .load_latest_model_context(params())
        .await
        .expect("load plain lineage model context");
    assert_eq!(plain.items.len(), 7);

    // Frames of two records put the frozen root cutoff in the middle of the archive, and the
    // stored byte position must select the same records as in the plain file.
    for path in [&root_path, &child_path] {
        let archive = path.with_extension("jsonl.seek.zst");
        let input = std::fs::File::open(path).expect("open rollout");
        let output = std::fs::File::create(&archive).expect("create archive");
        zstd::stream::copy_encode(input, output, /*level*/ 3).expect("compress rollout");
        codex_rollout::recovery(&archive, std::num::NonZeroUsize::new(/*n*/ 2).unwrap())
            .expect("frame existing records");
        std::fs::remove_file(path).expect("remove plain rollout");
    }
    let framed = store
        .load_latest_model_context(params())
        .await
        .expect("load framed lineage model context");

    assert_eq!(
        serde_json::to_value(framed.items).expect("serialize framed context"),
        serde_json::to_value(plain.items).expect("serialize plain context")
    );
    assert!([root_path, child_path].iter().all(|path| !path.exists()));
}
