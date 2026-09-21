use codex_rollout::ThreadItem;
use pretty_assertions::assert_eq;
use uuid::Uuid;

use super::*;

#[test]
fn stored_thread_from_framed_rollout_item_matches_the_plain_item() {
    let uuid = Uuid::from_u128(/*v*/ 2);
    let stored = |file_name: String| {
        stored_thread_from_rollout_item(
            ThreadItem {
                path: PathBuf::from("/tmp/sessions/2025/01/03").join(file_name),
                ..Default::default()
            },
            /*archived*/ false,
            "test-provider",
        )
        .map(|thread| (thread.thread_id, thread.rollout_path))
    };
    let plain = stored(format!("rollout-2025-01-03T12-00-00-{uuid}.jsonl"));

    assert!(plain.is_some());
    assert_eq!(
        stored(format!("rollout-2025-01-03T12-00-00-{uuid}.seek.zst")),
        plain
    );
    assert_eq!(
        stored(format!(
            ".pendings.rollout-2025-01-03T12-00-00-{uuid}.seek.zst"
        )),
        plain
    );
}
