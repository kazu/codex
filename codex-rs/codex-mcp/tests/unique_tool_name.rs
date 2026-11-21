use std::collections::HashSet;

use codex_mcp::make_unique_tool_name;

#[test]
fn unique_tool_names_preserve_distinct_names_and_disambiguate_collisions() {
    let mut used = HashSet::new();
    let first = make_unique_tool_name("mcp__example__lookup", "first", &mut used);
    let second = make_unique_tool_name("mcp__example__lookup", "second", &mut used);
    let third = make_unique_tool_name(&"x".repeat(129), "third", &mut used);

    assert_eq!(first, "mcp__example__lookup");
    assert_ne!(first, second);
    assert!(second.len() <= 128);
    assert!(third.len() <= 128);
    assert_eq!(used.len(), 3);
}
