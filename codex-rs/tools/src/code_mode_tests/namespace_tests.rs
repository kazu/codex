use super::*;
use pretty_assertions::assert_eq;

#[test]
fn code_mode_tool_names_preserve_existing_underscore_boundaries() {
    for (namespace, tool, expected) in [
        ("mcp__example", "lookup", "mcp__example__lookup"),
        ("mcp__example_", "lookup", "mcp__example_lookup"),
        ("mcp__example", "_lookup", "mcp__example_lookup"),
        ("mcp__example_", "_lookup", "mcp__example__lookup"),
    ] {
        assert_eq!(
            code_mode_name_for_tool_name(&ToolName::namespaced(namespace, tool)),
            expected
        );
    }
}
