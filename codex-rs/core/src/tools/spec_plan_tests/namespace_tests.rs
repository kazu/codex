use super::*;
use pretty_assertions::assert_eq;

fn disable_namespace_tools(turn: &mut TurnContext) {
    set_namespace_tools(turn, false);
}

fn set_namespace_tools(turn: &mut TurnContext, enabled: bool) {
    let mut value = serde_json::to_value(&turn.config.model_provider).unwrap();
    value["capabilities"] = json!({ "namespace_tools": enabled });
    let provider: ModelProviderInfo = serde_json::from_value(value).unwrap();
    update_config(turn, |config| config.model_provider = provider.clone());
    turn.provider = create_model_provider(provider, turn.auth_manager.clone());
}

#[tokio::test]
async fn provider_without_namespace_tools_exposes_mcp_as_function() {
    let probe = probe_with(
        disable_namespace_tools,
        ToolPlanInputs {
            tool_runtimes: vec![mcp_runtime(
                "direct",
                "mcp__direct",
                "lookup",
                ToolExposure::Direct,
            )],
            ..ToolPlanInputs::default()
        },
    )
    .await;

    let tool = probe
        .visible_specs
        .iter()
        .find_map(|spec| match spec {
            ToolSpec::Function(tool) if tool.name.starts_with("mcp__direct__lookup_") => Some(tool),
            _ => None,
        })
        .expect("the MCP tool must be advertised as a function");
    assert!(tool.description.contains("Tools from direct."));
    probe.assert_visible_lacks(&["mcp__direct"]);
}

#[tokio::test]
async fn builtin_lmstudio_exposes_mcp_as_function() {
    let probe = probe_with(
        |turn| {
            let provider = codex_model_provider_info::built_in_model_providers(None)
                .remove(codex_model_provider_info::LMSTUDIO_OSS_PROVIDER_ID)
                .unwrap();
            update_config(turn, |config| config.model_provider = provider.clone());
            turn.provider = create_model_provider(provider, turn.auth_manager.clone());
        },
        ToolPlanInputs {
            tool_runtimes: vec![mcp_runtime(
                "direct",
                "mcp__direct",
                "lookup",
                ToolExposure::Direct,
            )],
            ..ToolPlanInputs::default()
        },
    )
    .await;

    assert!(probe.visible_specs.iter().any(|spec| matches!(
        spec,
        ToolSpec::Function(tool) if tool.name.starts_with("mcp__direct__lookup_")
    )));
    probe.assert_visible_lacks(&["mcp__direct"]);
}

#[tokio::test]
async fn provider_with_namespace_tools_keeps_mcp_namespace() {
    let probe = probe_with(
        |turn| set_namespace_tools(turn, true),
        ToolPlanInputs {
            tool_runtimes: vec![mcp_runtime(
                "direct",
                "mcp__direct",
                "lookup",
                ToolExposure::Direct,
            )],
            ..ToolPlanInputs::default()
        },
    )
    .await;

    probe.assert_visible_contains(&["mcp__direct"]);
    assert_eq!(probe.namespace_function_names("mcp__direct"), &["lookup"]);
}

#[tokio::test]
async fn provider_without_namespace_tools_resolves_mcp_function_to_original_tool() {
    let (_session, mut turn) = make_session_and_context().await;
    disable_namespace_tools(&mut turn);
    let router = plan_with_model(
        &turn,
        turn.model_info(),
        ToolPlanInputs {
            tool_runtimes: vec![mcp_runtime(
                "direct",
                "mcp__direct",
                "lookup",
                ToolExposure::Direct,
            )],
            ..ToolPlanInputs::default()
        },
    );

    let specs = router.model_visible_specs();
    let name = specs
        .iter()
        .find_map(|spec| match spec {
            ToolSpec::Function(tool) if tool.name.starts_with("mcp__direct__lookup_") => {
                Some(tool.name.as_str())
            }
            _ => None,
        })
        .expect("the MCP tool must be advertised");
    let tool = router
        .registered_tool(&ToolName::plain(name))
        .expect("the advertised name must resolve to the MCP tool");
    assert_eq!(
        tool.runtime.tool_name(),
        ToolName::namespaced("mcp__direct", "lookup")
    );
    let item = serde_json::from_value(json!({
        "type": "function_call",
        "name": name,
        "arguments": "{}",
        "call_id": "call-lookup"
    }))
    .unwrap();
    let call = router.build_tool_call(item).unwrap().unwrap();
    assert_eq!(
        call.tool_name,
        ToolName::namespaced("mcp__direct", "lookup")
    );
}

#[tokio::test]
async fn flattened_mcp_name_collisions_keep_both_tools_callable() {
    let (_session, mut turn) = make_session_and_context().await;
    disable_namespace_tools(&mut turn);
    let router = plan_with_model(
        &turn,
        turn.model_info(),
        ToolPlanInputs {
            tool_runtimes: vec![
                mcp_runtime("first", "mcp__example", "lookup", ToolExposure::Direct),
                mcp_runtime("second", "mcp__example_", "_lookup", ToolExposure::Direct),
            ],
            ..ToolPlanInputs::default()
        },
    );

    let specs = router.model_visible_specs();
    let mut resolved = Vec::new();
    for spec in specs.iter() {
        if let ToolSpec::Function(tool) = spec
            && (tool.description.contains("lookup test tool")
                || tool.description.contains("_lookup test tool"))
        {
            let registered = router
                .registered_tool(&ToolName::plain(&tool.name))
                .expect("every advertised tool must be callable");
            resolved.push(registered.runtime.tool_name());
        }
    }
    assert_eq!(resolved.len(), 2);
    assert!(resolved.contains(&ToolName::namespaced("mcp__example", "lookup")));
    assert!(resolved.contains(&ToolName::namespaced("mcp__example_", "_lookup")));
}

#[tokio::test]
async fn flattened_mcp_name_keeps_its_target_when_a_collision_is_added() {
    let (_session, mut turn) = make_session_and_context().await;
    disable_namespace_tools(&mut turn);
    let original = plan_with_model(
        &turn,
        turn.model_info(),
        ToolPlanInputs {
            tool_runtimes: vec![mcp_runtime(
                "second",
                "mcp__example_",
                "_lookup",
                ToolExposure::Direct,
            )],
            ..ToolPlanInputs::default()
        },
    );
    let original_specs = original.model_visible_specs();
    let original_name = original_specs
        .iter()
        .find_map(|spec| match spec {
            ToolSpec::Function(tool) if tool.description.contains("_lookup test tool") => {
                Some(tool.name.clone())
            }
            _ => None,
        })
        .expect("the original MCP tool must be advertised");

    let refreshed = plan_with_model(
        &turn,
        turn.model_info(),
        ToolPlanInputs {
            tool_runtimes: vec![
                mcp_runtime("first", "mcp__example", "lookup", ToolExposure::Direct),
                mcp_runtime("second", "mcp__example_", "_lookup", ToolExposure::Direct),
            ],
            ..ToolPlanInputs::default()
        },
    );
    let resolved = refreshed
        .registered_tool(&ToolName::plain(original_name))
        .expect("the earlier name must remain callable");
    assert_eq!(
        resolved.runtime.tool_name(),
        ToolName::namespaced("mcp__example_", "_lookup")
    );
}

#[tokio::test]
async fn hidden_mcp_tools_do_not_gain_flat_model_call_names() {
    let (_session, mut turn) = make_session_and_context().await;
    disable_namespace_tools(&mut turn);
    let router = plan_with_model(
        &turn,
        turn.model_info(),
        ToolPlanInputs {
            tool_runtimes: vec![mcp_runtime(
                "hidden",
                "mcp__hidden",
                "lookup",
                ToolExposure::Hidden,
            )],
            ..ToolPlanInputs::default()
        },
    );

    assert!(
        router
            .registered_tool(&ToolName::plain("mcp__hidden__lookup"))
            .is_none()
    );
}

#[tokio::test]
async fn provider_without_namespace_tools_flattens_mcp_search_results() {
    let (session, mut turn) = make_session_and_context().await;
    disable_namespace_tools(&mut turn);
    update_turn_settings_for_test(&mut turn, |settings| {
        Arc::make_mut(&mut settings.model_info).supports_search_tool = true;
    });
    let router = Arc::new(plan_with_model(
        &turn,
        turn.model_info(),
        ToolPlanInputs {
            tool_runtimes: vec![mcp_runtime(
                "searchable",
                "mcp__searchable",
                "lookup",
                ToolExposure::Deferred,
            )],
            ..ToolPlanInputs::default()
        },
    ));
    let mut step = StepContext::for_test(Arc::new(turn));
    Arc::get_mut(&mut step).unwrap().tool_router = Arc::clone(&router);
    let item = serde_json::from_value(json!({
        "type": "tool_search_call",
        "call_id": "search",
        "execution": "client",
        "arguments": { "query": "lookup" }
    }))
    .unwrap();
    let call = router.build_tool_call(item).unwrap().unwrap();
    let result = router
        .dispatch_tool_call_with_code_mode_result(
            Arc::new(session),
            step,
            tokio_util::sync::CancellationToken::new(),
            Arc::new(tokio::sync::Mutex::new(
                crate::turn_diff_tracker::TurnDiffTracker::new(),
            )),
            call,
            crate::tools::context::ToolCallSource::Direct,
        )
        .await
        .unwrap();
    let response = serde_json::to_value(result.into_response(false).item).unwrap();
    let tools = response["tools"].as_array().unwrap();
    let tool = tools
        .iter()
        .find(|tool| {
            tool["type"] == "function"
                && tool["name"]
                    .as_str()
                    .is_some_and(|name| name.starts_with("mcp__searchable__lookup_"))
        })
        .expect("the search result must expose the MCP function");
    assert!(
        tool["description"]
            .as_str()
            .unwrap()
            .contains("Tools from searchable.")
    );
}
