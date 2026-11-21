use super::*;

const NAMESPACE_PREFIX: &str = "__codex_ns__";
const PLAIN_PREFIX: &str = "__codex_plain__";

impl ToolRegistry {
    pub(crate) fn configure_flat_tool_names(
        &mut self,
        visible_specs: &[ToolSpec],
        search_visible: bool,
    ) {
        let is_reserved =
            |name: &str| name.starts_with(NAMESPACE_PREFIX) || name.starts_with(PLAIN_PREFIX);
        let mut used_names: HashSet<String> = self
            .tools
            .keys()
            .filter(|name| name.is_default_namespace() && !is_reserved(&name.name))
            .map(|name| name.name.clone())
            .collect();
        let mut names = self
            .tools
            .iter()
            .filter(|(name, tool)| {
                search_visible
                    && tool.exposure.is_deferred()
                    && (!name.is_default_namespace() || is_reserved(&name.name))
            })
            .map(|(name, _)| name.clone())
            .collect::<BTreeSet<_>>();
        for spec in visible_specs {
            match spec {
                ToolSpec::Namespace(namespace) => {
                    for tool in &namespace.tools {
                        let tool_name = match tool {
                            ResponsesApiNamespaceTool::Function(tool) => &tool.name,
                            ResponsesApiNamespaceTool::Custom(tool) => &tool.name,
                        };
                        let name = ToolName::namespaced(&namespace.name, tool_name)
                            .with_default_namespace();
                        if !name.is_default_namespace() || is_reserved(&name.name) {
                            names.insert(name);
                        } else {
                            used_names.insert(name.name);
                        }
                    }
                }
                _ if is_reserved(spec.name()) => {
                    names.insert(ToolName::plain(spec.name()).with_default_namespace());
                }
                _ => {
                    used_names.insert(spec.name().to_string());
                }
            }
        }
        let mut flat_names = BTreeMap::new();
        for name in names {
            let preferred = if name.is_default_namespace() {
                format!("{PLAIN_PREFIX}{}", name.name)
            } else {
                format!("{NAMESPACE_PREFIX}{}", code_mode_name_for_tool_name(&name))
            };
            used_names.insert(preferred.clone());
            let identity = format!(
                "{}\0{}",
                name.namespace.as_deref().unwrap_or_default(),
                name.name
            );
            let flat = codex_mcp::make_unique_tool_name(&preferred, &identity, &mut used_names);
            self.flat_tool_aliases.insert(
                ToolName::plain(&flat).with_default_namespace(),
                name.clone(),
            );
            flat_names.insert(name, flat);
        }
        self.flat_tool_names = Some(flat_names);
    }

    pub(crate) fn resolve_tool_name(&self, name: ToolName) -> ToolName {
        if let Some(original) = self.flat_tool_aliases.get(&name) {
            return original.clone();
        }
        if self.flat_tool_names.is_some()
            && name.is_default_namespace()
            && (name.name.starts_with(NAMESPACE_PREFIX) || name.name.starts_with(PLAIN_PREFIX))
        {
            let mut namespace = "__codex_unknown_flat_tool__".to_string();
            while self
                .tools
                .contains_key(&ToolName::namespaced(&namespace, &name.name))
            {
                namespace.push('_');
            }
            return ToolName::namespaced(namespace, name.name);
        }
        name
    }

    pub(crate) fn flatten_tool_specs(&self, specs: Vec<ToolSpec>) -> Vec<ToolSpec> {
        let Some(flat_names) = &self.flat_tool_names else {
            return specs;
        };
        let mut flattened = Vec::new();
        for spec in specs {
            let ToolSpec::Namespace(namespace) = spec else {
                let mut spec = spec;
                let name = ToolName::plain(spec.name()).with_default_namespace();
                if let Some(flat) = flat_names.get(&name) {
                    match &mut spec {
                        ToolSpec::Function(tool) => tool.name.clone_from(flat),
                        ToolSpec::Freeform(tool) => tool.name.clone_from(flat),
                        _ => {}
                    }
                }
                flattened.push(spec);
                continue;
            };
            for mut tool in namespace.tools {
                let (tool_name, description) = match &mut tool {
                    ResponsesApiNamespaceTool::Function(tool) => {
                        (&mut tool.name, &mut tool.description)
                    }
                    ResponsesApiNamespaceTool::Custom(tool) => {
                        (&mut tool.name, &mut tool.description)
                    }
                };
                let name =
                    ToolName::namespaced(&namespace.name, &*tool_name).with_default_namespace();
                if let Some(flat) = flat_names.get(&name) {
                    tool_name.clone_from(flat);
                }
                if !namespace.description.is_empty() {
                    prepend_description(&namespace.description, description);
                }
                flattened.push(match tool {
                    ResponsesApiNamespaceTool::Function(tool) => ToolSpec::Function(tool),
                    ResponsesApiNamespaceTool::Custom(tool) => ToolSpec::Freeform(tool),
                });
            }
        }
        flattened
    }

    pub(crate) fn flatten_search_specs(
        &self,
        specs: Vec<LoadableToolSpec>,
    ) -> Vec<LoadableToolSpec> {
        if self.flat_tool_names.is_none() {
            return specs;
        }
        self.flatten_tool_specs(specs.into_iter().map(ToolSpec::from).collect())
            .into_iter()
            .map(|spec| match spec {
                ToolSpec::Function(tool) => LoadableToolSpec::Function(tool),
                ToolSpec::Freeform(tool) => LoadableToolSpec::Custom(tool),
                ToolSpec::Namespace(namespace) => LoadableToolSpec::Namespace(namespace),
                _ => unreachable!(
                    "loadable tool specifications contain only functions and namespaces"
                ),
            })
            .collect()
    }
}
