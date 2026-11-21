# Configuration

For basic configuration instructions, see [this documentation](https://developers.openai.com/codex/config-basic).

For advanced configuration instructions, see [this documentation](https://developers.openai.com/codex/config-advanced).

For a full configuration reference, see [this documentation](https://developers.openai.com/codex/config-reference).

## Lifecycle hooks

Admins can set top-level `allow_managed_hooks_only = true` in
`requirements.toml` to ignore user, project, and session hook configs while
still allowing managed hooks from requirements and managed config layers. This
setting is only supported in `requirements.toml`; putting it in `config.toml`
does not enable managed-hooks-only mode.

## Providers without namespace tools

Providers that do not accept namespace-wrapped tools can expose MCP tools as
ordinary functions by setting `namespace_tools = false`:

```toml
[model_providers.my_provider.capabilities]
namespace_tools = false
```

The built-in LM Studio provider uses this setting by default. Other providers
keep namespace tools enabled unless the setting is explicitly disabled.
