use super::*;
use pretty_assertions::assert_eq;

#[test]
fn namespace_tools_provider_setting_survives_toml_round_trip() {
    for enabled in [false, true] {
        let config = format!("name = \"LM Studio\"\n[capabilities]\nnamespace_tools = {enabled}\n");
        let provider: ModelProviderInfo = toml::from_str(&config).unwrap();
        let serialized = toml::to_string(&provider).unwrap();
        let round_trip: toml::Value = toml::from_str(&serialized).unwrap();
        assert_eq!(
            round_trip
                .get("capabilities")
                .and_then(|capabilities| capabilities.get("namespace_tools"))
                .and_then(toml::Value::as_bool),
            Some(enabled)
        );
    }
}

#[test]
fn builtin_lmstudio_declares_namespace_tools_unsupported() {
    let provider = built_in_model_providers(None)
        .remove(LMSTUDIO_OSS_PROVIDER_ID)
        .unwrap();
    let serialized = toml::to_string(&provider).unwrap();
    let value: toml::Value = toml::from_str(&serialized).unwrap();
    assert_eq!(
        value
            .get("capabilities")
            .and_then(|capabilities| capabilities.get("namespace_tools"))
            .and_then(toml::Value::as_bool),
        Some(false)
    );
}
