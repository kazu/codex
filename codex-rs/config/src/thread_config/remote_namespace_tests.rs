use super::model_provider_from_proto;
use super::model_provider_to_proto;
use super::proto;
use codex_model_provider_info::ModelProviderInfo;
use prost::Message;
use serde_json::json;

#[test]
fn remote_provider_preserves_namespace_tools_capability() {
    for enabled in [false, true] {
        let capabilities =
            proto::ModelProviderCapabilities::decode(&[0x10, u8::from(enabled)][..]).unwrap();
        let mut remote = model_provider_to_proto("lmstudio", ModelProviderInfo::default());
        remote.capabilities = Some(capabilities);

        let (_, provider) = model_provider_from_proto(remote).unwrap();
        let value = serde_json::to_value(&provider).unwrap();
        assert_eq!(value["capabilities"]["namespace_tools"], json!(enabled));
        let restored = model_provider_to_proto("lmstudio", provider);
        assert_eq!(
            restored.capabilities.unwrap().encode_to_vec(),
            vec![0x10, u8::from(enabled)]
        );
    }
}
