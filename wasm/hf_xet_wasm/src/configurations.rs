use xet_client::cas_client::auth::AuthConfig;
use xet_core_structures::xorb_object::CompressionScheme;

// configurations for hf_xet_wasm components, generally less complicated than hf_xet/data crate configurations

#[derive(Debug)]
pub struct DataConfig {
    pub endpoint: String,
    pub compression: CompressionScheme,
    pub auth: Option<AuthConfig>,
    pub prefix: String,
    pub user_agent: String,
}

#[derive(Debug)]
pub struct ShardConfig {
    pub prefix: String,
}

#[derive(Debug)]
pub struct TranslatorConfig {
    pub data_config: DataConfig,
    pub shard_config: ShardConfig,
    pub session_id: String,
}
