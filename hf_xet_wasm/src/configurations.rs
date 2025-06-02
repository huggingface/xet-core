use cas_object::CompressionScheme;
use utils::auth::AuthConfig;

#[derive(Debug)]
pub struct DataConfig {
    pub endpoint: String,
    pub compression: Option<CompressionScheme>,
    pub auth: Option<AuthConfig>,
    pub prefix: String,
}

// Salt is 256-bit in length.
pub const REPO_SALT_LEN: usize = 32;

pub type RepoSalt = [u8; REPO_SALT_LEN];

#[derive(Debug)]
pub struct ShardConfig {
    pub prefix: String,
    pub repo_salt: RepoSalt,
}

#[derive(Debug)]
pub struct TranslatorConfig {
    pub data_config: DataConfig,
    pub shard_config: ShardConfig,
    pub session_id: String,
}
