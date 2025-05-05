use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use cas_client::remote_client::PREFIX_DEFAULT;
use cas_client::{CacheConfig, CHUNK_CACHE_SIZE_BYTES};
use cas_object::CompressionScheme;
use utils::auth::AuthConfig;

use crate::errors::Result;
use crate::repo_salt::RepoSalt;

#[derive(Debug)]
pub enum Endpoint {
    Server(String),
    FileSystem(PathBuf),
}

#[derive(Debug)]
pub struct DataConfig {
    pub endpoint: Endpoint,
    pub compression: Option<CompressionScheme>,
    pub auth: Option<AuthConfig>,
    pub prefix: String,
    pub cache_config: CacheConfig,
    pub staging_directory: Option<PathBuf>,
}

#[derive(Debug)]
pub struct GlobalDedupConfig {
    pub global_dedup_policy: GlobalDedupPolicy,
}

#[derive(Debug)]
pub struct RepoInfo {
    pub repo_paths: Vec<String>,
}

#[derive(PartialEq, Default, Clone, Debug, Copy)]
pub enum GlobalDedupPolicy {
    /// Never query for new shards using chunk hashes.
    Never,

    /// Always query for new shards by chunks
    #[default]
    Always,
}

impl FromStr for GlobalDedupPolicy {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "never" => Ok(GlobalDedupPolicy::Never),
            "always" => Ok(GlobalDedupPolicy::Always),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid global dedup query policy, should be one of never, direct_only, always: {s}"),
            )),
        }
    }
}

#[derive(Debug)]
pub struct ShardConfig {
    pub prefix: String,
    pub session_directory: PathBuf,
    pub cache_directory: PathBuf,
    pub global_dedup_policy: GlobalDedupPolicy,
    pub repo_salt: RepoSalt,
}

#[derive(Debug)]
pub struct TranslatorConfig {
    pub data_config: DataConfig,
    pub shard_config: ShardConfig,
    pub repo_info: Option<RepoInfo>,
    pub session_id: Option<String>,
}

impl TranslatorConfig {
    pub fn local_config(base_dir: impl AsRef<Path>) -> Result<Arc<Self>> {
        let path = base_dir.as_ref().join("xet");
        std::fs::create_dir_all(&path)?;

        let translator_config = Self {
            data_config: DataConfig {
                endpoint: Endpoint::FileSystem(path.join("xorbs")),
                compression: Default::default(),
                auth: None,
                prefix: PREFIX_DEFAULT.into(),
                cache_config: CacheConfig {
                    cache_directory: path.join("cache"),
                    cache_size: *CHUNK_CACHE_SIZE_BYTES,
                },
                staging_directory: None,
            },
            shard_config: ShardConfig {
                prefix: PREFIX_DEFAULT.into(),
                cache_directory: path.join("shard-cache"),
                session_directory: path.join("shard-session"),
                global_dedup_policy: Default::default(),
                repo_salt: RepoSalt::default(),
            },
            repo_info: Some(RepoInfo {
                repo_paths: vec!["".into()],
            }),
            session_id: None,
        };

        Ok(Arc::new(translator_config))
    }
}
