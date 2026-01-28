use std::path::{Path, PathBuf};
use std::str::FromStr;

use cas_client::remote_client::PREFIX_DEFAULT;
use cas_object::CompressionScheme;
use utils::auth::AuthConfig;

use crate::errors::Result;

#[derive(Debug)]
pub enum Endpoint {
    Server(String),
    FileSystem(PathBuf),
    InMemory,
}

#[derive(Debug)]
pub struct DataConfig {
    pub endpoint: Endpoint,
    pub compression: Option<CompressionScheme>,
    pub auth: Option<AuthConfig>,
    pub prefix: String,
    pub staging_directory: Option<PathBuf>,
    pub user_agent: String,
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
}

#[derive(Debug)]
pub struct ProgressConfig {
    pub aggregate: bool,
}

#[derive(Debug)]
pub struct TranslatorConfig {
    pub data_config: DataConfig,
    pub shard_config: ShardConfig,
    pub repo_info: Option<RepoInfo>,
    pub session_id: Option<String>,
    pub progress_config: ProgressConfig,
}

impl TranslatorConfig {
    pub fn local_config(base_dir: impl AsRef<Path>) -> Result<Self> {
        let path = base_dir.as_ref().join("xet");
        std::fs::create_dir_all(&path)?;

        let translator_config = Self {
            data_config: DataConfig {
                endpoint: Endpoint::FileSystem(path.join("xorbs")),
                compression: Default::default(),
                auth: None,
                prefix: PREFIX_DEFAULT.into(),
                staging_directory: None,
                user_agent: String::new(),
            },
            shard_config: ShardConfig {
                prefix: PREFIX_DEFAULT.into(),
                cache_directory: path.join("shard-cache"),
                session_directory: path.join("shard-session"),
                global_dedup_policy: Default::default(),
            },
            repo_info: Some(RepoInfo {
                repo_paths: vec!["".into()],
            }),
            session_id: None,
            progress_config: ProgressConfig { aggregate: true },
        };

        Ok(translator_config)
    }

    /// Creates a TranslatorConfig that uses in-memory storage for XORBs.
    /// Shard data still uses file-based storage in the provided base directory.
    pub fn memory_config(base_dir: impl AsRef<Path>) -> Result<Self> {
        let path = base_dir.as_ref().join("xet");
        std::fs::create_dir_all(&path)?;

        let translator_config = Self {
            data_config: DataConfig {
                endpoint: Endpoint::InMemory,
                compression: Default::default(),
                auth: None,
                prefix: PREFIX_DEFAULT.into(),
                staging_directory: None,
                user_agent: String::new(),
            },
            shard_config: ShardConfig {
                prefix: PREFIX_DEFAULT.into(),
                cache_directory: path.join("shard-cache"),
                session_directory: path.join("shard-session"),
                global_dedup_policy: Default::default(),
            },
            repo_info: Some(RepoInfo {
                repo_paths: vec!["".into()],
            }),
            session_id: None,
            progress_config: ProgressConfig { aggregate: true },
        };

        Ok(translator_config)
    }

    /// Creates a TranslatorConfig that connects to a CAS server at the given endpoint.
    /// Shard cache and session directories are created under the provided base directory.
    /// Useful for tests that use LocalTestServer.
    pub fn test_server_config(endpoint: impl AsRef<str>, base_dir: impl AsRef<Path>) -> Result<Self> {
        let path = base_dir.as_ref().join("xet");
        std::fs::create_dir_all(&path)?;

        let translator_config = Self {
            data_config: DataConfig {
                endpoint: Endpoint::Server(endpoint.as_ref().to_string()),
                compression: Default::default(),
                auth: None,
                prefix: PREFIX_DEFAULT.into(),
                staging_directory: None,
                user_agent: String::new(),
            },
            shard_config: ShardConfig {
                prefix: PREFIX_DEFAULT.into(),
                cache_directory: path.join("shard-cache"),
                session_directory: path.join("shard-session"),
                global_dedup_policy: Default::default(),
            },
            repo_info: Some(RepoInfo {
                repo_paths: vec!["".into()],
            }),
            session_id: None,
            progress_config: ProgressConfig { aggregate: true },
        };

        Ok(translator_config)
    }

    pub fn disable_progress_aggregation(self) -> Self {
        Self {
            progress_config: ProgressConfig { aggregate: false },
            ..self
        }
    }

    pub fn with_session_id(self, session_id: &str) -> Self {
        if session_id.is_empty() {
            return self;
        }

        Self {
            session_id: Some(session_id.to_owned()),
            ..self
        }
    }
}
