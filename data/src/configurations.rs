use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;
use ulid::Ulid;
use utils::auth::{AuthConfig, TokenRefresher};
use xet_runtime::{xet_cache_root, xet_config};

use crate::errors::Result;

/// Session-specific configuration that varies per upload/download session.
/// These are runtime values that cannot be configured via environment variables.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// The endpoint URL. Use the `local://` prefix (configurable via `HF_XET_DATA_LOCAL_CAS_SCHEME`)
    /// to specify a local filesystem path instead of a remote server.
    pub endpoint: String,
    pub user_agent: String,
    pub auth: Option<AuthConfig>,
    pub repo_paths: Vec<String>,
    pub session_id: Option<String>,
}

impl SessionConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            user_agent: String::new(),
            auth: None,
            repo_paths: Vec::new(),
            session_id: None,
        }
    }

    /// Returns true if this endpoint points to a local filesystem path.
    pub fn is_local(&self) -> bool {
        self.endpoint.starts_with(&xet_config().data.local_cas_scheme)
    }

    /// Returns the local filesystem path if this is a local endpoint.
    pub fn local_path(&self) -> Option<PathBuf> {
        if self.is_local() {
            let path = self.endpoint.strip_prefix(&xet_config().data.local_cas_scheme)?;
            Some(PathBuf::from(path))
        } else {
            None
        }
    }

    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    pub fn with_auth(mut self, auth: Option<AuthConfig>) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_repo_paths(mut self, repo_paths: Vec<String>) -> Self {
        self.repo_paths = repo_paths;
        self
    }

    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        let id = session_id.into();
        if !id.is_empty() {
            self.session_id = Some(id);
        }
        self
    }

    /// Creates a new SessionConfig with authentication configured from token info.
    /// This is the standard way to create a session config for upload/download operations.
    pub fn with_default_auth(
        endpoint: impl Into<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
        user_agent: impl Into<String>,
    ) -> Self {
        let (token, token_expiration) = token_info.unzip();
        let auth_cfg = AuthConfig::maybe_new(token, token_expiration, token_refresher);

        Self::new(endpoint)
            .with_user_agent(user_agent)
            .with_auth(auth_cfg)
            .with_repo_paths(vec!["".into()])
            .with_session_id(Ulid::new().to_string())
    }

    /// Creates a SessionConfig for local filesystem-based operations.
    /// Useful for testing and local processing.
    pub fn for_local_path(base_dir: impl AsRef<std::path::Path>) -> Self {
        let path = base_dir.as_ref().to_path_buf();
        let endpoint = format!("{}{}", xet_config().data.local_cas_scheme, path.display());
        Self::new(endpoint).with_repo_paths(vec!["".into()])
    }
}

/// Main configuration for file upload/download operations.
/// Combines session-specific values with runtime-computed paths derived from the endpoint.
#[derive(Debug)]
pub(crate) struct TranslatorConfig {
    pub(crate) session: SessionConfig,

    /// Directory for chunk cache.
    pub(crate) chunk_cache_directory: PathBuf,

    /// Directory for staging xorb metadata during uploads.
    #[allow(dead_code)]
    pub(crate) staging_directory: PathBuf,

    /// Directory for caching shard files.
    pub(crate) shard_cache_directory: PathBuf,

    /// Directory for session-specific shard files.
    pub(crate) shard_session_directory: PathBuf,
}

impl TranslatorConfig {
    /// Creates a new TranslatorConfig from a SessionConfig, computing all derived paths.
    pub(crate) fn new(session: SessionConfig) -> Result<Self> {
        info!(
            endpoint = %session.endpoint,
            session_id = ?session.session_id,
            is_local = session.is_local(),
            "Initializing TranslatorConfig"
        );

        let (chunk_cache_directory, staging_directory, shard_cache_directory, shard_session_directory) =
            if let Some(local_path) = session.local_path() {
                // Local filesystem endpoint
                let base_path = local_path.join("client_cache");

                let chunk_cache_directory = base_path.join("cache");
                let staging_directory = base_path.join("staging");
                let shard_cache_directory = base_path.join("shard-cache");
                let shard_session_directory = base_path.join("shard-session");

                info!(
                    local_path = %local_path.display(),
                    chunk_cache = %chunk_cache_directory.display(),
                    staging = %staging_directory.display(),
                    shard_cache = %shard_cache_directory.display(),
                    shard_session = %shard_session_directory.display(),
                    "Using local filesystem endpoint"
                );

                (chunk_cache_directory, staging_directory, shard_cache_directory, shard_session_directory)
            } else {
                // Remote server endpoint
                let cache_path = compute_cache_path(&session.endpoint);
                std::fs::create_dir_all(&cache_path)?;

                let staging_directory = cache_path.join(&xet_config().data.staging_subdir);
                std::fs::create_dir_all(&staging_directory)?;

                let chunk_cache_directory = cache_path.join(&xet_config().chunk_cache.subdir);
                let shard_cache_directory = cache_path.join(&xet_config().mdb_shard.cache_subdir);
                let shard_session_directory = staging_directory.join(&xet_config().mdb_shard.session_subdir);

                info!(
                    cache_root = %cache_path.display(),
                    chunk_cache = %chunk_cache_directory.display(),
                    staging = %staging_directory.display(),
                    shard_cache = %shard_cache_directory.display(),
                    shard_session = %shard_session_directory.display(),
                    "Using remote server endpoint"
                );

                (chunk_cache_directory, staging_directory, shard_cache_directory, shard_session_directory)
            };

        Ok(Self {
            session,
            chunk_cache_directory,
            staging_directory,
            shard_cache_directory,
            shard_session_directory,
        })
    }
}

/// Computes a cache-safe path from an endpoint URL.
fn compute_cache_path(endpoint: &str) -> PathBuf {
    let cache_root = xet_cache_root();

    let endpoint_prefix = endpoint
        .chars()
        .take(16)
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();

    let endpoint_hash = merklehash::compute_data_hash(endpoint.as_bytes()).base64();
    let endpoint_tag = format!("{endpoint_prefix}-{}", &endpoint_hash[..16]);

    cache_root.join(endpoint_tag)
}
