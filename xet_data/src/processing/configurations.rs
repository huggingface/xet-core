use std::path::{Path, PathBuf};
use std::sync::Arc;

use http::HeaderMap;
use tracing::info;
use xet_client::cas_client::auth::AuthConfig;
use xet_core_structures::xorb_object::CompressionScheme;
use xet_runtime::core::{xet_cache_root, xet_config};

use super::errors::{DataProcessingError, Result};

/// Parses the `compression_policy` config string into an `Option<CompressionScheme>`.
/// Returns `None` for auto-detect (empty or "auto"), or `Some(scheme)` for explicit values.
pub fn resolve_compression_policy(policy: &str) -> Result<Option<CompressionScheme>> {
    let normalized_policy = policy.trim().to_lowercase();
    match normalized_policy.as_str() {
        "" | "auto" => Ok(None),
        "none" => Ok(Some(CompressionScheme::None)),
        "lz4" => Ok(Some(CompressionScheme::LZ4)),
        "bg4-lz4" => Ok(Some(CompressionScheme::ByteGrouping4LZ4)),
        _ => Err(DataProcessingError::ParameterError(format!(
            "Invalid HF_XET_DATA_COMPRESSION_POLICY value '{policy}'. Valid values are: auto, none, lz4, bg4-lz4."
        ))),
    }
}

/// Session-specific configuration that varies per upload/download session.
/// These are runtime values that cannot be configured via environment variables.
#[derive(Debug)]
pub struct SessionContext {
    /// The endpoint URL. Use the `local://` prefix (configurable via `HF_XET_DATA_LOCAL_CAS_SCHEME`)
    /// to specify a local filesystem path, or `memory://` for in-memory storage.
    pub endpoint: String,
    pub auth: Option<AuthConfig>,
    pub custom_headers: Option<Arc<HeaderMap>>,
    pub repo_paths: Vec<String>,
    pub session_id: Option<String>,
}

impl SessionContext {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth: None,
            custom_headers: None,
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
        let path = self.endpoint.strip_prefix(&xet_config().data.local_cas_scheme)?;
        Some(PathBuf::from(path))
    }

    /// Returns true if this endpoint uses in-memory storage.
    pub fn is_memory(&self) -> bool {
        self.endpoint == "memory://"
    }

    pub fn with_auth(mut self, auth: Option<AuthConfig>) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_custom_headers(mut self, headers: Option<Arc<HeaderMap>>) -> Self {
        self.custom_headers = headers;
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

    /// Creates a SessionContext for local filesystem-based operations.
    pub fn for_local_path(base_dir: impl AsRef<Path>) -> Self {
        let path = base_dir.as_ref().to_path_buf();
        let endpoint = format!("{}{}", xet_config().data.local_cas_scheme, path.display());
        Self::new(endpoint).with_repo_paths(vec!["".into()])
    }

    /// Creates a SessionContext for in-memory storage.
    pub fn for_memory() -> Self {
        Self::new("memory://").with_repo_paths(vec!["".into()])
    }
}

/// Main configuration for file upload/download operations.
/// Combines session-specific values with runtime-computed paths derived from the endpoint.
#[derive(Debug)]
pub struct TranslatorConfig {
    pub session: SessionContext,

    /// Directory for caching shard files.
    pub shard_cache_directory: PathBuf,

    /// Directory for session-specific shard files.
    pub shard_session_directory: PathBuf,

    /// Per-session override: when true, progress aggregation is disabled
    /// regardless of the global `HF_XET_DATA_AGGREGATE_PROGRESS` config value.
    pub force_disable_progress_aggregation: bool,
}

impl TranslatorConfig {
    fn with_shard_directories(
        session: SessionContext,
        shard_cache_directory: PathBuf,
        shard_session_directory: PathBuf,
    ) -> Self {
        Self {
            session,
            shard_cache_directory,
            shard_session_directory,
            force_disable_progress_aggregation: false,
        }
    }

    fn create_base_xet_dir(base_dir: impl AsRef<Path>) -> Result<PathBuf> {
        let base_path = base_dir.as_ref().join("xet");
        std::fs::create_dir_all(&base_path)?;
        Ok(base_path)
    }

    /// Creates a new TranslatorConfig from a SessionContext, computing all derived paths.
    pub fn new(session: SessionContext) -> Result<Self> {
        let config = xet_config();

        let (shard_cache_directory, shard_session_directory) = if let Some(local_path) = session.local_path() {
            let base_path = local_path.join("xet");
            std::fs::create_dir_all(&base_path)?;

            (base_path.join(&config.shard.cache_subdir), base_path.join(&config.session.session_dir_name))
        } else if session.is_memory() {
            let cache_path = xet_cache_root().join("memory");
            std::fs::create_dir_all(&cache_path)?;

            (cache_path.join(&config.shard.cache_subdir), cache_path.join(&config.session.session_dir_name))
        } else {
            let cache_path = compute_cache_path(&session.endpoint);
            std::fs::create_dir_all(&cache_path)?;

            let staging_directory = cache_path.join(&config.data.staging_subdir);
            std::fs::create_dir_all(&staging_directory)?;

            (
                cache_path.join(&config.shard.cache_subdir),
                staging_directory.join(&config.session.session_dir_name),
            )
        };

        info!(
            endpoint = %session.endpoint,
            session_id = ?session.session_id,
            shard_cache = %shard_cache_directory.display(),
            shard_session = %shard_session_directory.display(),
            "TranslatorConfig initialized"
        );

        Ok(Self::with_shard_directories(session, shard_cache_directory, shard_session_directory))
    }

    /// Creates a TranslatorConfig for local filesystem-based storage.
    pub fn local_config(base_dir: impl AsRef<Path>) -> Result<Self> {
        Self::new(SessionContext::for_local_path(base_dir))
    }

    /// Creates a TranslatorConfig that uses in-memory storage for XORBs.
    /// Shard data still uses file-based storage in the provided base directory.
    pub fn memory_config(base_dir: impl AsRef<Path>) -> Result<Self> {
        let session = SessionContext::for_memory();
        let config = xet_config();
        let base_path = Self::create_base_xet_dir(base_dir)?;

        Ok(Self::with_shard_directories(
            session,
            base_path.join(&config.shard.cache_subdir),
            base_path.join(&config.session.session_dir_name),
        ))
    }

    pub fn with_session_id(mut self, session_id: &str) -> Self {
        if !session_id.is_empty() {
            self.session.session_id = Some(session_id.to_owned());
        }
        self
    }

    /// Creates a TranslatorConfig that connects to a CAS server at the given endpoint.
    /// Shard cache and session directories are created under the provided base directory.
    /// Useful for tests that use LocalTestServer.
    pub fn test_server_config(endpoint: impl AsRef<str>, base_dir: impl AsRef<Path>) -> Result<Self> {
        let session = SessionContext::new(endpoint.as_ref().to_string()).with_repo_paths(vec!["".into()]);
        let config = xet_config();
        let base_path = Self::create_base_xet_dir(base_dir)?;

        Ok(Self::with_shard_directories(
            session,
            base_path.join(&config.shard.cache_subdir),
            base_path.join(&config.session.session_dir_name),
        ))
    }

    pub fn disable_progress_aggregation(mut self) -> Self {
        self.force_disable_progress_aggregation = true;
        self
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

    let endpoint_hash = xet_core_structures::merklehash::compute_data_hash(endpoint.as_bytes()).base64();
    let endpoint_tag = format!("{endpoint_prefix}-{}", &endpoint_hash[..16]);

    cache_root.join(endpoint_tag)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use xet_core_structures::xorb_object::CompressionScheme;

    use super::{SessionContext, TranslatorConfig, resolve_compression_policy};

    #[test]
    fn test_resolve_compression_policy_accepts_supported_values() {
        assert_eq!(resolve_compression_policy("").unwrap(), None);
        assert_eq!(resolve_compression_policy("auto").unwrap(), None);
        assert_eq!(resolve_compression_policy("none").unwrap(), Some(CompressionScheme::None));
        assert_eq!(resolve_compression_policy("lz4").unwrap(), Some(CompressionScheme::LZ4));
        assert_eq!(resolve_compression_policy("bg4-lz4").unwrap(), Some(CompressionScheme::ByteGrouping4LZ4));
        assert_eq!(resolve_compression_policy("  LZ4 ").unwrap(), Some(CompressionScheme::LZ4));
    }

    #[test]
    fn test_resolve_compression_policy_rejects_invalid_value() {
        let err = resolve_compression_policy("zstd").unwrap_err();
        assert!(err.to_string().contains("HF_XET_DATA_COMPRESSION_POLICY"));
    }

    #[test]
    fn test_session_context_mode_detection() {
        let temp_dir = tempdir().unwrap();
        let local_session = SessionContext::for_local_path(temp_dir.path());
        assert!(local_session.is_local());
        assert!(!local_session.is_memory());
        assert_eq!(local_session.local_path().unwrap(), temp_dir.path().to_path_buf());

        let memory_session = SessionContext::for_memory();
        assert!(memory_session.is_memory());
        assert!(!memory_session.is_local());
        assert!(memory_session.local_path().is_none());

        let remote_session = SessionContext::new("http://localhost:8080");
        assert!(!remote_session.is_local());
        assert!(!remote_session.is_memory());
        assert!(remote_session.local_path().is_none());
    }

    #[test]
    fn test_memory_and_server_configs_use_base_xet_layout() {
        let temp_dir = tempdir().unwrap();

        let memory_config = TranslatorConfig::memory_config(temp_dir.path()).unwrap();
        assert!(memory_config.shard_cache_directory.starts_with(temp_dir.path().join("xet")));
        assert!(memory_config.shard_session_directory.starts_with(temp_dir.path().join("xet")));

        let server_config = TranslatorConfig::test_server_config("http://localhost:8080", temp_dir.path()).unwrap();
        assert!(server_config.shard_cache_directory.starts_with(temp_dir.path().join("xet")));
        assert!(server_config.shard_session_directory.starts_with(temp_dir.path().join("xet")));
    }
}
