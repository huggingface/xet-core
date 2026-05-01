use std::path::{Path, PathBuf};
use std::sync::Arc;

use http::HeaderMap;
use tracing::info;
use xet_client::cas_client::auth::AuthConfig;
use xet_runtime::core::{XetContext, xet_cache_root};

use crate::error::Result;

/// Session-specific configuration that varies per upload/download session.
/// These are runtime values that cannot be configured via environment variables.
#[derive(Debug, Clone)]
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
    /// Returns true if this endpoint points to a local filesystem path.
    pub fn is_local(&self, ctx: &XetContext) -> bool {
        self.endpoint.starts_with(ctx.config.data.local_cas_scheme.as_str())
    }

    /// Returns the local filesystem path if this is a local endpoint.
    pub fn local_path(&self, ctx: &XetContext) -> Option<PathBuf> {
        let path = self.endpoint.strip_prefix(ctx.config.data.local_cas_scheme.as_str())?;
        Some(PathBuf::from(path))
    }

    /// Returns true if this endpoint uses in-memory storage.
    pub fn is_memory(&self) -> bool {
        self.endpoint == "memory://"
    }

    /// Creates a SessionContext for local filesystem-based operations.
    pub fn for_local_path(ctx: &XetContext, base_dir: impl AsRef<Path>) -> Self {
        let path = base_dir.as_ref().to_path_buf();
        let endpoint = format!("{}{}", ctx.config.data.local_cas_scheme, path.display());
        Self {
            endpoint,
            auth: None,
            custom_headers: None,
            repo_paths: vec!["".into()],
            session_id: None,
        }
    }

    /// Creates a SessionContext for in-memory storage.
    pub fn for_memory() -> Self {
        Self {
            endpoint: "memory://".into(),
            auth: None,
            custom_headers: None,
            repo_paths: vec!["".into()],
            session_id: None,
        }
    }
}

/// Main configuration for file upload/download operations.
/// Combines session-specific values with runtime-computed paths derived from the endpoint.
#[derive(Debug, Clone)]
pub struct TranslatorConfig {
    pub ctx: XetContext,
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
    fn create_base_xet_dir(base_dir: impl AsRef<Path>) -> Result<PathBuf> {
        let base_path = base_dir.as_ref().join("xet");
        std::fs::create_dir_all(&base_path)?;
        Ok(base_path)
    }

    /// Creates a new TranslatorConfig from a SessionContext, computing all derived paths.
    pub fn new(ctx: &XetContext, session: SessionContext) -> Result<Self> {
        let config = ctx.config.as_ref();

        let (shard_cache_directory, shard_session_directory) = if let Some(local_path) = session.local_path(ctx) {
            let base_path = local_path.join("xet");
            std::fs::create_dir_all(&base_path)?;

            (base_path.join(&config.shard.cache_subdir), base_path.join(&config.session.dir_name))
        } else if session.is_memory() {
            let cache_path = xet_cache_root().join("memory");
            std::fs::create_dir_all(&cache_path)?;

            (cache_path.join(&config.shard.cache_subdir), cache_path.join(&config.session.dir_name))
        } else {
            let cache_path = compute_cache_path(&session.endpoint);
            std::fs::create_dir_all(&cache_path)?;

            let staging_directory = cache_path.join(&config.data.staging_subdir);
            std::fs::create_dir_all(&staging_directory)?;

            (cache_path.join(&config.shard.cache_subdir), staging_directory.join(&config.session.dir_name))
        };

        info!(
            endpoint = %session.endpoint,
            session_id = ?session.session_id,
            shard_cache = %shard_cache_directory.display(),
            shard_session = %shard_session_directory.display(),
            "TranslatorConfig initialized"
        );

        Ok(Self {
            ctx: ctx.clone(),
            session,
            shard_cache_directory,
            shard_session_directory,
            force_disable_progress_aggregation: false,
        })
    }

    /// Creates a TranslatorConfig for local filesystem-based storage.
    pub fn local_config(ctx: &XetContext, base_dir: impl AsRef<Path>) -> Result<Self> {
        Self::new(ctx, SessionContext::for_local_path(ctx, base_dir))
    }

    /// Creates a TranslatorConfig that uses in-memory storage for XORBs.
    /// Shard data still uses file-based storage in the provided base directory.
    pub fn memory_config(ctx: &XetContext, base_dir: impl AsRef<Path>) -> Result<Self> {
        let session = SessionContext::for_memory();
        let config = ctx.config.as_ref();
        let base_path = Self::create_base_xet_dir(base_dir)?;

        Ok(Self {
            ctx: ctx.clone(),
            session,
            shard_cache_directory: base_path.join(&config.shard.cache_subdir),
            shard_session_directory: base_path.join(&config.session.dir_name),
            force_disable_progress_aggregation: false,
        })
    }

    /// Creates a TranslatorConfig that connects to a CAS server at the given endpoint.
    /// Shard cache and session directories are created under the provided base directory.
    /// Useful for tests that use LocalTestServer.
    pub fn test_server_config(ctx: &XetContext, endpoint: impl AsRef<str>, base_dir: impl AsRef<Path>) -> Result<Self> {
        let session = SessionContext {
            endpoint: endpoint.as_ref().to_string(),
            auth: None,
            custom_headers: None,
            repo_paths: vec!["".into()],
            session_id: None,
        };
        let config = ctx.config.as_ref();
        let base_path = Self::create_base_xet_dir(base_dir)?;

        Ok(Self {
            ctx: ctx.clone(),
            session,
            shard_cache_directory: base_path.join(&config.shard.cache_subdir),
            shard_session_directory: base_path.join(&config.session.dir_name),
            force_disable_progress_aggregation: false,
        })
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
    use xet_runtime::core::XetContext;

    use super::{SessionContext, TranslatorConfig};

    #[test]
    fn test_session_context_mode_detection() {
        let ctx = XetContext::default().unwrap();
        let temp_dir = tempdir().unwrap();
        let local_session = SessionContext::for_local_path(&ctx, temp_dir.path());
        assert!(local_session.is_local(&ctx));
        assert!(!local_session.is_memory());
        assert_eq!(local_session.local_path(&ctx).unwrap(), temp_dir.path().to_path_buf());

        let memory_session = SessionContext::for_memory();
        assert!(memory_session.is_memory());
        assert!(!memory_session.is_local(&ctx));
        assert!(memory_session.local_path(&ctx).is_none());

        let remote_session = SessionContext {
            endpoint: "http://localhost:8080".into(),
            auth: None,
            custom_headers: None,
            repo_paths: Vec::new(),
            session_id: None,
        };
        assert!(!remote_session.is_local(&ctx));
        assert!(!remote_session.is_memory());
        assert!(remote_session.local_path(&ctx).is_none());
    }

    #[test]
    fn test_memory_and_server_configs_use_base_xet_layout() {
        let ctx = XetContext::default().unwrap();
        let temp_dir = tempdir().unwrap();

        let memory_config = TranslatorConfig::memory_config(&ctx, temp_dir.path()).unwrap();
        assert!(memory_config.shard_cache_directory.starts_with(temp_dir.path().join("xet")));
        assert!(memory_config.shard_session_directory.starts_with(temp_dir.path().join("xet")));

        let server_config =
            TranslatorConfig::test_server_config(&ctx, "http://localhost:8080", temp_dir.path()).unwrap();
        assert!(server_config.shard_cache_directory.starts_with(temp_dir.path().join("xet")));
        assert!(server_config.shard_session_directory.starts_with(temp_dir.path().join("xet")));
    }
}
