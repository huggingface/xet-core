#[cfg(not(target_family = "wasm"))]
pub mod legacy;

use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::{fs::File, io::Read, path::Path};

use http::header::HeaderMap;
#[cfg(not(target_family = "wasm"))]
use tracing::{Span, instrument};
use uuid::Uuid;
use xet_client::cas_client::auth::{AuthConfig, TokenRefresher};
use xet_runtime::core::XetContext;

use super::configurations::{SessionContext, TranslatorConfig};
#[cfg(not(target_family = "wasm"))]
use super::{FileUploadSession, XetFileInfo, file_cleaner::Sha256Policy};
#[cfg(not(target_family = "wasm"))]
use crate::deduplication::DeduplicationMetrics;
use crate::error::Result;

pub fn default_config(
    ctx: &XetContext,
    endpoint: String,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<TranslatorConfig> {
    let (token, token_expiration) = token_info.unzip();
    let auth_cfg = AuthConfig::maybe_new(token, token_expiration, token_refresher);

    let session = SessionContext {
        endpoint,
        auth: auth_cfg,
        custom_headers,
        repo_paths: vec!["".into()],
        session_id: Some(Uuid::now_v7().to_string()),
    };

    TranslatorConfig::new(ctx, session)
}

#[cfg(not(target_family = "wasm"))]
#[instrument(skip_all, name = "clean_file", fields(file.name = tracing::field::Empty, file.len = tracing::field::Empty))]
pub async fn clean_file(
    processor: Arc<FileUploadSession>,
    filename: impl AsRef<Path>,
    sha256_policy: Sha256Policy,
) -> Result<(XetFileInfo, DeduplicationMetrics)> {
    let mut reader = File::open(&filename)?;

    let filesize = reader.metadata()?.len();
    let span = Span::current();
    span.record("file.name", filename.as_ref().to_str());
    span.record("file.len", filesize);
    let mut buffer = vec![0u8; u64::min(filesize, *processor.ctx.config.data.ingestion_block_size) as usize];

    let (_id, mut handle) =
        processor.start_clean(Some(filename.as_ref().to_string_lossy().into()), Some(filesize), sha256_policy)?;

    loop {
        let bytes = reader.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }

        handle.add_data(&buffer[0..bytes]).await?;
    }

    handle.finish().await
}

#[cfg(test)]
mod tests {
    use dirs::home_dir;
    use serial_test::serial;
    use tempfile::tempdir;
    use xet_runtime::core::XetContext;
    use xet_runtime::utils::EnvVarGuard;

    use super::*;

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_home() {
        let temp_dir = tempdir().unwrap();
        let _hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let ctx = XetContext::default().unwrap();
        let result = default_config(&ctx, endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache_and_hf_home() {
        let temp_dir_xet_cache = tempdir().unwrap();
        let temp_dir_hf_home = tempdir().unwrap();

        let hf_xet_cache_guard = EnvVarGuard::set("HF_XET_CACHE", temp_dir_xet_cache.path().to_str().unwrap());
        let hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir_hf_home.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let ctx = XetContext::default().unwrap();
        let result = default_config(&ctx, endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir_xet_cache.path()));

        drop(hf_xet_cache_guard);
        drop(hf_home_guard);

        let temp_dir = tempdir().unwrap();
        let _hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let ctx = XetContext::default().unwrap();
        let result = default_config(&ctx, endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache() {
        let temp_dir = tempdir().unwrap();
        let _hf_xet_cache_guard = EnvVarGuard::set("HF_XET_CACHE", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let ctx = XetContext::default().unwrap();
        let result = default_config(&ctx, endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_without_env_vars() {
        let endpoint = "http://localhost:8080".to_string();
        let ctx = XetContext::default().unwrap();
        let result = default_config(&ctx, endpoint, None, None, None);

        let expected = home_dir().unwrap().join(".cache").join("huggingface").join("xet");

        assert!(result.is_ok());
        let config = result.unwrap();
        let test_cache_dir = &config.shard_cache_directory;
        assert!(
            test_cache_dir.starts_with(&expected),
            "cache dir = {test_cache_dir:?}; does not start with {expected:?}",
        );
    }
}
