use std::sync::Arc;

use anyhow::{Context, Result};
use xet::xet_session::{XetSession, XetSessionBuilder};
use xet_client::cas_client::Client;
use xet_client::cas_client::auth::AuthConfig;
use xet_client::cas_client::remote_client::RemoteClient;
use xet_client::cas_client::simulation::LocalClient;
use xet_data::processing::configurations::TranslatorConfig;
use xet_runtime::config::XetConfig;
use xet_runtime::core::xet_cache_root;

const LOCAL_SCHEME: &str = "local://";

/// Build a XetSession for upload/download commands.
///
/// Auth tokens are supplied per upload commit or download stream group; see
/// [`UploadCommitBuilder::with_token_info`](xet::xet_session::UploadCommitBuilder::with_token_info)
/// and [`DownloadStreamGroupBuilder::with_token_info`](xet::xet_session::DownloadStreamGroupBuilder::with_token_info).
pub fn build_xet_session(endpoint: &str, config: XetConfig) -> Result<XetSession> {
    let builder = XetSessionBuilder::new_with_config(config).with_endpoint(endpoint.to_owned());
    let session: XetSession = builder.build().map_err(|e| anyhow::anyhow!(e))?;
    Ok(session)
}

/// Build a TranslatorConfig for the stats (dry-run) command.
pub fn build_translator_config(endpoint: &str) -> Result<Arc<TranslatorConfig>> {
    let config = if endpoint.starts_with(LOCAL_SCHEME) {
        let path = endpoint.strip_prefix(LOCAL_SCHEME).unwrap();
        TranslatorConfig::local_config(path)?
    } else {
        // For stats (dry-run), no data is sent to the remote endpoint.
        // We use a local shard-cache directory so the translator config
        // has a place to stage data. No network calls are made.
        let cache_dir = xet_cache_root().join("xet-cli-stats");
        std::fs::create_dir_all(&cache_dir)?;
        TranslatorConfig::local_config(&cache_dir)?
    };
    Ok(Arc::new(config))
}

/// Build a raw CAS client for the query command.
/// Upload/download use `build_xet_session` instead (which constructs its own
/// client internally via `XetSessionBuilder`). This is only for commands that
/// need direct `Client` trait access without a full `XetSession`.
pub async fn build_cas_client(endpoint: &str, token: Option<String>) -> Result<Arc<dyn Client>> {
    if endpoint.starts_with(LOCAL_SCHEME) {
        let base_path = endpoint.strip_prefix(LOCAL_SCHEME).unwrap();
        // The FileUploadSession stores data under <base_path>/xet/xorbs (see
        // xet_data::processing::remote_client_interface::create_remote_client).
        // Point LocalClient at the same sub-directory so it can find the shards and xorbs.
        let client_path = std::path::Path::new(base_path).join("xet").join("xorbs");
        let client: Arc<dyn Client> = LocalClient::new(client_path).await.context("Failed to create LocalClient")?;
        Ok(client)
    } else {
        let auth = AuthConfig::maybe_new(token, None, None);
        let client: Arc<dyn Client> = RemoteClient::new(endpoint, &auth, "", false, None);
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_build_session_local() {
        let dir = tempdir().unwrap();
        let endpoint = format!("local://{}", dir.path().display());
        let config = XetConfig::new();
        let session = build_xet_session(&endpoint, config);
        assert!(session.is_ok(), "expected Ok, got {:?}", session.err());
    }

    #[test]
    fn test_build_translator_config_local() {
        let dir = tempdir().unwrap();
        let endpoint = format!("local://{}", dir.path().display());
        assert!(build_translator_config(&endpoint).is_ok());
    }

    #[test]
    fn test_build_translator_config_remote_fallback() {
        let result = build_translator_config("https://example.com");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_build_cas_client_local() {
        let dir = tempdir().unwrap();
        let endpoint = format!("local://{}", dir.path().display());
        let client = build_cas_client(&endpoint, None).await;
        assert!(client.is_ok(), "expected Ok, got {:?}", client.err());
    }
}
