use std::sync::Arc;

use anyhow::{Context, Result};
use xet::xet_session::{XetSession, XetSessionBuilder};
use xet_client::cas_client::Client;
use xet_client::cas_client::auth::AuthConfig;
use xet_client::cas_client::remote_client::RemoteClient;
use xet_client::cas_client::simulation::LocalClient;
use xet_data::processing::configurations::TranslatorConfig;
use xet_runtime::core::{XetContext, xet_cache_root};

const LOCAL_SCHEME: &str = "local://";

pub fn build_xet_session(ctx: &XetContext) -> Result<XetSession> {
    let session = XetSessionBuilder::new_with_config(ctx.config.as_ref().clone())
        .with_tokio_handle(ctx.runtime.handle().clone())
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(session)
}

pub fn build_translator_config(ctx: &XetContext, endpoint: &str) -> Result<Arc<TranslatorConfig>> {
    let config = if endpoint.starts_with(LOCAL_SCHEME) {
        let path = endpoint.strip_prefix(LOCAL_SCHEME).unwrap();
        TranslatorConfig::local_config(ctx, path)?
    } else {
        let cache_dir = xet_cache_root().join("xet-cli-stats");
        std::fs::create_dir_all(&cache_dir)?;
        TranslatorConfig::local_config(ctx, &cache_dir)?
    };
    Ok(Arc::new(config))
}

pub async fn build_cas_client(ctx: &XetContext, endpoint: &str, token: Option<String>) -> Result<Arc<dyn Client>> {
    if endpoint.starts_with(LOCAL_SCHEME) {
        let base_path = endpoint.strip_prefix(LOCAL_SCHEME).unwrap();
        let client_path = std::path::Path::new(base_path).join("xet").join("xorbs");
        let client: Arc<dyn Client> = LocalClient::new(ctx.clone(), client_path)
            .await
            .context("Failed to create LocalClient")?;
        Ok(client)
    } else {
        let auth = AuthConfig::maybe_new(token, None, None);
        let client: Arc<dyn Client> = RemoteClient::new(ctx.clone(), endpoint, &auth, "", false, None);
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_build_session_local() {
        let ctx = XetContext::default().unwrap();
        let session = build_xet_session(&ctx);
        assert!(session.is_ok(), "expected Ok, got {:?}", session.err());
    }

    #[test]
    fn test_build_translator_config_local() {
        let ctx = XetContext::default().unwrap();
        let dir = tempdir().unwrap();
        let endpoint = format!("local://{}", dir.path().display());
        assert!(build_translator_config(&ctx, &endpoint).is_ok());
    }

    #[test]
    fn test_build_translator_config_remote_fallback() {
        let ctx = XetContext::default().unwrap();
        let result = build_translator_config(&ctx, "https://example.com");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_build_cas_client_local() {
        let ctx = XetContext::default().unwrap();
        let dir = tempdir().unwrap();
        let endpoint = format!("local://{}", dir.path().display());
        let client = build_cas_client(&ctx, &endpoint, None).await;
        assert!(client.is_ok(), "expected Ok, got {:?}", client.err());
    }
}
