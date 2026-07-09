use std::sync::Arc;

use anyhow::{Context, Result};
use http::HeaderMap;
use xet::xet_session::{XetSession, XetSessionBuilder};
use xet_client::cas_client::Client;
use xet_client::cas_client::auth::{AuthConfig, DirectRefreshRouteTokenRefresher, TokenRefresher};
use xet_client::cas_client::remote_client::RemoteClient;
use xet_client::cas_client::simulation::LocalClient;
use xet_client::common::http_client::build_http_client;
use xet_runtime::core::XetContext;

const LOCAL_SCHEME: &str = "local://";

pub fn build_xet_session(ctx: &XetContext) -> Result<XetSession> {
    let session = XetSessionBuilder::new_with_config(ctx.config.as_ref().clone())
        .with_tokio_handle(ctx.runtime.handle().clone())
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(session)
}

pub async fn build_cas_client(
    ctx: &XetContext,
    endpoint: &str,
    token_info: Option<(String, u64)>,
    token_refresh: Option<(String, HeaderMap)>,
) -> Result<Arc<dyn Client>> {
    if endpoint.starts_with(LOCAL_SCHEME) {
        let base_path = endpoint.strip_prefix(LOCAL_SCHEME).unwrap();
        let client_path = std::path::Path::new(base_path).join("xet").join("xorbs");
        let client: Arc<dyn Client> = LocalClient::new(ctx.clone(), client_path)
            .await
            .context("Failed to create LocalClient")?;
        Ok(client)
    } else {
        let token_refresher = if let Some((refresh_url, refresh_headers)) = token_refresh {
            let refresh_client = build_http_client(ctx, "", None, Some(Arc::new(refresh_headers)))?;
            Some(Arc::new(DirectRefreshRouteTokenRefresher::new(ctx.clone(), refresh_url, refresh_client, None))
                as Arc<dyn TokenRefresher>)
        } else {
            None
        };

        let (token, expiry) = match token_info {
            Some((token, expiry)) => (Some(token), Some(expiry)),
            None => (None, None),
        };
        let auth = AuthConfig::maybe_new(token, expiry, token_refresher);
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

    #[tokio::test]
    async fn test_build_cas_client_local() {
        let ctx = XetContext::default().unwrap();
        let dir = tempdir().unwrap();
        let endpoint = format!("local://{}", dir.path().display());
        let client = build_cas_client(&ctx, &endpoint, None, None).await;
        assert!(client.is_ok(), "expected Ok, got {:?}", client.err());
    }
}
