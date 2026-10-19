//! Connects to a repository's CAS endpoint through the Hub's Xet token API.
use std::sync::Arc;

use async_trait::async_trait;
use git_extension_common::GitUrl;
use xet_client::ClientError;
use xet_client::cas_client::auth::{AuthConfig, AuthError, TokenInfo, TokenRefresher};
use xet_client::cas_client::{Client, RemoteClient};
use xet_client::hub_client::{CredentialHelper, HubClient, Operation};
use xet_runtime::core::XetContext;

pub(crate) struct CasConnection {
    pub(crate) client: Arc<dyn Client>,
    pub(crate) endpoint: String,
    pub(crate) auth: Option<AuthConfig>,
}

struct HubTokens {
    hub: Arc<HubClient>,
    operation: Operation,
}

#[async_trait]
impl TokenRefresher for HubTokens {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let info = self
            .hub
            .get_cas_jwt(self.operation)
            .await
            .map_err(AuthError::token_refresh_failure)?;
        Ok((info.access_token, info.exp))
    }
}

/// Requests a CAS token for `operation` on `reference` (a branch name or full ref; the Hub defaults
/// to `main`) and returns a client for the CAS endpoint the Hub names, refreshing the token as needed.
/// The caller's credential decides whether this may prompt.
pub(crate) async fn connect(
    ctx: &XetContext,
    url: &GitUrl,
    reference: Option<String>,
    operation: Operation,
    credential: Arc<dyn CredentialHelper>,
    session_id: &str,
) -> Result<CasConnection, ClientError> {
    let hub = Arc::new(HubClient::new(
        ctx.clone(),
        &url.to_derived_http_host_url().map_err(|e| ClientError::Other(e.to_string()))?,
        url.repo_info().map_err(|e| ClientError::Other(e.to_string()))?,
        reference,
        session_id,
        Some(credential),
        None,
    )?);
    let info = hub.get_cas_jwt(operation).await?;
    let auth = Some(AuthConfig {
        token: info.access_token,
        token_expiration: info.exp,
        token_refresher: Arc::new(HubTokens { hub, operation }),
    });
    let client = RemoteClient::new(ctx.clone(), &info.cas_url, &auth, session_id, false, None);
    Ok(CasConnection {
        client,
        endpoint: info.cas_url,
        auth,
    })
}
