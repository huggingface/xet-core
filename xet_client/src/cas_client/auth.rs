use std::fmt::Debug;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};

use derivative::Derivative;
use reqwest_middleware::ClientWithMiddleware;
use thiserror::Error;
use tracing::info;
use xet_runtime::core::XetContext;

use crate::common::auth::CredentialHelper;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AuthError {
    #[error("Refresh function: {0} is not callable")]
    RefreshFunctionNotCallable(String),

    #[error("Token refresh failed: {0}")]
    TokenRefreshFailure(String),
}

impl AuthError {
    pub fn token_refresh_failure(err: impl ToString) -> Self {
        Self::TokenRefreshFailure(err.to_string())
    }
}

/// Seconds before the token expires to refresh
const REFRESH_BUFFER_SEC: u64 = 30;

/// Helper type for information about an auth token.
/// Namely, the token itself and expiration time
pub type TokenInfo = (String, u64);

/// Helper to provide auth tokens to CAS.
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait TokenRefresher: Send + Sync {
    /// Get a new auth token for CAS and the unixtime (in seconds) for expiration
    async fn refresh(&self) -> Result<TokenInfo, AuthError>;
}

#[derive(Debug)]
pub struct NoOpTokenRefresher;

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl TokenRefresher for NoOpTokenRefresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        Ok(("token".to_string(), 0))
    }
}

#[derive(Debug)]
pub struct ErrTokenRefresher;

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl TokenRefresher for ErrTokenRefresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        Err(AuthError::RefreshFunctionNotCallable("Token refresh not expected".to_string()))
    }
}

/// Token refresher that fetches a new token by making an authenticated GET request to a URL.
///
/// An optional [`CredentialHelper`](crate::common::auth::CredentialHelper) is applied to the
/// request before it is sent; pass `None` when no additional credentials are needed.
pub struct DirectRefreshRouteTokenRefresher {
    ctx: XetContext,
    refresh_route: String,
    client: ClientWithMiddleware,
    cred_helper: Option<Arc<dyn CredentialHelper>>,
}

impl std::fmt::Debug for DirectRefreshRouteTokenRefresher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectRefreshRouteTokenRefresher")
            .field("refresh_route", &self.refresh_route)
            .finish_non_exhaustive()
    }
}

impl DirectRefreshRouteTokenRefresher {
    pub fn new(
        ctx: XetContext,
        refresh_route: impl Into<String>,
        client: ClientWithMiddleware,
        cred_helper: Option<Arc<dyn CredentialHelper>>,
    ) -> Self {
        Self {
            ctx,
            refresh_route: refresh_route.into(),
            client,
            cred_helper,
        }
    }

    pub async fn get_cas_jwt(&self) -> Result<crate::hub_client::CasJWTInfo, crate::ClientError> {
        let client = self.client.clone();
        let refresh_route = self.refresh_route.clone();
        let cred_helper = self.cred_helper.clone();

        let jwt_info: crate::hub_client::CasJWTInfo =
            super::retry_wrapper::RetryWrapper::new(self.ctx.clone(), "xet-token")
                .run_and_extract_json(move || {
                    let refresh_route = refresh_route.clone();
                    let client = client.clone();
                    let cred_helper = cred_helper.clone();
                    async move {
                        let req = client
                            .get(&refresh_route)
                            .with_extension(crate::common::http_client::Api("xet-token"));
                        let req = if let Some(helper) = cred_helper {
                            helper
                                .fill_credential(req)
                                .await
                                .map_err(reqwest_middleware::Error::middleware)?
                        } else {
                            req
                        };
                        req.send().await
                    }
                })
                .await?;

        Ok(jwt_info)
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl TokenRefresher for DirectRefreshRouteTokenRefresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        let jwt_info = self.get_cas_jwt().await.map_err(AuthError::token_refresh_failure)?;

        Ok((jwt_info.access_token, jwt_info.exp))
    }
}

/// Shared configuration for token-based auth
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct AuthConfig {
    /// Initial token to use
    pub token: String,
    /// Initial token expiration time in epoch seconds
    pub token_expiration: u64,
    /// A function to refresh tokens.
    #[derivative(Debug = "ignore")]
    pub token_refresher: Arc<dyn TokenRefresher>,
}

impl AuthConfig {
    /// Builds a new AuthConfig from the indicated optional parameters.
    pub fn maybe_new(
        token: Option<String>,
        token_expiry: Option<u64>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
    ) -> Option<Self> {
        match (token, token_expiry, token_refresher) {
            // we have a refresher, so use that. Doesn't matter if the token/expiry are set since we can refresh them.
            (token, expiry, Some(refresher)) => Some(Self {
                token: token.unwrap_or_default(),
                token_expiration: expiry.unwrap_or_default(),
                token_refresher: refresher,
            }),
            // Since no refreshing, we instead use the token with some expiration (no expiration means we expect this
            // token to live forever.
            (Some(token), expiry, None) => Some(Self {
                token,
                token_expiration: expiry.unwrap_or(u64::MAX),
                token_refresher: Arc::new(ErrTokenRefresher),
            }),
            (_, _, _) => None,
        }
    }
}

pub struct TokenProvider {
    token: String,
    expiration: u64,
    refresher: Arc<dyn TokenRefresher>,
}

impl TokenProvider {
    pub fn new(cfg: &AuthConfig) -> Self {
        Self {
            token: cfg.token.clone(),
            expiration: cfg.token_expiration,
            refresher: cfg.token_refresher.clone(),
        }
    }

    pub async fn get_valid_token(&mut self) -> Result<String, AuthError> {
        if self.is_expired() {
            let (new_token, new_expiry) = self.refresher.refresh().await?;
            self.token = new_token;
            self.expiration = new_expiry;
            info!(new_expiry = new_expiry, "Token refreshed");
        }
        Ok(self.token.clone())
    }

    fn is_expired(&self) -> bool {
        #[cfg(not(target_family = "wasm"))]
        let cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(u64::MAX);
        #[cfg(target_family = "wasm")]
        let cur_time = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(u64::MAX);
        self.expiration <= cur_time + REFRESH_BUFFER_SEC
    }
}
