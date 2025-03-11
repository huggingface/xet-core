use std::fmt::Debug;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::errors::AuthError;

/// Helper type for information about an auth token.
/// Namely, the token itself and expiration time
pub type TokenInfo = (String, u64);

/// Helper to provide auth tokens to CAS.
#[async_trait]
pub trait TokenRefresher: Debug + Send + Sync {
    /// Get a new auth token for CAS and the unixtime (in seconds) for expiration
    async fn refresh(&self) -> Result<TokenInfo, AuthError>;
}

#[derive(Debug)]
pub struct NoOpTokenRefresher;

#[async_trait]
impl TokenRefresher for NoOpTokenRefresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        Ok(("token".to_string(), 0))
    }
}

#[derive(Debug)]
pub struct ErrTokenRefresher;

#[async_trait]
impl TokenRefresher for ErrTokenRefresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        Err(AuthError::RefreshFunctionNotCallable("Token refresh not expected".to_string()))
    }
}

/// Shared configuration for token-based auth
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Initial token to use
    pub token: String,
    /// Initial token expiration time in epoch seconds
    pub token_expiration: u64,
    /// A function to refresh tokens.
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
        }
        Ok(self.token.clone())
    }

    fn is_expired(&self) -> bool {
        let cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(u64::MAX);
        self.expiration <= cur_time
    }
}
