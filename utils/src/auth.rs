use crate::errors::AuthError;
use std::fmt::Debug;
use std::sync::Arc;

/// Helper to provide auth tokens to CAS.
pub trait TokenRefresher: Debug + Send + Sync {
    /// Get a new auth token for CAS and the unixtime (in seconds) for expiration
    fn refresh(&self) -> Result<(String, u64), AuthError>;
}

#[derive(Debug)]
pub struct NoOpTokenRefresher;

impl TokenRefresher for NoOpTokenRefresher {
    fn refresh(&self) -> Result<(String, u64), AuthError> {
        Ok(("token".to_string(), 0))
    }
}

/// Shared configuration for token-based auth
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Initial token to use
    pub token: Option<String>,
    /// Initial token expiration time epoch in seconds
    pub token_expiration: Option<u64>,
    /// A function to refresh tokens.
    pub token_refresher: Option<Arc<dyn TokenRefresher>>,
}
