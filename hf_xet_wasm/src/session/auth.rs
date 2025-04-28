use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::Mutex;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    pub type TokenInfo;
    #[wasm_bindgen(method, getter)]
    pub fn token(this: &TokenInfo) -> String;
    #[wasm_bindgen(method, getter)]
    pub fn expiration(this: &TokenInfo) -> u64;

    pub type TokenRefresher;
    #[wasm_bindgen(method, catch)]
    pub async fn refresh_token(this: &TokenRefresher) -> Result<TokenInfo, JsValue>;
}

/// interface TokenRefresher {
///    refresh_token(): Promise<TokenInfo>;
/// }

impl From<TokenInfo> for utils::auth::TokenInfo {
    fn from(value: TokenInfo) -> Self {
        (value.token(), value.expiration())
    }
}

impl Debug for TokenRefresher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TokenRefresher")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WrappedTokenRefresher(Arc<Mutex<TokenRefresher>>);

unsafe impl Send for WrappedTokenRefresher {}
unsafe impl Sync for WrappedTokenRefresher {}

impl From<TokenRefresher> for WrappedTokenRefresher {
    fn from(value: TokenRefresher) -> Self {
        WrappedTokenRefresher(Arc::new(Mutex::new(value)))
    }
}

#[async_trait::async_trait(?Send)]
impl utils::auth::TokenRefresher for WrappedTokenRefresher {
    async fn refresh(&self) -> Result<utils::auth::TokenInfo, utils::errors::AuthError> {
        self.0
            .lock()
            .await
            .refresh_token()
            .await
            .map(utils::auth::TokenInfo::from)
            .map_err(|e| utils::errors::AuthError::token_refresh_failure(format!("{e:?}")))
    }
}