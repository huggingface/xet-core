use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tokio::sync::Mutex;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;

/// The internal token refreshing mechanism expects to be passed in a TokenInfo and TokenRefresher
/// javascript interface objects that implement the following interface.
///
/// To pass these constructs into the wasm program, use the XetSession export.
/// 
///```typescript
/// interface TokenInfo {
///   token(): string {}
///   exp(): number {}
/// }
///
/// interface TokenRefresher {
///   async refreshToken(): TokenInfo {}
/// }
///```

#[wasm_bindgen]
extern "C" {
    pub type TokenInfo;
    #[wasm_bindgen(method, getter)]
    pub fn token(this: &TokenInfo) -> String;
    #[wasm_bindgen(method, getter)]
    pub fn exp(this: &TokenInfo) -> f64;

    pub type TokenRefresher;
    #[wasm_bindgen(method, catch, js_name = "refreshToken")]
    pub async fn refresh_token(this: &TokenRefresher) -> Result<TokenInfo, JsValue>;
}

impl From<TokenInfo> for utils::auth::TokenInfo {
    fn from(value: TokenInfo) -> Self {
        (value.token(), value.exp() as u64)
    }
}

impl Debug for TokenRefresher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TokenRefresher")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WrappedTokenRefresher(Arc<Mutex<TokenRefresher>>);

// TODO: revise the safety of this!
unsafe impl Send for WrappedTokenRefresher {}
unsafe impl Sync for WrappedTokenRefresher {}

impl From<TokenRefresher> for WrappedTokenRefresher {
    fn from(value: TokenRefresher) -> Self {
        #[allow(clippy::arc_with_non_send_sync)]
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
