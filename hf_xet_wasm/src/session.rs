mod auth;

use std::sync::Arc;

use utils::auth::AuthConfig;
use wasm_bindgen::prelude::*;
use web_sys::Blob;

use crate::session::auth::{TokenInfo, TokenRefresher, WrappedTokenRefresher};

#[wasm_bindgen]
pub struct XetSession {
    auth_config: AuthConfig,
}

#[wasm_bindgen]
impl XetSession {
    #[wasm_bindgen(constructor)]
    pub fn new(token_info: TokenInfo, token_refresher: TokenRefresher) -> Self {
        let auth_config = AuthConfig::maybe_new(
            Some(token_info.token()),
            Some(token_info.expiration()),
            Some(Arc::new(WrappedTokenRefresher::from(token_refresher))),
        )
        .unwrap();
        XetSession { auth_config }
    }

    #[wasm_bindgen]
    pub async fn start_clean_blob(repo_id: String, blob: Blob) -> Result<(), JsValue> {
        Ok(())
    }
}
