use crate::auth::WrappedTokenRefresher;
use std::sync::Arc;
use utils::auth::AuthConfig;
use wasm_bindgen::prelude::*;

struct Clients {
    authenticated_http_client: Arc<http_client::ClientWithMiddleware>,
    conservative_authenticated_http_client: Arc<http_client::ClientWithMiddleware>,
    http_client: Arc<http_client::ClientWithMiddleware>,
}

impl Clients {
    fn new(auth: Option<AuthConfig>) -> Self {
        Self {
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(&auth, http_client::RetryConfig::default()).unwrap(),
            ),
            conservative_authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(&auth, http_client::RetryConfig::no429retry()).unwrap(),
            ),
            http_client: Arc::new(http_client::build_http_client(http_client::RetryConfig::default()).unwrap()),
        }
    }
}

#[wasm_bindgen]
pub struct XetSession {
    cas_endpoint: String,
    clients: Clients,
}

#[wasm_bindgen]
impl XetSession {
    #[wasm_bindgen(constructor)]
    pub fn new(
        cas_endpoint: String,
        token_info: crate::auth::TokenInfo,
        token_refresher: crate::auth::TokenRefresher,
    ) -> Self {
        let (token, token_expiration): utils::auth::TokenInfo = token_info.into();
        let auth = AuthConfig {
            token,
            token_expiration,
            token_refresher: Arc::new(WrappedTokenRefresher::from(token_refresher)),
        };
        Self {
            cas_endpoint,
            clients: Clients::new(Some(auth)),
        }
    }

    #[wasm_bindgen(js_name = "chunk")]
    pub fn chunk(data: Vec<u8>) -> JsValue {
        crate::chunk(data)
    }
}
