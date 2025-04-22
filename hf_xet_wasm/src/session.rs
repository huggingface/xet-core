use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct XetSession {
    token_info: utils::auth::TokenInfo,
    token_refresher: Arc<dyn utils::auth::TokenRefresher>,
}

#[wasm_bindgen]
impl XetSession {
    #[wasm_bindgen(constructor)]
    pub fn new(
        token_info: crate::auth::TokenInfo,
        token_refresher: crate::auth::TokenRefresher,
    ) -> Self {
        Self {
            token_info: token_info.into(),
            token_refresher: Arc::new(token_refresher),
        }
    }
}