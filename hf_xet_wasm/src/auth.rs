use std::fmt::{Debug, Formatter};
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

impl utils::auth::TokenRefresher for TokenRefresher {
    async fn refresh(&self) -> Result<utils::auth::TokenInfo, utils::auth::AuthError> {
        Ok(utils::auth::TokenInfo::from(self.refresh_token().await))
    }
}