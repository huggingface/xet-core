use wasm_bindgen::prelude::*;

pub(crate) fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

pub(crate) fn validate_session_inputs(endpoint: &str, token: &str, token_expiry: f64) -> Result<u64, JsValue> {
    if !token_expiry.is_finite() || token_expiry <= 0.0 {
        return Err(JsValue::from_str("tokenExpiry must be a finite, positive Unix timestamp (seconds)"));
    }
    if token.trim().is_empty() {
        return Err(JsValue::from_str("token must be non-empty"));
    }
    let trimmed = endpoint.trim();
    if trimmed.is_empty() || !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(JsValue::from_str("endpoint must be a valid URL"));
    }
    Ok(token_expiry as u64)
}
