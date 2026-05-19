use wasm_bindgen::prelude::*;

pub(crate) fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

pub(crate) fn validate_session_inputs(endpoint: &str, token: &str, token_expiry: f64) -> Result<u64, JsValue> {
    if !token_expiry.is_finite() || token_expiry < 0.0 {
        return Err(JsValue::from_str("tokenExpiry must be a finite, non-negative number"));
    }
    if token.trim().is_empty() {
        return Err(JsValue::from_str("token must be non-empty"));
    }
    let trimmed = endpoint.trim();
    if trimmed.is_empty() || !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(JsValue::from_str("endpoint must be a valid URL"));
    }
    // Map `0` to u64::MAX so the documented "0 = no expiry" semantics hold.
    // The inner `AuthConfig::maybe_new` does not special-case `0` — it would
    // treat the token as already expired and fail with an auth error on the
    // first CAS request. `u64::MAX` matches the `None`-expiry default path.
    let token_expiry = if token_expiry == 0.0 {
        u64::MAX
    } else {
        token_expiry as u64
    };
    Ok(token_expiry)
}
