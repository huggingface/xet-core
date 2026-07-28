use js_sys::{Array, Object};
use wasm_bindgen::prelude::*;
use xet::xet_session::header::HeaderName;
use xet::xet_session::{HeaderMap, HeaderValue};

pub(crate) fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

/// Validated auth inputs for a commit / download-group builder, as resolved by
/// [`resolve_auth_inputs`].
pub(crate) struct AuthInputs {
    pub(crate) endpoint: Option<String>,
    pub(crate) token_info: Option<(String, u64)>,
    pub(crate) token_refresh: Option<(String, HeaderMap)>,
}

/// Treat `undefined`, `null`, `""` and whitespace-only strings alike as "not
/// provided", so JS callers can pass a positional placeholder for an argument
/// they want resolved from the token refresh route instead.
fn optional_string(value: Option<String>) -> Option<String> {
    value.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn validate_url(value: &str, field: &str) -> Result<(), JsValue> {
    if !(value.starts_with("http://") || value.starts_with("https://")) {
        return Err(JsValue::from_str(&format!("{field} must be a valid URL")));
    }
    Ok(())
}

/// Convert a plain JS object of `{ "Header-Name": "value" }` pairs into a [`HeaderMap`].
///
/// `undefined` / `null` yield an empty map. A non-object, or any non-string value,
/// is rejected rather than skipped: silently dropping an `Authorization` header
/// would resurface later as an opaque 401 from the refresh route.
pub(crate) fn parse_header_map(value: JsValue, field: &str) -> Result<HeaderMap, JsValue> {
    let mut headers = HeaderMap::new();
    if value.is_undefined() || value.is_null() {
        return Ok(headers);
    }
    if !value.is_object() {
        return Err(JsValue::from_str(&format!("{field} must be an object of header name/value strings")));
    }

    for entry in Object::entries(&Object::from(value)).iter() {
        let entry = Array::from(&entry);
        let name = entry
            .get(0)
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("{field} has a non-string header name")))?;
        let header_value = entry
            .get(1)
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("{field}[{name:?}] must be a string")))?;

        let name = HeaderName::try_from(name.as_str())
            .map_err(|e| JsValue::from_str(&format!("invalid header name in {field}: {e}")))?;
        let header_value = HeaderValue::from_str(&header_value)
            .map_err(|e| JsValue::from_str(&format!("invalid header value in {field}: {e}")))?;
        headers.insert(name, header_value);
    }

    Ok(headers)
}

/// Validate the auth arguments shared by `newUploadCommit` and
/// `newDownloadStreamGroup`.
///
/// Either an explicit `endpoint` or a `tokenRefreshUrl` must be present — given
/// only the latter, the underlying builder resolves the CAS endpoint by calling
/// the refresh route once during `build()`. Likewise either a `token` +
/// `tokenExpiry` pair or a `tokenRefreshUrl` must be present, since a commit or
/// group with neither could not authenticate a single CAS request.
pub(crate) fn resolve_auth_inputs(
    endpoint: Option<String>,
    token: Option<String>,
    token_expiry: Option<f64>,
    token_refresh_url: Option<String>,
    token_refresh_headers: JsValue,
) -> Result<AuthInputs, JsValue> {
    let endpoint = optional_string(endpoint);
    if let Some(endpoint) = endpoint.as_deref() {
        validate_url(endpoint, "endpoint")?;
    }

    let token_info = match (optional_string(token), token_expiry) {
        (Some(token), Some(expiry)) => {
            if !expiry.is_finite() || expiry <= 0.0 {
                return Err(JsValue::from_str("tokenExpiry must be a finite, positive Unix timestamp (seconds)"));
            }
            Some((token, expiry as u64))
        },
        (None, None) => None,
        _ => return Err(JsValue::from_str("token and tokenExpiry must be provided together")),
    };

    let token_refresh = match optional_string(token_refresh_url) {
        Some(url) => {
            validate_url(&url, "tokenRefreshUrl")?;
            Some((url, parse_header_map(token_refresh_headers, "tokenRefreshHeaders")?))
        },
        None => {
            if !parse_header_map(token_refresh_headers, "tokenRefreshHeaders")?.is_empty() {
                return Err(JsValue::from_str("tokenRefreshHeaders requires tokenRefreshUrl"));
            }
            None
        },
    };

    if endpoint.is_none() && token_refresh.is_none() {
        return Err(JsValue::from_str("endpoint is required unless tokenRefreshUrl is provided"));
    }
    if token_info.is_none() && token_refresh.is_none() {
        return Err(JsValue::from_str("token and tokenExpiry are required unless tokenRefreshUrl is provided"));
    }

    Ok(AuthInputs {
        endpoint,
        token_info,
        token_refresh,
    })
}
