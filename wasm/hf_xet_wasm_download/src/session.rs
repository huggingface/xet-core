use wasm_bindgen::prelude::*;
use xet::xet_session::{XetSession as InnerSession, XetSessionBuilder};

use crate::group::XetDownloadStreamGroup;

fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

fn validate_session_inputs(endpoint: &str, token: &str, token_expiry: f64) -> Result<u64, JsValue> {
    if !token_expiry.is_finite() || token_expiry < 0.0 {
        return Err(JsValue::from_str(
            "tokenExpiry must be a finite, non-negative number",
        ));
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

/// WASM-facing session for streaming downloads from the Xet CAS server.
///
/// Mirrors the Rust [`xet::xet_session::XetSession`]: the session itself owns
/// no auth state. Construct with `new XetSession()`, then call
/// [`newDownloadStreamGroup`](Self::new_download_stream_group) to mint an
/// authenticated [`XetDownloadStreamGroup`] from which you can stream files.
///
/// A single session can hand out many groups, each with its own endpoint /
/// token pair.
///
/// ## No automatic token refresh
///
/// This wrapper does **not** expose
/// [`with_token_refresh_url`](xet::xet_session::AuthGroupBuilder::with_token_refresh_url),
/// so a group has no way to obtain a fresh CAS token mid-stream. If
/// `tokenExpiry` is reached during a download the underlying request will
/// fail with an auth error. Callers are responsible for fetching a new
/// `xet-read-token` from the Hub and constructing a fresh group before expiry.
///
/// Wiring `with_token_refresh_url` here would need either a JS-callback
/// bridge (so JS can mint and return a token via `Promise`) or a
/// URL-based refresher backed by a route the wasm reqwest client can hit
/// directly; both are out of scope for the initial download-only wrapper.
#[wasm_bindgen(js_name = "XetSession")]
pub struct XetSession {
    inner: InnerSession,
}

#[wasm_bindgen(js_class = "XetSession")]
impl XetSession {
    /// Create a new session. Mirrors `XetSessionBuilder::new().build()` and
    /// takes no auth — auth lives on the per-group builder.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<XetSession, JsValue> {
        let session = XetSessionBuilder::new().build().map_err(js_err)?;
        Ok(Self { inner: session })
    }

    /// Build an authenticated [`XetDownloadStreamGroup`].
    ///
    /// - `endpoint`: CAS server URL, e.g. `"https://cas-server.xethub.com"`
    /// - `token`: CAS access token string
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds). Pass `0` for no expiry.
    ///
    /// The returned group is reusable across many `downloadStream(...)` calls.
    #[wasm_bindgen(js_name = "newDownloadStreamGroup")]
    pub async fn new_download_stream_group(
        &self,
        endpoint: String,
        token: String,
        token_expiry: f64,
    ) -> Result<XetDownloadStreamGroup, JsValue> {
        let token_expiry = validate_session_inputs(&endpoint, &token, token_expiry)?;

        let group = self
            .inner
            .new_download_stream_group()
            .map_err(js_err)?
            .with_endpoint(&endpoint)
            .with_token_info(token, token_expiry)
            .build()
            .await
            .map_err(js_err)?;

        Ok(XetDownloadStreamGroup::new(group))
    }
}
