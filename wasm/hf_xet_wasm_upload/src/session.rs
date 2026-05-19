use wasm_bindgen::prelude::*;
use xet::xet_session::{XetSession as InnerSession, XetSessionBuilder};

use crate::upload_commit::XetUploadCommit;

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

/// WASM-facing session for uploading files via the Xet upload commit API.
///
/// Mirrors the Rust [`xet::xet_session::XetSession`]: the session itself owns
/// no auth state. Construct with `new XetSession()`, then call
/// [`newUploadCommit`](Self::new_upload_commit) — passing the endpoint and
/// token — to mint a fresh [`XetUploadCommit`]. A single session can produce
/// many independent commits.
#[wasm_bindgen(js_name = "XetSession")]
pub struct XetSession {
    inner: InnerSession,
}

#[wasm_bindgen(js_class = "XetSession")]
impl XetSession {
    /// Create a new session. Mirrors `XetSessionBuilder::new().build()` and
    /// takes no auth — auth lives on the per-commit builder.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<XetSession, JsValue> {
        let session = XetSessionBuilder::new().build().map_err(js_err)?;
        Ok(Self { inner: session })
    }

    /// Begin a new upload commit. Resolves to an `XetUploadCommit` to which
    /// you can `uploadBytes(...)` / `uploadStream(...)` and finally `commit()`.
    ///
    /// - `endpoint`: CAS server URL.
    /// - `token`: CAS access token string.
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds). Pass the
    ///   real `exp` from the Hub `xet-write-token` response. `0` is accepted
    ///   as a sentinel for "no expiry" (mapped to `u64::MAX` internally), but
    ///   any other value at or before now-ish causes the underlying client to
    ///   treat the token as expired and fail with an auth error on the first
    ///   CAS request, since this wrapper does not wire a token refresher.
    #[wasm_bindgen(js_name = "newUploadCommit")]
    pub async fn new_upload_commit(
        &self,
        endpoint: String,
        token: String,
        token_expiry: f64,
    ) -> Result<XetUploadCommit, JsValue> {
        let token_expiry = validate_session_inputs(&endpoint, &token, token_expiry)?;

        let commit = self
            .inner
            .new_upload_commit()
            .map_err(js_err)?
            .with_endpoint(&endpoint)
            .with_token_info(token, token_expiry)
            .build()
            .await
            .map_err(js_err)?;
        Ok(XetUploadCommit::new(commit))
    }
}
