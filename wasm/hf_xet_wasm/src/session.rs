use wasm_bindgen::prelude::*;
use xet::xet_session::{XetSession as InnerSession, XetSessionBuilder};

use crate::common::{js_err, validate_session_inputs};
use crate::download_group::XetDownloadStreamGroup;
use crate::upload_commit::XetUploadCommit;

/// WASM-facing session for both Xet uploads and downloads.
///
/// Mirrors the Rust [`xet::xet_session::XetSession`]: the session itself owns
/// no auth state. Construct with `new XetSession()`, then call
/// [`newUploadCommit`](Self::new_upload_commit) to mint a fresh
/// [`XetUploadCommit`] for uploads, or
/// [`newDownloadStreamGroup`](Self::new_download_stream_group) to mint an
/// authenticated [`XetDownloadStreamGroup`] for downloads. A single session can
/// produce many independent commits and groups, each with its own endpoint /
/// token pair.
///
/// ## No automatic token refresh
///
/// This wrapper does **not** expose `with_token_refresh_url` on either the
/// upload or download builder, so a commit or group has no way to obtain a
/// fresh CAS token mid-transfer. If `tokenExpiry` is reached during an upload
/// or download the underlying request will fail with an auth error. Callers
/// are responsible for fetching a new `xet-write-token` / `xet-read-token`
/// from the Hub and constructing a fresh commit or group before expiry.
///
/// Wiring `with_token_refresh_url` here would need either a JS-callback
/// bridge (so JS can mint and return a token via `Promise`) or a URL-based
/// refresher backed by a route the wasm reqwest client can hit directly; both
/// are out of scope for this example wrapper.
#[wasm_bindgen(js_name = "XetSession")]
pub struct XetSession {
    inner: InnerSession,
}

#[wasm_bindgen(js_class = "XetSession")]
impl XetSession {
    /// Create a new session. Mirrors `XetSessionBuilder::new().build()` and
    /// takes no auth — auth lives on the per-commit / per-group builder.
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
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds). Pass the real `exp` from the Hub `xet-write-token`
    ///   response. Must be a positive value; a value at or before now-ish causes the underlying client to treat the
    ///   token as expired and fail with an auth error on the first CAS request, since this wrapper does not wire a
    ///   token refresher.
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

    /// Build an authenticated [`XetDownloadStreamGroup`].
    ///
    /// - `endpoint`: CAS server URL, e.g. `"https://cas-server.xethub.com"`
    /// - `token`: CAS access token string
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds). Pass the real `exp` from the Hub `xet-read-token`
    ///   response. Must be a positive value; a value at or before now-ish causes the underlying client to treat the
    ///   token as expired and fail with an auth error on the first CAS request, since this wrapper does not wire a
    ///   token refresher.
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
