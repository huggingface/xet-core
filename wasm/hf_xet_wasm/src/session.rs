use wasm_bindgen::prelude::*;
use xet::xet_session::{XetSession as InnerSession, XetSessionBuilder};

use crate::common::{AuthInputs, js_err, resolve_auth_inputs};
use crate::download_group::XetDownloadStreamGroup;
use crate::upload_commit::XetUploadCommit;

/// WASM-facing session for both Xet uploads and downloads.
///
/// Mirrors the Rust [`xet::xet_session::XetSession`]: the session owns no auth
/// state. Construct with `new XetSession()`, then call
/// [`newUploadCommit`](Self::new_upload_commit) for a fresh [`XetUploadCommit`]
/// or [`newDownloadStreamGroup`](Self::new_download_stream_group) for an
/// authenticated [`XetDownloadStreamGroup`]. One session can produce many
/// independent commits and groups, each with its own endpoint / token pair.
///
/// ## Token refresh
///
/// Both builders accept an optional `tokenRefreshUrl` plus `tokenRefreshHeaders`,
/// wiring the same URL-based refresher the native API exposes through
/// `with_token_refresh_url`. Point it at the Hub `xet-write-token` /
/// `xet-read-token` route for the repo and pass the Hub token in the headers:
///
/// ```js
/// const group = await session.newDownloadStreamGroup(
///   null, null, null,
///   'https://huggingface.co/api/models/org/repo/xet-read-token/main',
///   { Authorization: `Bearer ${hfToken}` },
/// );
/// ```
///
/// The route must answer `{ accessToken, exp, casUrl }`. With a refresh URL set,
/// `endpoint` / `token` / `tokenExpiry` all become optional: the missing pieces
/// are resolved by calling the route once during construction, and the CAS token
/// is then kept fresh for the lifetime of the commit or group.
///
/// Without a refresh URL, `endpoint`, `token` and `tokenExpiry` are all required
/// and the CAS token cannot be refreshed mid-transfer — once `tokenExpiry` passes,
/// requests fail with an auth error and the caller must build a new commit or group.
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

    /// Begin a new upload commit. Resolves to an `XetUploadCommit` for
    /// `uploadBytes(...)` / `uploadStream(...)` and finally `commit()`.
    ///
    /// - `endpoint`: CAS server URL. Optional when `tokenRefreshUrl` is given, in which case the route's `casUrl` is
    ///   used.
    /// - `token`: CAS access token string. Optional when `tokenRefreshUrl` is given.
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds), the real `exp` from the Hub `xet-write-token`
    ///   response. Must be positive, and must be passed together with `token`. Without a `tokenRefreshUrl`, an
    ///   already-expired value fails with an auth error on the first CAS request.
    /// - `tokenRefreshUrl`: optional Hub `xet-write-token` route used to mint fresh CAS tokens as the current one nears
    ///   expiry.
    /// - `tokenRefreshHeaders`: optional headers object sent with every refresh request, e.g. `{ Authorization: "Bearer
    ///   <hub-token>" }`. Requires `tokenRefreshUrl`.
    #[wasm_bindgen(js_name = "newUploadCommit")]
    pub async fn new_upload_commit(
        &self,
        endpoint: Option<String>,
        token: Option<String>,
        token_expiry: Option<f64>,
        token_refresh_url: Option<String>,
        token_refresh_headers: JsValue,
    ) -> Result<XetUploadCommit, JsValue> {
        let AuthInputs {
            endpoint,
            token_info,
            token_refresh,
        } = resolve_auth_inputs(endpoint, token, token_expiry, token_refresh_url, token_refresh_headers)?;

        let mut builder = self.inner.new_upload_commit().map_err(js_err)?;
        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some((token, token_expiry)) = token_info {
            builder = builder.with_token_info(token, token_expiry);
        }
        if let Some((url, headers)) = token_refresh {
            builder = builder.with_token_refresh_url(url, headers);
        }

        let commit = builder.build().await.map_err(js_err)?;
        Ok(XetUploadCommit::new(commit))
    }

    /// Build an authenticated [`XetDownloadStreamGroup`], reusable across many
    /// `downloadStream(...)` calls.
    ///
    /// - `endpoint`: CAS server URL, e.g. `"https://cas-server.xethub.com"`. Optional when `tokenRefreshUrl` is given,
    ///   in which case the route's `casUrl` is used.
    /// - `token`: CAS access token string. Optional when `tokenRefreshUrl` is given.
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds), the real `exp` from the Hub `xet-read-token`
    ///   response. Must be positive, and must be passed together with `token`. Without a `tokenRefreshUrl`, an
    ///   already-expired value fails with an auth error on the first CAS request.
    /// - `tokenRefreshUrl`: optional Hub `xet-read-token` route used to mint fresh CAS tokens as the current one nears
    ///   expiry.
    /// - `tokenRefreshHeaders`: optional headers object sent with every refresh request, e.g. `{ Authorization: "Bearer
    ///   <hub-token>" }`. Requires `tokenRefreshUrl`.
    #[wasm_bindgen(js_name = "newDownloadStreamGroup")]
    pub async fn new_download_stream_group(
        &self,
        endpoint: Option<String>,
        token: Option<String>,
        token_expiry: Option<f64>,
        token_refresh_url: Option<String>,
        token_refresh_headers: JsValue,
    ) -> Result<XetDownloadStreamGroup, JsValue> {
        let AuthInputs {
            endpoint,
            token_info,
            token_refresh,
        } = resolve_auth_inputs(endpoint, token, token_expiry, token_refresh_url, token_refresh_headers)?;

        let mut builder = self.inner.new_download_stream_group().map_err(js_err)?;
        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some((token, token_expiry)) = token_info {
            builder = builder.with_token_info(token, token_expiry);
        }
        if let Some((url, headers)) = token_refresh {
            builder = builder.with_token_refresh_url(url, headers);
        }

        let group = builder.build().await.map_err(js_err)?;
        Ok(XetDownloadStreamGroup::new(group))
    }
}
