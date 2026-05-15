use wasm_bindgen::prelude::*;
use xet::xet_session::{XetSession as InnerSession, XetSessionBuilder};

use crate::upload_commit::XetUploadCommit;

fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

/// WASM-facing session for uploading files via the Xet upload commit API.
///
/// Construct with `new(endpoint, token, tokenExpiry)`, then call
/// `newUploadCommit()` to begin a commit. Per-session, multiple commits
/// are allowed (each one is a separate atomic upload group).
#[wasm_bindgen(js_name = "XetSession")]
pub struct XetSession {
    inner: InnerSession,
    endpoint: String,
    token: String,
    token_expiry: u64,
}

#[wasm_bindgen(js_class = "XetSession")]
impl XetSession {
    /// Create a new session.
    ///
    /// - `endpoint`: CAS server URL.
    /// - `token`: CAS access token string.
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds). Pass `0`
    ///   for no expiry.
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: String, token: String, token_expiry: f64) -> Result<XetSession, JsValue> {
        let session = XetSessionBuilder::new().build().map_err(js_err)?;
        Ok(Self {
            inner: session,
            endpoint,
            token,
            token_expiry: token_expiry as u64,
        })
    }

    /// Begin a new upload commit. Resolves to an `XetUploadCommit` to which
    /// you can `uploadBytes(...)` / `uploadStream(...)` and finally `commit()`.
    #[wasm_bindgen(js_name = "newUploadCommit")]
    pub async fn new_upload_commit(&self) -> Result<XetUploadCommit, JsValue> {
        let commit = self
            .inner
            .new_upload_commit()
            .map_err(js_err)?
            .with_endpoint(&self.endpoint)
            .with_token_info(self.token.clone(), self.token_expiry)
            .build()
            .await
            .map_err(js_err)?;
        Ok(XetUploadCommit::new(commit))
    }
}
