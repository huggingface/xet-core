use std::ops::Range;

use wasm_bindgen::prelude::*;
use xet::xet_session::{XetFileInfo, XetSession as InnerSession, XetSessionBuilder};

use crate::stream::XetDownloadStream;

fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

/// WASM-facing session for streaming downloads from the Xet CAS server.
///
/// Construct one with `new(endpoint, token, tokenExpiry)`, then call
/// `downloadStream(fileInfo)` to begin streaming a file.
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
    /// - `endpoint`: CAS server URL, e.g. `"https://cas-server.xethub.com"`
    /// - `token`: CAS access token string
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds). Pass `0` for no expiry.
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

    /// Begin streaming a file described by `fileInfo`.
    ///
    /// `fileInfo` must be a plain JS object matching the `XetFileInfo` shape:
    /// `{ hash: string, file_size: number }`.
    ///
    /// `byteRangeStart` and `byteRangeEnd` are optional; when both are provided,
    /// only that byte range is downloaded.
    #[wasm_bindgen(js_name = "downloadStream")]
    pub async fn download_stream(
        &self,
        file_info: JsValue,
        byte_range_start: Option<f64>,
        byte_range_end: Option<f64>,
    ) -> Result<XetDownloadStream, JsValue> {
        let file_info: XetFileInfo = serde_wasm_bindgen::from_value(file_info).map_err(js_err)?;

        let range: Option<Range<u64>> = match (byte_range_start, byte_range_end) {
            (Some(start), Some(end)) => Some(start as u64..end as u64),
            _ => None,
        };

        let group = self
            .inner
            .new_download_stream_group()
            .map_err(js_err)?
            .with_endpoint(&self.endpoint)
            .with_token_info(self.token.clone(), self.token_expiry)
            .build()
            .await
            .map_err(js_err)?;

        let stream = group.download_stream(file_info, range).await.map_err(js_err)?;

        Ok(XetDownloadStream::new(stream))
    }
}
