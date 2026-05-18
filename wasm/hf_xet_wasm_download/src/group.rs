use std::ops::Range;

use wasm_bindgen::prelude::*;
use xet::xet_session::{XetDownloadStreamGroup as InnerGroup, XetFileInfo};

use crate::stream::XetDownloadStream;

fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

/// Authenticated stream-download group.
///
/// Mirrors [`xet::xet_session::XetDownloadStreamGroup`]: holds the CAS endpoint
/// and token that were supplied when the group was constructed, and exposes
/// [`downloadStream`](Self::download_stream) to begin streaming individual
/// files. A single group can be reused across many files; all streams created
/// from the same group share the underlying CAS connection pool and auth token.
///
/// Construct via [`XetSession::new_download_stream_group`] — there is no JS
/// constructor.
///
/// Cloning is cheap (the inner group is reference-counted), but we don't expose
/// `clone` to JS today; if you need multiple references, hold onto the same
/// handle.
#[wasm_bindgen(js_name = "XetDownloadStreamGroup")]
pub struct XetDownloadStreamGroup {
    inner: InnerGroup,
}

impl XetDownloadStreamGroup {
    pub(crate) fn new(inner: InnerGroup) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = "XetDownloadStreamGroup")]
impl XetDownloadStreamGroup {
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

        let stream = self.inner.download_stream(file_info, range).await.map_err(js_err)?;

        Ok(XetDownloadStream::new(stream))
    }
}
