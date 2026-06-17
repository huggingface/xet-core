use std::ops::Range;

use wasm_bindgen::prelude::*;
use xet::xet_session::{XetDownloadStreamGroup as InnerGroup, XetFileInfo};

use crate::common::js_err;
use crate::download_stream::XetDownloadStream;

/// Authenticated stream-download group.
///
/// Mirrors [`xet::xet_session::XetDownloadStreamGroup`]: holds the CAS endpoint
/// and token supplied at construction and exposes
/// [`downloadStream`](Self::download_stream) to stream individual files. One
/// group is reusable across many files; all its streams share the underlying
/// CAS connection pool and auth token. Construct via
/// [`XetSession::new_download_stream_group`] — there is no JS constructor.
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
    /// Begin streaming a file described by `fileInfo`, a plain JS object of the
    /// `XetFileInfo` shape `{ hash: string, file_size: number }`.
    ///
    /// `byteRangeStart` / `byteRangeEnd` are optional; when both are given, only
    /// that byte range is downloaded.
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

    /// Download a file described by `fileInfo` (the `XetFileInfo` shape
    /// `{ hash: string, file_size: number }`) fully into memory, returned as a
    /// `Uint8Array`.
    ///
    /// `byteRangeStart` / `byteRangeEnd` are optional; when both are given, only
    /// that byte range is downloaded. Uses
    /// `FileDownloadSession::download_to_writer`; for large files prefer
    /// [`downloadStream`](Self::download_stream).
    #[wasm_bindgen(js_name = "downloadToBytes")]
    pub async fn download_to_bytes(
        &self,
        file_info: JsValue,
        byte_range_start: Option<f64>,
        byte_range_end: Option<f64>,
    ) -> Result<Vec<u8>, JsValue> {
        let file_info: XetFileInfo = serde_wasm_bindgen::from_value(file_info).map_err(js_err)?;

        let range: Option<Range<u64>> = match (byte_range_start, byte_range_end) {
            (Some(start), Some(end)) => Some(start as u64..end as u64),
            _ => None,
        };

        self.inner.download_to_bytes(file_info, range).await.map_err(js_err)
    }
}
