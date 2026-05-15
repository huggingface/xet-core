use wasm_bindgen::prelude::*;
use xet::xet_session::XetDownloadStream as InnerStream;

#[wasm_bindgen(js_name = "XetDownloadStream")]
pub struct XetDownloadStream {
    inner: InnerStream,
}

impl XetDownloadStream {
    pub(crate) fn new(inner: InnerStream) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = "XetDownloadStream")]
impl XetDownloadStream {
    /// Returns the next chunk as a `Uint8Array`, or `undefined` when the stream is complete.
    #[wasm_bindgen]
    pub async fn next(&mut self) -> Result<JsValue, JsValue> {
        match self.inner.next().await.map_err(|e| JsValue::from_str(&format!("{e:?}")))? {
            Some(bytes) => Ok(js_sys::Uint8Array::from(bytes.as_ref()).into()),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Cancels the in-progress download.
    #[wasm_bindgen]
    pub fn cancel(&mut self) {
        self.inner.cancel();
    }
}
