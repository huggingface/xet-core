use wasm_bindgen::prelude::*;
use xet::xet_session::XetDownloadStream as InnerStream;

/// Streaming download handle.
///
/// Both [`next`](Self::next) and [`cancel`](Self::cancel) take `&mut self`, and wasm-bindgen
/// enforces this borrow at runtime. JS callers MUST NOT call them concurrently: calling
/// `cancel()` while a `next()` Promise is still pending (or calling `next()` again before the
/// previous resolves) throws `"recursive use of an object detected"`. Always `await` a pending
/// `next()` before calling either again.
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
    /// Borrows the stream mutably until the returned Promise resolves; see the type docs for
    /// the no-concurrent-use rule.
    #[wasm_bindgen]
    pub async fn next(&mut self) -> Result<JsValue, JsValue> {
        match self.inner.next().await.map_err(|e| JsValue::from_str(&format!("{e:?}")))? {
            Some(bytes) => Ok(js_sys::Uint8Array::from(bytes.as_ref()).into()),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Cancels the in-progress download. Must not be called while a `next()` Promise is still
    /// pending; see the type docs for the no-concurrent-use rule.
    #[wasm_bindgen]
    pub fn cancel(&mut self) {
        self.inner.cancel();
    }
}
