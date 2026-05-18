use wasm_bindgen::prelude::*;
use xet::xet_session::XetDownloadStream as InnerStream;

/// Streaming download handle.
///
/// Both [`next`](Self::next) and [`cancel`](Self::cancel) take `&mut self`, and wasm-bindgen
/// enforces this borrow at runtime on the JS side. JS callers MUST NOT invoke `next()` and
/// `cancel()` concurrently: calling `cancel()` while a `next()` Promise is still pending (or
/// calling `next()` again before the previous one resolves) will throw
/// `"recursive use of an object detected"` at runtime.
///
/// Safe pattern: always `await` the pending `next()` Promise before calling `cancel()`, or
/// arrange the JS control flow so `cancel()` only runs after `next()` has resolved.
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
    ///
    /// Borrows the stream mutably for the lifetime of the returned Promise. Do not call
    /// `next()` or `cancel()` again until this Promise has resolved, or wasm-bindgen will
    /// throw `"recursive use of an object detected"`.
    #[wasm_bindgen]
    pub async fn next(&mut self) -> Result<JsValue, JsValue> {
        match self.inner.next().await.map_err(|e| JsValue::from_str(&format!("{e:?}")))? {
            Some(bytes) => Ok(js_sys::Uint8Array::from(bytes.as_ref()).into()),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Cancels the in-progress download.
    ///
    /// Must not be called while a `next()` Promise is still pending; await that Promise
    /// first. Concurrent invocation will throw `"recursive use of an object detected"` at
    /// runtime.
    #[wasm_bindgen]
    pub fn cancel(&mut self) {
        self.inner.cancel();
    }
}
