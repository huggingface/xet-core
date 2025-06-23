use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys::{Reflect, Uint8Array};
use web_sys::wasm_bindgen::{JsCast, JsValue};
use web_sys::{Blob, ReadableStreamDefaultReader};

#[derive(Default)]
enum BlobReaderState {
    #[default]
    Init,
    Done,
    LeftOverBuf(Vec<u8>),
    AwaitBuf(Pin<Box<JsFuture>>),
}

/// BlobReader provides a wrapper over a web_sys::Blob to make it futures::AsyncRead
/// when reading Files/Blobs this provides a more rusty interface.
pub struct BlobReader {
    // https://developer.mozilla.org/en-US/docs/Web/API/ReadableStreamDefaultReader
    reader: ReadableStreamDefaultReader,
    state: RefCell<Option<BlobReaderState>>,
}

impl BlobReader {
    pub fn new(blob: Blob) -> Result<Self, JsValue> {
        let reader: ReadableStreamDefaultReader = blob.stream().get_reader().dyn_into()?;
        Ok(Self {
            reader,
            state: RefCell::new(Some(BlobReaderState::Init)),
        })
    }
}

fn std_io_err_from_js_value(js_value: JsValue) -> std::io::Error {
    std::io::Error::other(js_value.as_string().unwrap_or("js value error".to_string()))
}

impl futures::AsyncRead for BlobReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let Some(state) = self.state.take() else {
            // when entering and leaving poll_read the inner state must be set to Some(<state>)
            return Poll::Ready(Err(std::io::Error::other("state invariant violated")));
        };

        let (poll_result, next_state) = match state {
            BlobReaderState::Done => (Poll::Ready(Ok(0)), BlobReaderState::Done),
            BlobReaderState::Init => {
                cx.waker().wake_by_ref();
                (Poll::Pending, BlobReaderState::AwaitBuf(Box::pin(JsFuture::from(self.reader.read()))))
            },
            BlobReaderState::LeftOverBuf(mut inner) => {
                let bytes_to_copy = inner.len().min(buf.len());
                buf[..bytes_to_copy].copy_from_slice(&inner[..bytes_to_copy]);
                // copy as many bytes as possible to buf
                let next_state = if inner.len() > bytes_to_copy {
                    inner.drain(..bytes_to_copy);
                    BlobReaderState::LeftOverBuf(inner)
                } else {
                    BlobReaderState::AwaitBuf(Box::pin(JsFuture::from(self.reader.read())))
                };
                (Poll::Ready(Ok(bytes_to_copy)), next_state)
            },
            BlobReaderState::AwaitBuf(mut promise) => {
                // result from: https://developer.mozilla.org/en-US/docs/Web/API/ReadableStreamDefaultReader/read
                let Poll::Ready(res) = promise.as_mut().poll(cx) else {
                    self.state.replace(Some(BlobReaderState::AwaitBuf(promise)));
                    return Poll::Pending;
                };
                let js_value = res.map_err(std_io_err_from_js_value)?;
                if !js_value.is_object() {
                    self.state.replace(Some(BlobReaderState::Done));
                    return Poll::Ready(Err(std::io::Error::other("unexpected value")));
                }
                let done = Reflect::get(&js_value, &"done".into())
                    .map_err(std_io_err_from_js_value)?
                    .as_bool()
                    .unwrap_or(true);
                if done {
                    self.state.replace(Some(BlobReaderState::Done));
                    return Poll::Ready(Ok(0));
                }
                let js_data: Uint8Array = Reflect::get(&js_value, &"value".into())
                    .map_err(std_io_err_from_js_value)?
                    .dyn_into()
                    .map_err(std_io_err_from_js_value)?;
                let data = js_data.to_vec();
                cx.waker().wake_by_ref();
                (Poll::Pending, BlobReaderState::LeftOverBuf(data))
            },
        };
        self.state.replace(Some(next_state));
        poll_result
    }
}
