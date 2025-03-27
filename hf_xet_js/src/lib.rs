mod types;
mod utils;
mod error;

pub use types::*;
use utils::sha256_from_reader;
use wasm_bindgen_file_reader::WebSysFile;

use merklehash::MerkleHash;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use wasm_bindgen::JsCast;
use wasm_bindgen::{prelude::*, JsObject};
use web_sys::js_sys::{Function, Reflect, Uint8Array};
use web_sys::{console, Blob, File, ReadableStream, ReadableStreamDefaultController};
use crate::error::HFXetJSError;

static CALL_COUNT: AtomicU32 = AtomicU32::new(0);

struct ConsoleLogger;

impl Write for ConsoleLogger {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        let as_str = std::str::from_utf8(buf).map_err(std::io::Error::other)?;
        console::log_1(&JsValue::from_str(as_str));
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn _log(args: std::fmt::Arguments<'_>) {
    ConsoleLogger.write_fmt(args).expect("failed to console.log");
}

macro_rules! log {
    ($($arg:tt)*) => {{
        _log(std::format_args!($($arg)*));
    }};
}

// fn log<T: ToString>(message: T) {
//     console::log_1(&JsValue::from_str(&message.to_string()));
// }

// #[wasm_bindgen]
// extern "C" {
//     type TokenRefresher;
//
//     #[wasm_bindgen(method)]
//     async fn refresh_token(this: &TokenRefresher) -> JsValue;
// }

///
/// class TokenRefresher {
///     constructor(url: string, token: string) {
///         this.url = url;
///         this.token = token;
///     }
///     
///     async function refresh_token() {
///         const response = await fetch(this.url, {
///             method: 'POST',
///             body: JSON.stringify({ token: this.token }),
///         });
///         const data = await response.json();
///         return data.token;
///     }
/// }
///

#[wasm_bindgen]
pub async fn upload_async(
    files: Vec<File>,
    url: String,
    token: String,
) -> Result<JsValue, JsValue> {
    log!("upload_async");
    let output = _upload_async(files, url, token).await?;
    serde_wasm_bindgen::to_value(&output).map_err(JsValue::from)
}

pub async fn _upload_async(
    files: Vec<File>,
    url: String,
    token: String,
) -> Result<Vec<PointerFile>, HFXetJSError> {
    let value = CALL_COUNT.fetch_add(1, SeqCst);
    log!("call count value = {value}");

    let files_it = files.into_iter().map(|file| {
        let path = file.name().to_string();
        let size = file.size();
        log!("path = {path:?}; size = {size:?}");
        let reader = WebSysFile::new(file);
        (reader, path, size)
    });

    // if files.is_empty() {
    //     return Err(SharedWorkerError::invalid_arguments("no files provided"));
    // }
    // if files.len() != file_paths.len() {
    //     return Err(SharedWorkerError::invalid_arguments(
    //         "files array length does not match file_paths array",
    //     ));
    // }
    // if url.is_empty() || token.is_empty() {
    //     return Err(SharedWorkerError::invalid_arguments(
    //         "url and/or token are missing",
    //     ));
    // }
    //
    // log("uploading files passed validation");

    Ok(files_it
        .map(|(mut reader, path, size)| {
            reader.seek(SeekFrom::Start(0)).unwrap();
            PointerFile {
                hash: MerkleHash::default(),
                size: size as u64,
                path,
                sha256: sha256_from_reader(&mut reader).unwrap(),
            }
        })
        .collect())
}

#[wasm_bindgen]
pub async fn download_async(
    repo: String,
    file: String,
    writer: Blob,
    url: String,
    token: String,
) -> Result<JsValue, JsValue> {
    log!("download_async");
    let output = _download_async(repo, file, writer, url, token).await?;
    serde_wasm_bindgen::to_value(&output).map_err(JsValue::from)
}

async fn _download_async(
    _repo: String,
    _file: String,
    _writer: Blob,
    _url: String,
    _token: String,
) -> Result<(), HFXetJSError> {
    Ok(())
}

// #[wasm_bindgen]
// struct MyReadableStream;
//
// #[wasm_bindgen]
// impl MyReadableStream {}
//
// #[wasm_bindgen]
// pub struct ByteProducer {
//     offset: usize,
//     total: usize,
// }
//
// #[wasm_bindgen]
// impl ByteProducer {
//     #[wasm_bindgen(constructor)]
//     pub fn new(total: usize) -> ByteProducer {
//         ByteProducer { offset: 0, total }
//     }
//
//     #[wasm_bindgen]
//     pub fn into_stream(self) -> Result<ReadableStream, JsValue> {
//         let producer = std::rc::Rc::new(std::cell::RefCell::new(self));
//
//         let pull_producer = producer.clone();
//         let pull_closure = Closure::wrap(Box::new(
//             move |controller: ReadableStreamDefaultController| {
//                 let mut prod = pull_producer.borrow_mut();
//
//                 // Simulate generating 8 bytes per pull
//                 let chunk_size = 8;
//                 let remaining = prod.total - prod.offset;
//                 let size = chunk_size.min(remaining);
//
//                 let mut chunk = vec![0u8; size];
//                 for i in 0..size {
//                     chunk[i] = (prod.offset + i) as u8; // Example: Fill with offset values
//                 }
//
//                 prod.offset += size;
//
//                 let array = Uint8Array::from(&chunk[..]);
//                 controller.enqueue_with_chunk(&array).unwrap();
//
//                 if prod.offset >= prod.total {
//                     controller.close().unwrap();
//                 }
//             },
//         ) as Box<dyn FnMut(_)>);
//
//         let underlying_source = JsValue::from_str("hopeful");
//
//         Reflect::set(
//             &underlying_source,
//             &JsValue::from_str("pull"),
//             pull_closure.as_ref().unchecked_ref(),
//         )?;
//
//         pull_closure.forget(); // Prevents dropping the closure prematurely
//
//         let stream = ReadableStream::new_with_underlying_source(&underlying_source)?;
//
//         Ok(stream)
//     }
// }
