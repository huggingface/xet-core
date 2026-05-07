//! Minimal napi smoke-test binding for `hf-xet` (the `xet` crate at `xet_pkg/`).
//!
//! The goal here is *not* a full upload/download API surface — it's to prove the
//! Rust client compiles, links, and starts inside a Node.js native addon
//! (libuv main thread, no Python interpreter, no tokio runtime owned by the
//! host). If this loads and `smokeTest()` returns without panicking, the
//! integration is ready for a fuller binding.
//!
//! Exposes:
//!   - `initLogging(version)`  — install xet's tracing subscriber
//!   - `smokeTest()`           — build a `XetSession` synchronously, return a
//!                               short status string. Exercises xet-runtime
//!                               startup from inside a JS-hosted thread.

use napi::Error as NapiError;
use napi::Status;
use napi_derive::napi;
use xet::xet_session::XetSessionBuilder;

fn to_napi_err<E: std::fmt::Display>(e: E) -> NapiError {
    NapiError::new(Status::GenericFailure, e.to_string())
}

#[napi(js_name = "initLogging")]
pub fn init_logging(version: String) {
    xet::init_logging(version);
}

#[napi(js_name = "smokeTest")]
pub fn smoke_test() -> Result<String, NapiError> {
    let session = XetSessionBuilder::new().build().map_err(to_napi_err)?;

    // Build (and immediately drop) auth-group builders to exercise the parts of
    // the runtime that lazily spin up workers.
    let _upload = session.new_upload_commit().map_err(to_napi_err)?;
    let _download = session.new_file_download_group().map_err(to_napi_err)?;

    Ok("xet session built; runtime initialized".to_string())
}
