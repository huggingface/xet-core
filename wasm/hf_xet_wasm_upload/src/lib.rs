//! Example / smoke-test `#[wasm_bindgen]` wrapper around
//! `xet::xet_session::XetSession` for uploads.
//!
//! This crate is **not** a published browser SDK. It exists so the wasm
//! build of the upload data-prep path is exercised end-to-end in CI (the
//! regression guard for the `XetRuntime::spawn_blocking` panic) and so we
//! have a hand-runnable browser page for manual testing. Real browser
//! consumers should depend on `hf-xet` directly with their own
//! `#[wasm_bindgen]` glue, or use a downstream SDK such as
//! `huggingface.js`. The JS surface exposed here is not versioned and may
//! change without notice. See `README.md` for the full positioning.

#[cfg(not(target_family = "wasm"))]
compile_error!("hf_xet_wasm_upload is only for the wasm32-unknown-unknown target");

mod session;
mod upload_commit;

use wasm_bindgen::prelude::*;

pub use session::XetSession;
pub use upload_commit::{XetStreamUpload, XetUploadCommit};

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    let _ = console_log::init_with_level(log::Level::Info);
}
