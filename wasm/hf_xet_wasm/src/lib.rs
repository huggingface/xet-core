//! Example / smoke-test `#[wasm_bindgen]` wrapper around
//! `xet::xet_session::XetSession`, exposing upload and download flows to JS.
//!
//! Not a published browser SDK: it exists to exercise the wasm build of
//! `xet_pkg` end-to-end in CI (including the regression guard for the
//! `XetRuntime::spawn_blocking` panic on the upload data-prep path) and to
//! provide hand-runnable browser pages for manual testing. Real consumers
//! should depend on `hf-xet` directly with their own `#[wasm_bindgen]` glue.
//! The JS surface here is unversioned and may change without notice; see
//! `README.md`.

#[cfg(not(target_family = "wasm"))]
compile_error!("hf_xet_wasm is only for the wasm32-unknown-unknown target");

mod common;
mod download_group;
mod download_stream;
mod session;
mod upload_commit;

pub use download_group::XetDownloadStreamGroup;
pub use download_stream::XetDownloadStream;
pub use session::XetSession;
pub use upload_commit::{XetStreamUpload, XetUploadCommit};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    let _ = console_log::init_with_level(log::Level::Info);
}
