//! Example / smoke-test `#[wasm_bindgen]` wrapper around
//! `xet::xet_session::XetSession` for downloads.
//!
//! This crate is **not** a published browser SDK. It exists so the wasm
//! build of `xet_pkg` is exercised end-to-end in CI and so we have a
//! hand-runnable browser page for manual testing. Real browser consumers
//! should depend on `hf-xet` directly with their own `#[wasm_bindgen]`
//! glue, or use a downstream SDK such as `huggingface.js`. The JS surface
//! exposed here is not versioned and may change without notice. See
//! `README.md` for the full positioning.

#[cfg(not(target_family = "wasm"))]
compile_error!("hf_xet_wasm_download is only for the wasm32-unknown-unknown target");

mod group;
mod session;
mod stream;

use wasm_bindgen::prelude::*;

pub use group::XetDownloadStreamGroup;
pub use session::XetSession;
pub use stream::XetDownloadStream;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    let _ = console_log::init_with_level(log::Level::Info);
}
