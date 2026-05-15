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
