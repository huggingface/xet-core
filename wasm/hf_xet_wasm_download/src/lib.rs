#[cfg(not(target_family = "wasm"))]
compile_error!("hf_xet_wasm_download is only for the wasm32-unknown-unknown target");

mod session;
mod stream;

use wasm_bindgen::prelude::*;

pub use session::XetSession;
pub use stream::XetDownloadStream;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    let _ = console_log::init_with_level(log::Level::Info);
}
