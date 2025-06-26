#[cfg(not(target_family = "wasm"))]
compile_error!("This crate is only meant to be used on the WebAssembly target");

mod auth;
pub mod blob_reader;
pub mod configurations;
mod errors;
mod session;
mod sha256;
mod wasm_deduplication_interface;
mod wasm_file_cleaner;
pub mod wasm_file_upload_session;
pub mod wasm_timer;
mod xorb_uploader;

pub use session::XetSession;

// sample test
#[cfg(test)]
mod tests {
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    #[test]
    #[wasm_bindgen_test]
    fn simple_test() {
        assert_eq!(1, 1);
    }
}
