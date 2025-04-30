pub mod blob_reader;
mod configurations;
mod errors;
mod sha256;
mod wasm_deduplication_interface;
mod wasm_file_cleaner;
pub mod wasm_file_upload_session;

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
