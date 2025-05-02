#[cfg(not(target_family = "wasm"))]
compile_error!("This crate is only meant to be used on the WebAssembly target");

pub mod blob_reader;
pub mod configurations;
mod errors;
mod sha256;
mod wasm_deduplication_interface;
mod wasm_file_cleaner;
pub mod wasm_file_upload_session;
mod xorb_uploader;
mod interface;

pub use interface::{
    auth::{TokenInfo, TokenRefresher},
    session::XetSession,
};
