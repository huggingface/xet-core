//! C ABI bindings for the Hugging Face Xet client (`hf-xet`).
//!
//! All types are opaque handles freed with their `xet_*_free` function.
//! Async transfers use handle-based polling via [`XetOp`]. No callbacks
//! cross the ABI in either direction.

mod bytes;
mod error;
mod handle;

pub use bytes::{XetBytes, xet_bytes_data, xet_bytes_free, xet_bytes_len, xet_test_make_bytes};
pub use error::{XetError, XetStatus, xet_error_code, xet_error_free, xet_error_message, xet_test_make_auth_error};

/// Returns the xet_capi version as a static NUL-terminated C string.
#[unsafe(no_mangle)]
pub extern "C" fn xet_version() -> *const std::os::raw::c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const _
}
