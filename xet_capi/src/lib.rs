//! C ABI bindings for the Hugging Face Xet client (`hf-xet`).
//!
//! All types are opaque handles freed with their `xet_*_free` function.
//! Async transfers use handle-based polling via [`XetOp`]. No callbacks
//! cross the ABI in either direction.

mod error;

pub use error::{XetError, XetStatus};

/// Returns the xet_capi version as a static NUL-terminated C string.
#[unsafe(no_mangle)]
pub extern "C" fn xet_version() -> *const std::os::raw::c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const _
}
