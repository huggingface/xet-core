//! C ABI bindings for the Hugging Face Xet client (`hf-xet`).
//!
//! All types are opaque handles freed with their `xet_*_free` function.
//! Async transfers use handle-based polling via [`XetOp`]. No callbacks
//! cross the ABI in either direction.

mod bytes;
mod error;
mod file_info;
mod handle;
mod op;
mod reports;
mod session;
mod upload;

pub use bytes::{XetBytes, xet_bytes_data, xet_bytes_free, xet_bytes_len, xet_test_make_bytes};
pub use error::{XetError, XetStatus, xet_error_code, xet_error_free, xet_error_message, xet_test_make_auth_error};
pub use file_info::{
    XetFileInfo, XetSha256Policy, xet_file_info_free, xet_file_info_new, xet_file_info_new_with_sha256,
};
pub use op::{
    XetOp, XetPollState, xet_op_free, xet_op_poll, xet_op_take_bytes, xet_op_take_chunk, xet_op_take_error,
    xet_op_take_void, xet_test_make_error_op, xet_test_make_void_op,
};
pub use reports::{XetDedupMetrics, XetProgress};
pub use session::{XetAuthConfig, XetHeader, XetSession, xet_init_logging, xet_session_free, xet_session_new};
pub use upload::{
    XetFileMetadataHandle, XetFileUpload, XetUploadCommit, xet_file_metadata_file_size, xet_file_metadata_free,
    xet_file_metadata_hash, xet_file_metadata_sha256, xet_file_metadata_tracking_name, xet_file_upload_finalize_start,
    xet_file_upload_free, xet_upload_commit_abort, xet_upload_commit_commit_start, xet_upload_commit_free,
    xet_upload_commit_progress, xet_upload_commit_upload_bytes, xet_upload_commit_upload_from_path,
};

/// Returns the xet_capi version as a static NUL-terminated C string.
#[unsafe(no_mangle)]
pub extern "C" fn xet_version() -> *const std::os::raw::c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const _
}
