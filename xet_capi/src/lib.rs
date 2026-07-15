//! C ABI bindings for the Hugging Face Xet client (`hf-xet`).
//!
//! All types are opaque handles freed with their `xet_*_free` function.
//! Transfer functions block the calling thread until the operation completes.
//! Progress can be polled concurrently from another thread via the
//! `xet_*_progress` functions. No callbacks cross the ABI in either direction.

mod bytes;
mod download_file;
mod download_stream;
mod error;
mod file_info;
mod handle;
mod reports;
mod session;
mod upload;
mod upload_stream;

pub use bytes::{XetBytes, xet_bytes_data, xet_bytes_free, xet_bytes_len, xet_test_make_bytes};
pub use download_file::{
    XetFileDownload, XetFileDownloadGroup, xet_file_download_free, xet_file_download_group_abort,
    xet_file_download_group_download_to_path, xet_file_download_group_finish, xet_file_download_group_free,
    xet_file_download_group_progress, xet_file_download_task_id,
};
pub use download_stream::{
    XetDownloadStream, XetDownloadStreamGroup, xet_download_stream_cancel, xet_download_stream_free,
    xet_download_stream_group_download_stream, xet_download_stream_group_download_unordered_stream,
    xet_download_stream_group_free, xet_download_stream_next, xet_download_stream_progress,
    xet_download_stream_task_id,
};
pub use error::{XetError, XetStatus, xet_error_code, xet_error_free, xet_error_message, xet_test_make_auth_error};
pub use file_info::{
    XetFileInfo, XetSha256Policy, xet_file_info_free, xet_file_info_new, xet_file_info_new_with_sha256,
};
pub use reports::{
    XetCommitReportHandle, XetDedupMetrics, XetDownloadGroupReportHandle, XetProgress, xet_commit_report_dedup,
    xet_commit_report_file_at, xet_commit_report_file_count, xet_commit_report_free, xet_commit_report_progress,
    xet_download_group_report_at, xet_download_group_report_count, xet_download_group_report_free,
};
pub use session::{
    XetAuthConfig, XetHeader, XetSession, xet_session_free, xet_session_new, xet_session_new_download_stream_group,
    xet_session_new_file_download_group, xet_session_new_upload_commit,
};
pub use upload::{
    XetFileMetadataHandle, XetFileUpload, XetUploadCommit, xet_file_metadata_file_size, xet_file_metadata_free,
    xet_file_metadata_hash, xet_file_metadata_sha256, xet_file_metadata_tracking_name, xet_file_upload_finalize,
    xet_file_upload_free, xet_upload_commit_abort, xet_upload_commit_commit, xet_upload_commit_free,
    xet_upload_commit_progress, xet_upload_commit_upload_bytes, xet_upload_commit_upload_from_path,
    xet_upload_commit_upload_stream,
};
pub use upload_stream::{XetStreamUpload, xet_stream_upload_finish, xet_stream_upload_free, xet_stream_upload_write};

/// Returns the xet_capi version as a static NUL-terminated C string.
#[unsafe(no_mangle)]
pub extern "C" fn xet_version() -> *const std::os::raw::c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const _
}
