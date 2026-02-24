//! # xet-session
//!
//! Session-based file upload/download API for XetHub.
//!
//! This crate provides a hierarchical API for managing file uploads and downloads:
//! - `XetSession` - Manages runtime and configuration
//! - `UploadCommit` - Groups related uploads (like git staging + commit)
//! - `DownloadGroup` - Groups related downloads
//!
//! ## Example
//!
//! ```rust,no_run
//! use std::path::PathBuf;
//! use xet_session::XetSession;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let session = XetSession::new(None, None, None, "my-app/1.0".to_string())?;
//!
//!     // Upload commit
//!     let upload_commit = session.new_upload_commit()?;
//!     upload_commit.upload_from_path(PathBuf::from("file.bin"))?;
//!     let metadata = upload_commit.commit()?;
//!
//!     // Download group
//!     let download_group = session.new_download_group()?;
//!     download_group.download_file(
//!         metadata[0].hash.clone(),
//!         metadata[0].file_size,
//!         PathBuf::from("dest.bin"),
//!     )?;
//!     let _results = download_group.finish()?;
//!
//!     Ok(())
//! }
//! ```

mod session;
mod upload_commit;
mod download_group;
mod progress;
mod errors;

// C FFI bindings (enabled with "ffi" feature)
#[cfg(feature = "ffi")]
pub mod ffi;

pub use session::XetSession;
pub use upload_commit::{UploadCommit, UploadProgress, FileMetadata};
pub use download_group::{DownloadGroup, DownloadProgress, DownloadResult};
pub use progress::{AtomicProgress, TaskStatus};
pub use errors::SessionError;

// Re-export XetConfig for convenience
pub use xet_config::XetConfig;
