//! Session-based file upload and download API for XetHub / HuggingFace Hub.
//!
//! This crate exposes a three-level hierarchy that maps naturally onto batch
//! file operations:
//!
//! ```text
//! XetSession          — owns the tokio runtime and authentication credentials
//!   ├── UploadCommit  — groups related uploads; finalised with commit()
//!   └── DownloadGroup — groups related downloads; finalised with finish()
//! ```
//!
//! Each [`XetSession`] owns its own tokio runtime and configuration, so
//! multiple sessions with different endpoints or credentials can coexist in
//! the same process.  Cloning a session, commit, or group is cheap — all
//! clones share the same underlying state via `Arc`.
//!
//! ## Uploads
//!
//! Create an [`UploadCommit`] with [`XetSession::new_upload_commit`], queue
//! files with [`upload_from_path`](UploadCommit::upload_from_path) or
//! [`upload_bytes`](UploadCommit::upload_bytes), then call
//! [`commit`](UploadCommit::commit) to wait for all transfers to finish and
//! receive a `HashMap<Ulid, `[`UploadResult`]`>` keyed by task ID, where each
//! value is `Arc<Result<`[`FileMetadata`]`, `[`SessionError`]`>>`.  Per-task
//! results can also be read directly from the returned [`UploadTaskHandle`]
//! via [`result`](UploadTaskHandle::result) after `commit()` returns.
//!
//! ## Downloads
//!
//! Create a [`DownloadGroup`] with [`XetSession::new_download_group`], queue
//! files with [`download_file_to_path`](DownloadGroup::download_file_to_path),
//! then call [`finish`](DownloadGroup::finish) to wait for all transfers and
//! receive a `HashMap<Ulid, `[`DownloadResult`]`>` keyed by task ID, where each
//! value is `Arc<Result<`[`DownloadedFile`]`, `[`SessionError`]`>>`.  Per-task
//! results can also be read directly from the returned [`DownloadTaskHandle`]
//! via [`result`](DownloadTaskHandle::result) after `finish()` returns.
//!
//! ## Progress tracking
//!
//! Both [`UploadCommit`] and [`DownloadGroup`] expose
//! [`get_progress`](UploadCommit::get_progress), which returns a
//! [`ProgressSnapshot`] without acquiring a lock on the calling thread
//! (useful for Python bindings that must release the GIL).  Poll it from a
//! background thread while the main thread blocks in `commit()` / `finish()`.
//!
//! ## Error handling
//!
//! All public methods return `Result<_, `[`SessionError`]`>`.
//! [`commit`](UploadCommit::commit) returns `HashMap<Ulid, `[`UploadResult`]`>`
//! keyed by task ID, and [`finish`](DownloadGroup::finish) returns
//! `HashMap<Ulid, `[`DownloadResult`]`>` keyed by task ID, so a single failed
//! file does not discard all others.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use xet_session::{XetFileInfo, XetSessionBuilder};
//!
//! // 1. Build a session
//! let session = XetSessionBuilder::new()
//!     .with_endpoint("https://cas.example.com".into())
//!     .with_token_info("my-token".into(), 1_700_000_000)
//!     .build()?;
//!
//! // 2. Upload
//! let commit = session.new_upload_commit()?;
//! let handle = commit.upload_from_path("file.bin".into())?;
//! commit.commit()?;
//! // Access result directly from the handle (populated by commit())
//! let m = handle.result().unwrap();
//! let m = m.as_ref().as_ref().unwrap();
//!
//! // 3. Download
//! let group = session.new_download_group()?;
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: m.file_size,
//! };
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into())?;
//! let finish_results = group.finish()?;
//! // Pattern 1: look up by task ID in the returned HashMap
//! let r1 = finish_results.get(&dl_handle.task_id()).unwrap();
//! let r1 = r1.as_ref().as_ref().unwrap();
//! // Pattern 2: read directly from the handle (populated by finish())
//! let r2 = dl_handle.result().unwrap();
//! let r2 = r2.as_ref().as_ref().unwrap();
//! # Ok::<(), xet_session::SessionError>(())
//! ```

mod common;
mod download_group;
mod errors;
mod progress;
mod session;
mod upload_commit;

pub use data::XetFileInfo;
pub use download_group::{DownloadGroup, DownloadResult, DownloadedFile};
pub use errors::SessionError;
pub use progress::{
    DownloadTaskHandle, FileProgress, ProgressSnapshot, TaskHandle, TaskStatus, TotalProgressSnapshot, UploadTaskHandle,
};
pub use session::{XetSession, XetSessionBuilder};
pub use upload_commit::{FileMetadata, UploadCommit, UploadResult};
// Re-export XetConfig for convenience
pub use xet_config::XetConfig;
