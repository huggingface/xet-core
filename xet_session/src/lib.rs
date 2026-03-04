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
//! receive a [`Vec`] of [`FileMetadata`].
//!
//! ## Downloads
//!
//! Create a [`DownloadGroup`] with [`XetSession::new_download_group`], queue
//! files with [`download_file_to_path`](DownloadGroup::download_file_to_path),
//! then call [`finish`](DownloadGroup::finish) to wait for all transfers.
//!
//! ## Progress tracking
//!
//! Both [`UploadCommit`] and [`DownloadGroup`] expose
//! [`get_progress`](UploadCommit::get_progress), which returns a
//! [`ProgressSnapshot`](progress::ProgressSnapshot) without acquiring a lock
//! on the calling thread (useful for Python bindings that must release the
//! GIL).  Poll it from a background thread while the main thread blocks in
//! `commit()` / `finish()`.
//!
//! ## Error handling
//!
//! All public methods return `Result<_, `[`SessionError`]`>`.
//! [`commit`](UploadCommit::commit) and [`finish`](DownloadGroup::finish)
//! return `Vec<Result<_, SessionError>>` so a single failed file does not
//! discard the results of all others.
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
//! commit.upload_from_path("file.bin".into())?;
//! let metadata = commit.commit()?;
//!
//! // 3. Download
//! let group = session.new_download_group()?;
//! let m = metadata[0].as_ref().unwrap();
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: m.file_size,
//! };
//! group.download_file_to_path(info, "out/file.bin".into())?;
//! group.finish()?;
//! # Ok::<(), xet_session::SessionError>(())
//! ```

mod common;
mod download_group;
mod errors;
mod progress;
mod session;
mod upload_commit;

pub use data::XetFileInfo;
pub use download_group::{DownloadGroup, DownloadProgress, DownloadResult};
pub use errors::SessionError;
pub use progress::{TaskHandle, TaskStatus};
pub use session::{XetSession, XetSessionBuilder};
pub use upload_commit::{FileMetadata, UploadCommit};
// Re-export XetConfig for convenience
pub use xet_config::XetConfig;
