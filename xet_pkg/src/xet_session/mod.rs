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
//! For **sync** callers: create an [`UploadCommitSync`] with
//! [`XetSession::new_upload_commit_blocking`], queue files with
//! [`upload_from_path`](UploadCommitSync::upload_from_path) or
//! [`upload_bytes`](UploadCommitSync::upload_bytes), then call
//! [`commit`](UploadCommitSync::commit) to block until all transfers finish and
//! receive a `HashMap<Ulid, `[`UploadResult`]`>` keyed by task ID.
//!
//! For **async** callers: create an [`UploadCommit`] with
//! [`XetSession::new_upload_commit`], queue files the same way, then
//! `await` [`commit`](UploadCommit::commit).
//!
//! `UploadResult` = `Arc<Result<`[`FileMetadata`]`, `[`SessionError`]`>>`.
//! Per-task results can also be read from the returned [`UploadTaskHandle`]
//! via [`result`](UploadTaskHandle::result) after `commit()` returns.
//!
//! ## Downloads
//!
//! For **sync** callers: create a [`DownloadGroupSync`] with
//! [`XetSession::new_download_group_blocking`], queue files with
//! [`download_file_to_path`](DownloadGroupSync::download_file_to_path), then
//! call [`finish`](DownloadGroupSync::finish) to block until all transfers
//! complete and receive a `HashMap<Ulid, `[`DownloadResult`]`>` keyed by task ID.
//!
//! For **async** callers: create a [`DownloadGroup`] with
//! [`XetSession::new_download_group`], queue files the same way, then
//! `await` [`finish`](DownloadGroup::finish).
//!
//! `DownloadResult` = `Arc<Result<`[`DownloadedFile`]`, `[`SessionError`]`>>`.
//! Per-task results can also be read from the returned [`DownloadTaskHandle`]
//! via [`result`](DownloadTaskHandle::result) after `finish()` returns.
//!
//! ## Progress tracking
//!
//! All four types ([`UploadCommit`], [`UploadCommitSync`], [`DownloadGroup`],
//! [`DownloadGroupSync`]) expose `get_progress()`, which returns a
//! [`ProgressSnapshot`] without acquiring a lock on the calling thread
//! (useful for Python bindings that must release the GIL).  Poll it from a
//! background thread/task while the main thread/task blocks in
//! `commit()` / `finish()`.
//!
//! ## Error handling
//!
//! All public methods return `Result<_, `[`SessionError`]`>`.
//! [`commit`](UploadCommit::commit) returns `HashMap<Ulid, `[`UploadResult`]`>`
//! keyed by task ID, and [`finish`](DownloadGroup::finish) returns
//! `HashMap<Ulid, `[`DownloadResult`]`>` keyed by task ID, so a single failed
//! file does not discard all others.
//!
//! # Quick start — sync API
//!
//! ```rust,no_run
//! use xet::xet_session::{XetFileInfo, XetSessionBuilder};
//! use xet_data::processing::Sha256Policy;
//!
//! // 1. Build a session (sync or async context)
//! let session = XetSessionBuilder::new()
//!     .with_endpoint("https://cas.example.com".into())
//!     .with_token_info("my-token".into(), 1_700_000_000)
//!     .build()?;
//!
//! // 2. Upload — use the _blocking factory; returns UploadCommitSync
//! let commit = session.new_upload_commit_blocking()?;
//! let handle = commit.upload_from_path("file.bin".into(), Sha256Policy::Compute)?;
//! // UploadResult = Arc<Result<FileMetadata, SessionError>>
//! let results = commit.commit()?;
//! let m = results.values().next().unwrap().as_ref().as_ref().unwrap();
//!
//! // 3. Download — use the _blocking factory; returns DownloadGroupSync
//! let group = session.new_download_group_blocking()?;
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: m.file_size,
//! };
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into())?;
//! let finish_results = group.finish()?;
//! // DownloadResult = Arc<Result<DownloadedFile, SessionError>>
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//!
//! # Ok::<(), xet::xet_session::SessionError>(())
//! ```
//!
//! # Quick start — async API
//!
//! ```rust,no_run
//! use xet::xet_session::{XetFileInfo, XetSessionBuilder};
//! use xet_data::processing::Sha256Policy;
//!
//! # async fn example() -> Result<(), xet::xet_session::SessionError> {
//! // 1. Build a session (sync or async context)
//! let session = XetSessionBuilder::new()
//!     .with_endpoint("https://cas.example.com".into())
//!     .with_token_info("my-token".into(), 1_700_000_000)
//!     .build()?;
//!
//! // 2. Upload — use the async factory; returns UploadCommit
//! let commit = session.new_upload_commit().await?;
//! let handle = commit.upload_from_path("file.bin".into(), Sha256Policy::Compute).await?;
//! // UploadResult = Arc<Result<FileMetadata, SessionError>>
//! let results = commit.commit().await?;
//! let m = results.values().next().unwrap().as_ref().as_ref().unwrap();
//!
//! // 3. Download — use the async factory; returns DownloadGroup
//! let group = session.new_download_group().await?;
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: m.file_size,
//! };
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into())?;
//! let finish_results = group.finish().await?;
//! // DownloadResult = Arc<Result<DownloadedFile, SessionError>>
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//! # Ok(())
//! # }
//! ```

mod common;
mod download_group;
mod errors;
mod progress;
mod session;
pub mod sync;
mod upload_commit;

pub use download_group::{DownloadGroup, DownloadResult, DownloadedFile};
pub use errors::SessionError;
pub use progress::{
    DownloadTaskHandle, FileProgress, ProgressSnapshot, TaskHandle, TaskStatus, TotalProgressSnapshot, UploadTaskHandle,
};
pub use session::{XetSession, XetSessionBuilder};
pub use sync::{DownloadGroupSync, UploadCommitSync};
pub use upload_commit::{FileMetadata, UploadCommit, UploadResult};
pub use xet_data::processing::XetFileInfo;
// Re-export XetConfig for convenience
pub use xet_runtime::config::XetConfig;
