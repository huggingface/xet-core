//! Session-based file upload and download API for XetHub / HuggingFace Hub.
//!
//! This crate exposes a three-level hierarchy that maps naturally onto batch
//! file operations:
//!
//! ```text
//! XetSession          — holds runtime context and authentication credentials
//!   ├── UploadCommit  — groups related uploads; finalised with commit()
//!   └── FileDownloadGroup — groups related file downloads; finalised with finish()
//! ```
//!
//! Each [`XetSession`] holds its own runtime context and configuration, so
//! multiple sessions with different endpoints or credentials can coexist in
//! the same process.  Cloning a session, commit, or group is cheap — all
//! clones share the same underlying state via `Arc`.
//!
//! ## Uploads
//!
//! Create an [`UploadCommit`] with [`XetSession::new_upload_commit`] (async)
//! or [`XetSession::new_upload_commit_blocking`] (sync), queue files with
//! [`upload_from_path`](UploadCommit::upload_from_path) /
//! [`upload_from_path_blocking`](UploadCommit::upload_from_path_blocking) or
//! [`upload_bytes`](UploadCommit::upload_bytes) /
//! [`upload_bytes_blocking`](UploadCommit::upload_bytes_blocking), then call
//! [`commit`](UploadCommit::commit) or
//! [`commit_blocking`](UploadCommit::commit_blocking) to wait for all
//! transfers to finish and receive a `HashMap<`[`UniqueID`]`, `[`UploadResult`]`>`
//! keyed by task ID.
//!
//! `UploadResult` = `Arc<Result<`[`FileMetadata`]`, `[`SessionError`]`>>`.
//! Per-task results can also be read from the returned [`UploadTaskHandle`]
//! via [`result`](UploadTaskHandle::result) after `commit()` returns.
//!
//! ## Downloads
//!
//! Create a [`FileDownloadGroup`] with [`XetSession::new_file_download_group`] (async)
//! or [`XetSession::new_file_download_group_blocking`] (sync), queue files with
//! [`download_file_to_path`](FileDownloadGroup::download_file_to_path) /
//! [`download_file_to_path_blocking`](FileDownloadGroup::download_file_to_path_blocking),
//! then call [`finish`](FileDownloadGroup::finish) (async) or
//! [`finish_blocking`](FileDownloadGroup::finish_blocking) (sync) to wait for all
//! transfers to complete and receive a `HashMap<`[`UniqueID`]`, `[`DownloadResult`]`>`
//! keyed by task ID.
//!
//! `DownloadResult` = `Arc<Result<`[`DownloadedFile`]`, `[`SessionError`]`>>`.
//! Per-task results can also be read from the returned [`DownloadTaskHandle`]
//! via [`result`](DownloadTaskHandle::result) after `finish()` returns.
//!
//! ## Progress tracking
//!
//! Both [`UploadCommit`] and [`FileDownloadGroup`] expose `get_progress()`,
//! which returns a [`GroupProgressReport`] without acquiring a lock on the
//! calling thread (useful for Python bindings that must release the GIL).
//! Poll it from a background thread/task while the main thread/task blocks
//! in `commit()` / `finish()`.
//!
//! ## Error handling
//!
//! All public methods return `Result<_, `[`SessionError`]`>`.
//! [`commit`](UploadCommit::commit) returns `HashMap<`[`UniqueID`]`, `[`UploadResult`]`>`
//! keyed by task ID, and [`finish`](FileDownloadGroup::finish) returns
//! `HashMap<`[`UniqueID`]`, `[`DownloadResult`]`>` keyed by task ID, so a single failed
//! file does not discard all others.
//!
//! # Quick start — sync API
//!
//! ```rust,no_run
//! use xet::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};
//!
//! // 1. Build a session — sync (non-async) context only.
//! //    For async code call build_async().await instead.
//! let session = XetSessionBuilder::new()
//!     .with_endpoint("https://cas.example.com".into())
//!     .with_token_info("my-token".into(), 1_700_000_000)
//!     .build()?;
//!
//! // 2. Upload — use the _blocking factory and _blocking methods
//! let commit = session.new_upload_commit_blocking()?;
//! let handle = commit.upload_from_path_blocking("file.bin".into(), Sha256Policy::Compute)?;
//! // UploadResult = Arc<Result<FileMetadata, SessionError>>
//! let results = commit.commit_blocking()?;
//! let m = results.values().next().unwrap().as_ref().as_ref().unwrap();
//!
//! // 3. Download — use the _blocking factory and finish_blocking
//! let group = session.new_file_download_group_blocking()?;
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: Some(m.file_size),
//!     sha256: m.sha256.clone(),
//! };
//! let dl_handle = group.download_file_to_path_blocking(info, "out/file.bin".into())?;
//! let finish_results = group.finish_blocking()?;
//! // DownloadResult = Arc<Result<DownloadedFile, SessionError>>
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//!
//! # Ok::<(), xet::xet_session::SessionError>(())
//! ```
//!
//! # Quick start — async API
//!
//! ```rust,no_run
//! use xet::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};
//!
//! # async fn example() -> Result<(), xet::xet_session::SessionError> {
//! // 1. Build a session. build_async() auto-detects the executor:
//! //    - tokio (multi-thread): wraps the caller's handle, no second thread pool.
//! //    - non-tokio (smol, async-std, etc.): creates an owned thread pool.
//! let session = XetSessionBuilder::new()
//!     .with_endpoint("https://cas.example.com".into())
//!     .with_token_info("my-token".into(), 1_700_000_000)
//!     .build_async()
//!     .await?;
//!
//! // 2. Upload — use the async factory and async methods
//! let commit = session.new_upload_commit().await?;
//! let handle = commit.upload_from_path("file.bin".into(), Sha256Policy::Compute).await?;
//! // UploadResult = Arc<Result<FileMetadata, SessionError>>
//! let results = commit.commit().await?;
//! let m = results.values().next().unwrap().as_ref().as_ref().unwrap();
//!
//! // 3. Download — use the async factory and async finish
//! let group = session.new_file_download_group().await?;
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: Some(m.file_size),
//!     sha256: m.sha256.clone(),
//! };
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into()).await?;
//! let finish_results = group.finish().await?;
//! // DownloadResult = Arc<Result<DownloadedFile, SessionError>>
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//! # Ok(())
//! # }
//! ```

mod common;
mod download_streams;
mod errors;
mod file_download_group;
mod session;
mod tasks;
mod upload_commit;

pub use download_streams::{XetDownloadStream, XetUnorderedDownloadStream};
pub use errors::SessionError;
pub use file_download_group::{DownloadResult, DownloadedFile, FileDownloadGroup};
pub use session::{XetSession, XetSessionBuilder};
pub use tasks::{DownloadTaskHandle, TaskHandle, TaskStatus, UploadTaskHandle};
pub use upload_commit::{FileMetadata, UploadCommit, UploadResult};
pub use xet_data::processing::{Sha256Policy, XetFileInfo};
pub use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID};
pub use xet_runtime::config::XetConfig;
