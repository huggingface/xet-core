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
//! transfers to finish and push metadata to the CAS server.
//!
//! Per-file results are available via [`UploadFileHandle::finish`] or
//! [`UploadStreamHandle::finish`] at any time — even before `commit()`
//! completes.  Each result is a [`FileMetadata`] containing [`XetFileInfo`],
//! [`DeduplicationMetrics`], and an optional tracking name.
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
//! ## Streaming downloads
//!
//! Use [`XetSession::download_stream`] / [`XetSession::download_stream_blocking`]
//! for ordered byte streaming, or [`XetSession::download_unordered_stream`] /
//! [`XetSession::download_unordered_stream_blocking`] for unordered chunks.
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
//! [`commit`](UploadCommit::commit) returns a [`CommitReport`] containing
//! aggregate dedup metrics, progress, and per-file [`FileMetadata`].
//! [`finish`](FileDownloadGroup::finish) returns
//! `HashMap<`[`UniqueID`]`, `[`DownloadResult`]`>` keyed by task ID, so a single
//! failed download does not discard all others.
//!
//! # Quick start — sync API
//!
//! ```rust,no_run
//! use xet::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};
//!
//! let session = XetSessionBuilder::new()
//!     .with_endpoint("https://cas.example.com".into())
//!     .with_token_info("my-token".into(), 1_700_000_000)
//!     .build()?;
//!
//! // Upload — use the _blocking factory and _blocking methods
//! let commit = session.new_upload_commit_blocking()?;
//! let handle = commit.upload_from_path_blocking("file.bin".into(), Sha256Policy::Compute)?;
//! let meta = handle.finish_blocking()?;
//! let report = commit.commit_blocking()?;
//! // report.dedup_metrics, report.progress, report.files
//!
//! // Download — use the _blocking factory and finish_blocking
//! let group = session.new_file_download_group_blocking()?;
//! let info = meta.xet_info.clone();
//! let dl_handle = group.download_file_to_path_blocking(info, "out/file.bin".into())?;
//! let finish_results = group.finish_blocking()?;
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
//! // Upload — async methods
//! let commit = session.new_upload_commit().await?;
//! let handle = commit.upload_from_path("file.bin".into(), Sha256Policy::Compute).await?;
//! let meta = handle.finish().await?;
//! let report = commit.commit().await?;
//! // report.dedup_metrics, report.progress, report.files
//!
//! // Download — async methods
//! let group = session.new_file_download_group().await?;
//! let info = meta.xet_info.clone();
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into()).await?;
//! let finish_results = group.finish().await?;
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//! # Ok(())
//! # }
//! ```

mod common;
mod download_streams;
mod errors;
mod file_download_group;
mod session;
mod task_runtime;
mod tasks;
mod upload_commit;
mod upload_file_handle;
mod upload_stream_handle;

pub use download_streams::{XetDownloadStream, XetUnorderedDownloadStream};
pub use errors::SessionError;
pub use file_download_group::{DownloadResult, DownloadTaskHandle, DownloadedFile, FileDownloadGroup};
pub use session::{XetSession, XetSessionBuilder};
pub use tasks::{TaskHandle, TaskStatus, UploadResult, UploadTaskHandle};
pub use upload_commit::{CommitReport, FileMetadata, UploadCommit};
pub use upload_file_handle::UploadFileHandle;
pub use upload_stream_handle::UploadStreamHandle;
pub use xet_data::deduplication::DeduplicationMetrics;
pub use xet_data::processing::{Sha256Policy, XetFileInfo};
pub use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID};
pub use xet_runtime::config::XetConfig;
