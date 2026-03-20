//! Session-based file upload and download API for XetHub / HuggingFace Hub.
//!
//! This crate exposes a three-level hierarchy that maps naturally onto batch
//! file operations:
//!
//! ```text
//! XetSession          — holds runtime context and authentication credentials
//!   ├── UploadCommit  — groups related uploads; finalised with commit()
//!   └── DownloadGroup — groups related downloads; finalised with finish()
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
//! Create a [`DownloadGroup`] with [`XetSession::new_download_group`] (async)
//! or [`XetSession::new_download_group_blocking`] (sync), queue files with
//! [`download_file_to_path`](DownloadGroup::download_file_to_path) /
//! [`download_file_to_path_blocking`](DownloadGroup::download_file_to_path_blocking),
//! then call [`finish`](DownloadGroup::finish) (async) or
//! [`finish_blocking`](DownloadGroup::finish_blocking) (sync) to wait for all
//! transfers to complete and receive a `HashMap<`[`UniqueID`]`, `[`DownloadResult`]`>`
//! keyed by task ID.
//!
//! `DownloadResult` = `Arc<Result<`[`DownloadedFile`]`, `[`XetError`]`>>`.
//! Per-task results can also be read from the returned [`DownloadTaskHandle`]
//! via [`result`](DownloadTaskHandle::result) after `finish()` returns.
//!
//! ## Progress tracking
//!
//! Both [`UploadCommit`] and [`DownloadGroup`] expose `get_progress()`,
//! which returns a [`GroupProgressReport`] without acquiring a lock on the
//! calling thread (useful for Python bindings that must release the GIL).
//! Poll it from a background thread/task while the main thread/task blocks
//! in `commit()` / `finish()`.
//!
//! ## Error handling
//!
//! All public methods return `Result<_, `[`XetError`]`>`.
//! [`commit`](UploadCommit::commit) returns a [`CommitReport`] containing
//! aggregate dedup metrics, progress, and per-file [`FileMetadata`].
//! [`finish`](DownloadGroup::finish) returns
//! `HashMap<`[`UniqueID`]`, `[`DownloadResult`]`>` keyed by task ID, so a single
//! failed download does not discard all others.
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
//! let meta = handle.finish_blocking()?;
//! let report = commit.commit_blocking()?;
//! // report.dedup_metrics, report.progress, report.files
//!
//! // 3. Download — use the _blocking factory and finish_blocking
//! let group = session.new_download_group_blocking()?;
//! let info = meta.xet_info.clone();
//! let dl_handle = group.download_file_to_path_blocking(info, "out/file.bin".into())?;
//! let finish_results = group.finish_blocking()?;
//! // DownloadResult = Arc<Result<DownloadedFile, XetError>>
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//!
//! # Ok::<(), xet::XetError>(())
//! ```
//!
//! # Quick start — async API
//!
//! ```rust,no_run
//! use xet::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};
//!
//! # async fn example() -> Result<(), xet::XetError> {
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
//! let meta = handle.finish().await?;
//! let report = commit.commit().await?;
//! // report.dedup_metrics, report.progress, report.files
//!
//! // 3. Download — use the async factory and async finish
//! let group = session.new_download_group().await?;
//! let info = meta.xet_info.clone();
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into()).await?;
//! let finish_results = group.finish().await?;
//! // DownloadResult = Arc<Result<DownloadedFile, XetError>>
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//! # Ok(())
//! # }
//! ```

mod common;
mod download_group;
mod session;
mod tasks;
mod upload_commit;
mod upload_file_handle;
mod upload_stream_handle;

pub use download_group::{DownloadGroup, DownloadResult, DownloadedFile};
pub use session::{XetSession, XetSessionBuilder};
pub use tasks::{DownloadTaskHandle, TaskHandle, TaskStatus};
pub use upload_commit::{CommitReport, FileMetadata, UploadCommit};
pub use upload_file_handle::UploadFileHandle;
pub use upload_stream_handle::UploadStreamHandle;
pub use xet_data::deduplication::DeduplicationMetrics;
pub use xet_data::processing::{Sha256Policy, XetFileInfo};
pub use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID};
pub use xet_runtime::config::XetConfig;
