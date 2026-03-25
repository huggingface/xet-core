//! Session-based file upload and download API for XetHub / HuggingFace Hub.
//!
//! This crate exposes a three-level hierarchy that maps naturally onto batch
//! file operations:
//!
//! ```text
//! XetSession                 — holds runtime context and shared HTTP settings
//!   ├── UploadCommitBuilder  — configures per-commit auth; build() → UploadCommit
//!   ├── FileDownloadGroupBuilder — configures per-group auth; build() → FileDownloadGroup
//!   └── DownloadStreamGroupBuilder — configures per-group auth; build() → DownloadStreamGroup
//! ```
//!
//! Each [`XetSession`] holds its own runtime context and configuration, so
//! multiple sessions with different endpoints can coexist in the same process.
//! Auth tokens are per-commit/group so uploads and downloads can use different
//! access levels from the same session.  Cloning a session, commit, or group is
//! cheap — all clones share the same underlying state via `Arc`.
//!
//! ## Uploads
//!
//! Call [`XetSession::new_upload_commit`] to obtain an [`UploadCommitBuilder`].
//! Configure auth with [`with_token_info`](UploadCommitBuilder::with_token_info) and
//! [`with_token_refresh_url`](UploadCommitBuilder::with_token_refresh_url), then call
//! [`build`](UploadCommitBuilder::build) (async) or
//! [`build_blocking`](UploadCommitBuilder::build_blocking) (sync).
//! Queue files with [`upload_from_path`](UploadCommit::upload_from_path) /
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
//! ## File Downloads
//!
//! Call [`XetSession::new_file_download_group`] to obtain a [`FileDownloadGroupBuilder`].
//! Configure auth similarly, then call [`build`](FileDownloadGroupBuilder::build) (async) or
//! [`build_blocking`](FileDownloadGroupBuilder::build_blocking) (sync).
//! Queue files with [`download_file_to_path`](FileDownloadGroup::download_file_to_path) /
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
//! ## Streaming Downloads
//!
//! Call [`XetSession::new_download_stream_group`] to obtain a [`DownloadStreamGroupBuilder`].
//! Configure auth similarly, then call [`build`](DownloadStreamGroupBuilder::build) (async) or
//! [`build_blocking`](DownloadStreamGroupBuilder::build_blocking) (sync).
//! Create individual streams with
//! [`download_stream`](DownloadStreamGroup::download_stream) /
//! [`download_stream_blocking`](DownloadStreamGroup::download_stream_blocking) for
//! ordered byte delivery, or
//! [`download_unordered_stream`](DownloadStreamGroup::download_unordered_stream) /
//! [`download_unordered_stream_blocking`](DownloadStreamGroup::download_unordered_stream_blocking)
//! for out-of-order `(offset, bytes)` chunks.  Multiple streams can be active
//! concurrently from the same group; they share a single CAS connection pool and
//! auth token.
//!
//! Each stream exposes [`get_progress`](XetDownloadStream::get_progress) (returning
//! [`ItemProgressReport`]) and can be explicitly cancelled via
//! [`cancel`](XetDownloadStream::cancel).
//!
//! ## Progress tracking
//!
//! [`UploadCommit`] and [`FileDownloadGroup`] expose `get_progress()`, which
//! returns a [`GroupProgressReport`] without acquiring a lock on the calling
//! thread (useful for Python bindings that must release the GIL).
//! Poll it from a background thread/task while the main thread/task blocks
//! in `commit()` / `finish()`.
//!
//! Individual [`XetDownloadStream`] and [`XetUnorderedDownloadStream`] objects expose
//! their own [`get_progress`](XetDownloadStream::get_progress), returning an
//! [`ItemProgressReport`] with lock-free atomic reads.
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
//! let session = XetSessionBuilder::new().with_endpoint("https://cas.example.com").build()?;
//!
//! // Upload — configure token on the commit builder, then build_blocking
//! let commit = session
//!     .new_upload_commit()?
//!     .with_token_info("write-token", 1_700_000_000)
//!     .build_blocking()?;
//! let handle = commit.upload_from_path_blocking("file.bin".into(), Sha256Policy::Compute)?;
//! let results = commit.commit_blocking()?;
//! let m = results.values().next().unwrap().as_ref().as_ref().unwrap();
//!
//! // Download — configure token on the group builder, then build_blocking
//! let group = session
//!     .new_file_download_group()?
//!     .with_token_info("read-token", 1_700_000_000)
//!     .build_blocking()?;
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: Some(m.file_size),
//!     sha256: m.sha256.clone(),
//! };
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
//! // build() auto-detects: if inside a suitable tokio runtime, wraps it;
//! // otherwise creates an owned thread pool.
//! let session = XetSessionBuilder::new().with_endpoint("https://cas.example.com").build()?;
//!
//! // Upload — configure token on the commit builder, then build().await
//! let commit = session
//!     .new_upload_commit()?
//!     .with_token_info("write-token", 1_700_000_000)
//!     .build()
//!     .await?;
//! let handle = commit.upload_from_path("file.bin".into(), Sha256Policy::Compute).await?;
//! let results = commit.commit().await?;
//! let m = results.values().next().unwrap().as_ref().as_ref().unwrap();
//!
//! // Download — configure token on the group builder, then build().await
//! let group = session
//!     .new_file_download_group()?
//!     .with_token_info("read-token", 1_700_000_000)
//!     .build()
//!     .await?;
//! let info = XetFileInfo {
//!     hash: m.hash.clone(),
//!     file_size: Some(m.file_size),
//!     sha256: m.sha256.clone(),
//! };
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into()).await?;
//! let finish_results = group.finish().await?;
//! let r = finish_results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
//! # Ok(())
//! # }
//! ```

mod common;
mod download_stream_group;
mod errors;
mod file_download_group;
mod session;
mod tasks;
mod upload_commit;

pub use download_stream_group::{
    DownloadStreamGroup, DownloadStreamGroupBuilder, XetDownloadStream, XetUnorderedDownloadStream,
};
pub use errors::SessionError;
pub use file_download_group::{DownloadResult, DownloadedFile, FileDownloadGroup, FileDownloadGroupBuilder};
pub use session::{XetSession, XetSessionBuilder};
pub use tasks::{DownloadTaskHandle, TaskHandle, TaskStatus, UploadTaskHandle};
pub use upload_commit::{FileMetadata, UploadCommit, UploadCommitBuilder, UploadResult};
pub use xet_data::processing::{Sha256Policy, XetFileInfo};
pub use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID};
pub use xet_runtime::config::XetConfig;
