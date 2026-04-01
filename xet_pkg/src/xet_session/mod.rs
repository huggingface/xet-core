//! Session-based file upload and download API for XetHub / HuggingFace Hub.
//!
//! This crate exposes a three-level hierarchy that maps naturally onto batch
//! file operations:
//!
//! ```text
//! XetSession                 — holds runtime context and shared HTTP settings
//!   ├── UploadCommitBuilder  — configures per-commit auth; build() → XetUploadCommit
//!   ├── FileDownloadGroupBuilder — configures per-group auth; build() → XetDownloadGroup
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
//! Queue files with [`upload_from_path`](XetUploadCommit::upload_from_path) /
//! [`upload_from_path_blocking`](XetUploadCommit::upload_from_path_blocking) or
//! [`upload_bytes`](XetUploadCommit::upload_bytes) /
//! [`upload_bytes_blocking`](XetUploadCommit::upload_bytes_blocking), then call
//! [`commit`](XetUploadCommit::commit) or
//! [`commit_blocking`](XetUploadCommit::commit_blocking) to wait for all
//! transfers to finish and receive a [`XetCommitReport`].
//!
//! Per-file results are available via [`XetFileUpload::finalize_ingestion`] or
//! [`XetStreamUpload::finish`] at any time — even before `commit()`
//! completes.  Each result is a [`XetFileMetadata`] containing [`XetFileInfo`],
//! [`DeduplicationMetrics`], and an optional tracking name.
//!
//! ## File Downloads
//!
//! Call [`XetSession::new_file_download_group`] to obtain a [`FileDownloadGroupBuilder`].
//! Configure auth similarly, then call [`build`](FileDownloadGroupBuilder::build) (async) or
//! [`build_blocking`](FileDownloadGroupBuilder::build_blocking) (sync).
//! Queue files with [`download_file_to_path`](XetFileDownloadGroup::download_file_to_path) /
//! [`download_file_to_path_blocking`](XetFileDownloadGroup::download_file_to_path_blocking),
//! then call [`finish`](XetFileDownloadGroup::finish) (async) or
//! [`finish_blocking`](XetFileDownloadGroup::finish_blocking) (sync) to wait for all
//! transfers to complete and receive an [`XetDownloadGroupReport`] containing
//! per-file [`XetDownloadReport`] entries keyed by [`UniqueID`].
//!
//! ## Streaming Downloads
//!
//! Call [`XetSession::new_download_stream_group`] to obtain a [`DownloadStreamGroupBuilder`].
//! Configure auth similarly, then call [`build`](DownloadStreamGroupBuilder::build) (async) or
//! [`build_blocking`](DownloadStreamGroupBuilder::build_blocking) (sync).
//! Create individual streams with
//! [`download_stream`](XetDownloadStreamGroup::download_stream) /
//! [`download_stream_blocking`](XetDownloadStreamGroup::download_stream_blocking) for
//! ordered byte delivery, or
//! [`download_unordered_stream`](XetDownloadStreamGroup::download_unordered_stream) /
//! [`download_unordered_stream_blocking`](XetDownloadStreamGroup::download_unordered_stream_blocking)
//! for out-of-order `(offset, bytes)` chunks.  Multiple streams can be active
//! concurrently from the same group; they share a single CAS connection pool and
//! auth token.
//!
//! Each stream exposes [`progress`](XetDownloadStream::progress) (returning
//! [`ItemProgressReport`]) and can be explicitly cancelled via
//! [`cancel`](XetDownloadStream::cancel).
//!
//! ## Progress tracking
//!
//! Both [`XetUploadCommit`] and [`XetFileDownloadGroup`] expose `progress()`,
//! which returns a [`GroupProgressReport`] without acquiring a lock on the
//! calling thread (useful for Python bindings that must release the GIL).
//! Poll it from a background thread/task while the main thread/task blocks
//! in `commit()` / `finish()`.
//!
//! Individual [`XetDownloadStream`] and [`XetUnorderedDownloadStream`] objects expose
//! their own [`progress`](XetDownloadStream::progress), returning an
//! [`ItemProgressReport`] with lock-free atomic reads.
//!
//! ## Error handling
//!
//! Session-level factory methods and upload/file-download operations return
//! `Result<_, `[`SessionError`]`>`.
//! Streaming operations — [`DownloadStreamGroupBuilder::build`],
//! [`XetDownloadStreamGroup`] methods, [`XetDownloadStream`] methods, and
//! [`XetUnorderedDownloadStream`] methods — return `Result<_, XetError>`.
//! [`commit`](XetUploadCommit::commit) returns a [`XetCommitReport`] containing
//! aggregate dedup metrics, progress, and per-file [`XetFileMetadata`].
//! [`finish`](XetFileDownloadGroup::finish) returns
//! [`XetDownloadGroupReport`] keyed by task ID. If any download
//! fails, the error is propagated immediately.
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
//! let meta = handle.finalize_ingestion_blocking()?;
//! let report = commit.commit_blocking()?;
//! // report.dedup_metrics, report.progress, report.files
//!
//! // Download — configure token on the group builder, then build_blocking
//! let group = session
//!     .new_file_download_group()?
//!     .with_token_info("read-token", 1_700_000_000)
//!     .build_blocking()?;
//! let info = meta.xet_info.clone();
//! let dl_handle = group.download_file_to_path_blocking(info, "out/file.bin".into())?;
//! let finish_report = group.finish_blocking()?;
//! let r = finish_report.downloads.get(&dl_handle.task_id()).unwrap();
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
//! let meta = handle.finalize_ingestion().await?;
//! let report = commit.commit().await?;
//! // report.dedup_metrics, report.progress, report.files
//!
//! // Download — configure token on the group builder, then build().await
//! let group = session
//!     .new_file_download_group()?
//!     .with_token_info("read-token", 1_700_000_000)
//!     .build()
//!     .await?;
//! let info = meta.xet_info.clone();
//! let dl_handle = group.download_file_to_path(info, "out/file.bin".into()).await?;
//! let finish_report = group.finish().await?;
//! let r = finish_report.downloads.get(&dl_handle.task_id()).unwrap();
//! # Ok(())
//! # }
//! ```

mod common;
mod download_stream_group;
mod download_stream_handle;
mod errors;
mod file_download_group;
mod file_download_handle;
mod session;
mod task_runtime;
mod upload_commit;
mod upload_file_handle;
mod upload_stream_handle;

pub use download_stream_group::{DownloadStreamGroupBuilder, XetDownloadStreamGroup};
pub use download_stream_handle::{XetDownloadStream, XetUnorderedDownloadStream};
pub use errors::SessionError;
pub use file_download_group::{FileDownloadGroupBuilder, XetDownloadGroupReport, XetFileDownloadGroup};
pub use file_download_handle::{XetDownloadReport, XetFileDownload};
pub use session::{XetSession, XetSessionBuilder};
pub use task_runtime::XetTaskState;
pub use upload_commit::{UploadCommitBuilder, XetCommitReport, XetFileMetadata, XetUploadCommit};
pub use upload_file_handle::XetFileUpload;
pub use upload_stream_handle::XetStreamUpload;
pub use xet_data::deduplication::DeduplicationMetrics;
pub use xet_data::processing::{Sha256Policy, XetFileInfo};
pub use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID};
pub use xet_runtime::config::XetConfig;
