//! Session-based file upload and download API for XetHub / HuggingFace Hub.
//!
//! ## WASM availability
//!
//! On `wasm32-unknown-unknown` targets the surface is a strict subset:
//!
//! - **Async only** — no `_blocking` variants. Wasm cannot block the host
//!   thread, and the bridged `_blocking` methods are gated to non-wasm.
//! - **No filesystem entrypoints** — `upload_from_path`,
//!   `XetFileDownloadGroup`, and `XetFileDownload` are non-wasm-only.
//!   Wasm callers use `upload_bytes` / `upload_stream` and
//!   `XetDownloadStreamGroup` instead.
//! - **No external tokio handle** — `XetSessionBuilder::with_tokio_handle`
//!   and `XetSession::new_from_external_runtime` are non-wasm-only.
//!
//! The `[`...`]` doc links below resolve on every target, but the rendered
//! `_blocking` and path-based names point at items that don't exist when
//! building docs for wasm. See the per-method `#[cfg(...)]` attributes for
//! the authoritative target gate.
//!
//! This crate exposes a three-level hierarchy that maps naturally onto batch
//! file operations:
//!
//! ```text
//! XetSession                          — holds runtime context and shared HTTP settings
//!   ├── AuthGroupBuilder<XetUploadCommit>         — configures per-commit auth; build() → XetUploadCommit
//!   ├── AuthGroupBuilder<XetFileDownloadGroup>    — configures per-group auth;  build() → XetFileDownloadGroup
//!   └── AuthGroupBuilder<XetDownloadStreamGroup>  — configures per-group auth;  build() → XetDownloadStreamGroup
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
//! Call [`XetSession::new_upload_commit`] to obtain an [`AuthGroupBuilder`].
//! Configure auth with [`with_token_info`](AuthGroupBuilder::with_token_info) and
//! [`with_token_refresh_url`](AuthGroupBuilder::with_token_refresh_url), then call
//! [`build`](AuthGroupBuilder::build) (async) or
//! [`build_blocking`](AuthGroupBuilder::build_blocking) (sync).
//!
//! There are three ways to queue data for upload:
//!
//! - **From a file path** — [`upload_from_path`](XetUploadCommit::upload_from_path) /
//!   [`upload_from_path_blocking`](XetUploadCommit::upload_from_path_blocking). The file is read in a background task.
//! - **From raw bytes** — [`upload_bytes`](XetUploadCommit::upload_bytes) /
//!   [`upload_bytes_blocking`](XetUploadCommit::upload_bytes_blocking). Useful when data is already in memory.
//! - **Incrementally via a stream** — [`upload_stream`](XetUploadCommit::upload_stream) /
//!   [`upload_stream_blocking`](XetUploadCommit::upload_stream_blocking). Returns an [`XetStreamUpload`] handle; call
//!   [`write`](XetStreamUpload::write) to feed chunks, then [`finish`](XetStreamUpload::finish) to finalise. **`finish`
//!   must be called before [`commit`](XetUploadCommit::commit).** Use this when data arrives incrementally (e.g. from a
//!   network socket or a generator) and you don't want to buffer it all in memory first.
//!
//! Then call [`commit`](XetUploadCommit::commit) or
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
//! Call [`XetSession::new_file_download_group`] to obtain an [`AuthGroupBuilder`].
//! Configure auth similarly, then call [`build`](AuthGroupBuilder::build) (async) or
//! [`build_blocking`](AuthGroupBuilder::build_blocking) (sync).
//! Queue files with [`download_file_to_path`](XetFileDownloadGroup::download_file_to_path) /
//! [`download_file_to_path_blocking`](XetFileDownloadGroup::download_file_to_path_blocking),
//! then call [`finish`](XetFileDownloadGroup::finish) (async) or
//! [`finish_blocking`](XetFileDownloadGroup::finish_blocking) (sync) to wait for all
//! transfers to complete and receive an [`XetDownloadGroupReport`] containing
//! per-file [`XetDownloadReport`] entries keyed by [`UniqueID`].
//!
//! ## Streaming Downloads
//!
//! Call [`XetSession::new_download_stream_group`] to obtain an [`AuthGroupBuilder`].
//! Configure auth similarly, then call [`build`](AuthGroupBuilder::build) (async) or
//! [`build_blocking`](AuthGroupBuilder::build_blocking) (sync).
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
//! Streaming operations — [`AuthGroupBuilder::build`] (for `XetDownloadStreamGroup`),
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
//! # fn example() -> Result<(), xet::xet_session::SessionError> {
//! let session = XetSessionBuilder::new().build()?;
//!
//! // Upload — configure endpoint and token on the commit builder, then build_blocking
//! let commit = session
//!     .new_upload_commit()?
//!     .with_endpoint("https://cas.example.com")
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
//! # Ok(())
//! # }
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
//! let session = XetSessionBuilder::new().build()?;
//!
//! // Upload — configure endpoint and token on the commit builder, then build().await
//! let commit = session
//!     .new_upload_commit()?
//!     .with_endpoint("https://cas.example.com")
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
//!
//! # Streaming upload
//!
//! Use [`upload_stream`](XetUploadCommit::upload_stream) when data arrives
//! incrementally and you don't want to buffer it all in memory or on disk
//! first.  Call [`write`](XetStreamUpload::write) for each chunk, then
//! [`finish`](XetStreamUpload::finish) before [`commit`](XetUploadCommit::commit).
//!
//! ```rust,no_run
//! use xet::xet_session::{Sha256Policy, XetSessionBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let session = XetSessionBuilder::new().build()?;
//! let commit = session
//!     .new_upload_commit()?
//!     .with_endpoint("https://cas.example.com")
//!     .with_token_info("write-token", 1_700_000_000)
//!     .build()
//!     .await?;
//!
//! // Begin a streaming upload with an optional tracking name
//! let stream = commit
//!     .upload_stream(Some("generated-data.bin".into()), Sha256Policy::Compute)
//!     .await?;
//!
//! // Feed data in chunks — could come from a network socket, a generator, etc.
//! for chunk in vec![b"hello ".to_vec(), b"world".to_vec()] {
//!     stream.write(chunk).await?;
//! }
//!
//! // Finalise the stream and get per-file metadata
//! let meta = stream.finish().await?;
//! println!("hash: {}, size: {:?}", meta.xet_info.hash, meta.xet_info.file_size);
//!
//! // Commit all uploads in this group
//! let report = commit.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Streaming download
//!
//! Use [`XetDownloadStreamGroup`] when you want to consume file data as a
//! byte stream rather than writing it to disk.  This is useful for serving
//! data over HTTP, piping it to another process, or processing it on the fly.
//!
//! [`download_stream`](XetDownloadStreamGroup::download_stream) returns
//! chunks in file order.
//! [`download_unordered_stream`](XetDownloadStreamGroup::download_unordered_stream)
//! returns `(offset, Bytes)` chunks in completion order for higher throughput
//! when the consumer can handle out-of-order data.
//!
//! ```rust,no_run
//! use xet::xet_session::{XetFileInfo, XetSessionBuilder};
//!
//! # async fn example(file_info: XetFileInfo) -> Result<(), Box<dyn std::error::Error>> {
//! let session = XetSessionBuilder::new().build()?;
//! let group = session
//!     .new_download_stream_group()?
//!     .with_token_info("read-token", 1_700_000_000)
//!     .build()
//!     .await?;
//!
//! // Ordered stream — chunks arrive in file order
//! let mut stream = group.download_stream(file_info.clone(), None).await?;
//! let mut total = 0u64;
//! while let Some(chunk) = stream.next().await? {
//!     total += chunk.len() as u64;
//!     // process chunk...
//! }
//! println!("received {total} bytes");
//!
//! // Byte-range request — only download bytes 1000..2000
//! let mut range_stream = group.download_stream(file_info.clone(), Some(1000..2000)).await?;
//! while let Some(chunk) = range_stream.next().await? {
//!     // process partial data...
//! }
//! # Ok(())
//! # }
//! ```

mod auth_group_builder;
mod common;
mod download_stream_group;
mod download_stream_handle;
mod errors;
#[cfg(not(target_family = "wasm"))]
mod file_download_group;
#[cfg(not(target_family = "wasm"))]
mod file_download_handle;
mod session;
mod task_runtime;
mod upload_commit;
mod upload_file_handle;
mod upload_stream_handle;

pub use download_stream_group::{XetDownloadStreamGroup, XetDownloadStreamGroupBuilder};
pub use download_stream_handle::{XetDownloadStream, XetUnorderedDownloadStream};
pub use errors::SessionError;
#[cfg(not(target_family = "wasm"))]
pub use file_download_group::{XetDownloadGroupReport, XetFileDownloadGroup, XetFileDownloadGroupBuilder};
#[cfg(not(target_family = "wasm"))]
pub use file_download_handle::{XetDownloadReport, XetFileDownload};
pub use http::{HeaderMap, HeaderValue, header};
pub use session::{XetSession, XetSessionBuilder};
pub use task_runtime::XetTaskState;
pub use upload_commit::{XetCommitReport, XetFileMetadata, XetUploadCommit, XetUploadCommitBuilder};
pub use upload_file_handle::XetFileUpload;
pub use upload_stream_handle::XetStreamUpload;
pub use xet_data::deduplication::DeduplicationMetrics;
pub use xet_data::processing::{Sha256Policy, XetFileInfo};
pub use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport};
pub use xet_runtime::config::XetConfig;
pub use xet_runtime::utils::UniqueId;
