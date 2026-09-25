//! Session-based file upload and download API for XetHub / HuggingFace Hub.
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
#![cfg_attr(feature = "upload", doc = "## Uploads")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "Call [`XetSession::new_upload_commit`] to obtain an [`AuthGroupBuilder`]."
)]
#![cfg_attr(
    feature = "upload",
    doc = "Configure auth with [`with_token_info`](AuthGroupBuilder::with_token_info) and"
)]
#![cfg_attr(
    feature = "upload",
    doc = "[`with_token_refresh_url`](AuthGroupBuilder::with_token_refresh_url), then call"
)]
#![cfg_attr(feature = "upload", doc = "[`build`](AuthGroupBuilder::build) (async) or")]
#![cfg_attr(
    feature = "upload",
    doc = "[`build_blocking`](AuthGroupBuilder::build_blocking) (sync)."
)]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(feature = "upload", doc = "There are three ways to queue data for upload:")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "- **From a file path** — [`upload_from_path`](XetUploadCommit::upload_from_path) /"
)]
#![cfg_attr(
    feature = "upload",
    doc = "  [`upload_from_path_blocking`](XetUploadCommit::upload_from_path_blocking). The file is read in a background task."
)]
#![cfg_attr(
    feature = "upload",
    doc = "- **From raw bytes** — [`upload_bytes`](XetUploadCommit::upload_bytes) /"
)]
#![cfg_attr(
    feature = "upload",
    doc = "  [`upload_bytes_blocking`](XetUploadCommit::upload_bytes_blocking). Useful when data is already in memory."
)]
#![cfg_attr(
    feature = "upload",
    doc = "- **Incrementally via a stream** — [`upload_stream`](XetUploadCommit::upload_stream) /"
)]
#![cfg_attr(
    feature = "upload",
    doc = "  [`upload_stream_blocking`](XetUploadCommit::upload_stream_blocking). Returns an [`XetStreamUpload`] handle; call"
)]
#![cfg_attr(
    feature = "upload",
    doc = "  [`write`](XetStreamUpload::write) to feed chunks, then [`finish`](XetStreamUpload::finish) to finalise. **`finish`"
)]
#![cfg_attr(
    feature = "upload",
    doc = "  must be called before [`commit`](XetUploadCommit::commit).** Use this when data arrives incrementally (e.g. from a"
)]
#![cfg_attr(
    feature = "upload",
    doc = "  network socket or a generator) and you don't want to buffer it all in memory first."
)]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(feature = "upload", doc = "Then call [`commit`](XetUploadCommit::commit) or")]
#![cfg_attr(
    feature = "upload",
    doc = "[`commit_blocking`](XetUploadCommit::commit_blocking) to wait for all"
)]
#![cfg_attr(feature = "upload", doc = "transfers to finish and receive a [`XetCommitReport`].")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "Per-file results are available via [`XetFileUpload::finalize_ingestion`] or"
)]
#![cfg_attr(
    feature = "upload",
    doc = "[`XetStreamUpload::finish`] at any time — even before `commit()`"
)]
#![cfg_attr(
    feature = "upload",
    doc = "completes.  Each result is a [`XetFileMetadata`] containing [`XetFileInfo`],"
)]
#![cfg_attr(feature = "upload", doc = "[`DeduplicationMetrics`], and an optional tracking name.")]
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
#![cfg_attr(
    feature = "upload",
    doc = "Both [`XetUploadCommit`] and [`XetFileDownloadGroup`] expose `progress()`,"
)]
#![cfg_attr(
    feature = "upload",
    doc = "which returns a [`GroupProgressReport`] without acquiring a lock on the"
)]
#![cfg_attr(
    feature = "upload",
    doc = "calling thread (useful for Python bindings that must release the GIL)."
)]
#![cfg_attr(
    feature = "upload",
    doc = "Poll it from a background thread/task while the main thread/task blocks"
)]
#![cfg_attr(feature = "upload", doc = "in `commit()` / `finish()`.")]
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
#![cfg_attr(
    feature = "upload",
    doc = "[`commit`](XetUploadCommit::commit) returns a [`XetCommitReport`] containing"
)]
#![cfg_attr(
    feature = "upload",
    doc = "aggregate dedup metrics, progress, and per-file [`XetFileMetadata`]."
)]
//! [`finish`](XetFileDownloadGroup::finish) returns
//! [`XetDownloadGroupReport`] keyed by task ID. If any download
//! fails, the error is propagated immediately.
//!
//! # Feature flags
//!
//! The `upload` feature (enabled by default) compiles in the upload pipeline:
//! `XetUploadCommit`, chunking/deduplication, and the CAS shard/xorb upload
//! code paths. Download-only consumers can disable it to
//! shrink the build:
//!
//! ```toml
//! hf-xet = { version = "1", default-features = false, features = ["rustls-tls"] }
//! ```
//!
//! All upload API is unavailable in that configuration; the download API
//! above is unaffected.
//!
//! # Quick start — downloads
//!
//! ```rust,no_run
//! use xet::xet_session::{XetFileInfo, XetSessionBuilder};
//!
//! # async fn example(file_info: XetFileInfo) -> Result<(), xet::xet_session::SessionError> {
//! let session = XetSessionBuilder::new().build()?;
//! let group = session
//!     .new_file_download_group()?
//!     .with_token_info("read-token", 1_700_000_000)
//!     .build()
//!     .await?;
//! let handle = group.download_file_to_path(file_info, "out/file.bin".into()).await?;
//! let finish_report = group.finish().await?;
//! let r = finish_report.downloads.get(&handle.task_id()).unwrap();
//! # Ok(())
//! # }
//! ```
#![cfg_attr(feature = "upload", doc = "# Quick start — sync API (upload + download)")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(feature = "upload", doc = "```rust,no_run")]
#![cfg_attr(
    feature = "upload",
    doc = "use xet::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};"
)]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "# fn example() -> Result<(), xet::xet_session::SessionError> {"
)]
#![cfg_attr(feature = "upload", doc = "let session = XetSessionBuilder::new().build()?;")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "// Upload — configure endpoint and token on the commit builder, then build_blocking"
)]
#![cfg_attr(feature = "upload", doc = "let commit = session")]
#![cfg_attr(feature = "upload", doc = "    .new_upload_commit()?")]
#![cfg_attr(feature = "upload", doc = "    .with_endpoint(\"https://cas.example.com\")")]
#![cfg_attr(feature = "upload", doc = "    .with_token_info(\"write-token\", 1_700_000_000)")]
#![cfg_attr(feature = "upload", doc = "    .build_blocking()?;")]
#![cfg_attr(
    feature = "upload",
    doc = "let handle = commit.upload_from_path_blocking(\"file.bin\".into(), Sha256Policy::Compute)?;"
)]
#![cfg_attr(feature = "upload", doc = "let meta = handle.finalize_ingestion_blocking()?;")]
#![cfg_attr(feature = "upload", doc = "let report = commit.commit_blocking()?;")]
#![cfg_attr(feature = "upload", doc = "// report.dedup_metrics, report.progress, report.files")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "// Download — configure token on the group builder, then build_blocking"
)]
#![cfg_attr(feature = "upload", doc = "let group = session")]
#![cfg_attr(feature = "upload", doc = "    .new_file_download_group()?")]
#![cfg_attr(feature = "upload", doc = "    .with_token_info(\"read-token\", 1_700_000_000)")]
#![cfg_attr(feature = "upload", doc = "    .build_blocking()?;")]
#![cfg_attr(feature = "upload", doc = "let info = meta.xet_info.clone();")]
#![cfg_attr(
    feature = "upload",
    doc = "let dl_handle = group.download_file_to_path_blocking(info, \"out/file.bin\".into())?;"
)]
#![cfg_attr(feature = "upload", doc = "let finish_report = group.finish_blocking()?;")]
#![cfg_attr(
    feature = "upload",
    doc = "let r = finish_report.downloads.get(&dl_handle.task_id()).unwrap();"
)]
#![cfg_attr(feature = "upload", doc = "# Ok(())")]
#![cfg_attr(feature = "upload", doc = "# }")]
#![cfg_attr(feature = "upload", doc = "```")]
//!
#![cfg_attr(feature = "upload", doc = "# Quick start — async API (upload + download)")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(feature = "upload", doc = "```rust,no_run")]
#![cfg_attr(
    feature = "upload",
    doc = "use xet::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};"
)]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "# async fn example() -> Result<(), xet::xet_session::SessionError> {"
)]
#![cfg_attr(
    feature = "upload",
    doc = "// build() auto-detects: if inside a suitable tokio runtime, wraps it;"
)]
#![cfg_attr(feature = "upload", doc = "// otherwise creates an owned thread pool.")]
#![cfg_attr(feature = "upload", doc = "let session = XetSessionBuilder::new().build()?;")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "// Upload — configure endpoint and token on the commit builder, then build().await"
)]
#![cfg_attr(feature = "upload", doc = "let commit = session")]
#![cfg_attr(feature = "upload", doc = "    .new_upload_commit()?")]
#![cfg_attr(feature = "upload", doc = "    .with_endpoint(\"https://cas.example.com\")")]
#![cfg_attr(feature = "upload", doc = "    .with_token_info(\"write-token\", 1_700_000_000)")]
#![cfg_attr(feature = "upload", doc = "    .build()")]
#![cfg_attr(feature = "upload", doc = "    .await?;")]
#![cfg_attr(
    feature = "upload",
    doc = "let handle = commit.upload_from_path(\"file.bin\".into(), Sha256Policy::Compute).await?;"
)]
#![cfg_attr(feature = "upload", doc = "let meta = handle.finalize_ingestion().await?;")]
#![cfg_attr(feature = "upload", doc = "let report = commit.commit().await?;")]
#![cfg_attr(feature = "upload", doc = "// report.dedup_metrics, report.progress, report.files")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "// Download — configure token on the group builder, then build().await"
)]
#![cfg_attr(feature = "upload", doc = "let group = session")]
#![cfg_attr(feature = "upload", doc = "    .new_file_download_group()?")]
#![cfg_attr(feature = "upload", doc = "    .with_token_info(\"read-token\", 1_700_000_000)")]
#![cfg_attr(feature = "upload", doc = "    .build()")]
#![cfg_attr(feature = "upload", doc = "    .await?;")]
#![cfg_attr(feature = "upload", doc = "let info = meta.xet_info.clone();")]
#![cfg_attr(
    feature = "upload",
    doc = "let dl_handle = group.download_file_to_path(info, \"out/file.bin\".into()).await?;"
)]
#![cfg_attr(feature = "upload", doc = "let finish_report = group.finish().await?;")]
#![cfg_attr(
    feature = "upload",
    doc = "let r = finish_report.downloads.get(&dl_handle.task_id()).unwrap();"
)]
#![cfg_attr(feature = "upload", doc = "# Ok(())")]
#![cfg_attr(feature = "upload", doc = "# }")]
#![cfg_attr(feature = "upload", doc = "```")]
//!
#![cfg_attr(feature = "upload", doc = "# Streaming upload")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "Use [`upload_stream`](XetUploadCommit::upload_stream) when data arrives"
)]
#![cfg_attr(
    feature = "upload",
    doc = "incrementally and you don't want to buffer it all in memory or on disk"
)]
#![cfg_attr(
    feature = "upload",
    doc = "first.  Call [`write`](XetStreamUpload::write) for each chunk, then"
)]
#![cfg_attr(
    feature = "upload",
    doc = "[`finish`](XetStreamUpload::finish) before [`commit`](XetUploadCommit::commit)."
)]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(feature = "upload", doc = "```rust,no_run")]
#![cfg_attr(feature = "upload", doc = "use xet::xet_session::{Sha256Policy, XetSessionBuilder};")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "# async fn example() -> Result<(), Box<dyn std::error::Error>> {"
)]
#![cfg_attr(feature = "upload", doc = "let session = XetSessionBuilder::new().build()?;")]
#![cfg_attr(feature = "upload", doc = "let commit = session")]
#![cfg_attr(feature = "upload", doc = "    .new_upload_commit()?")]
#![cfg_attr(feature = "upload", doc = "    .with_endpoint(\"https://cas.example.com\")")]
#![cfg_attr(feature = "upload", doc = "    .with_token_info(\"write-token\", 1_700_000_000)")]
#![cfg_attr(feature = "upload", doc = "    .build()")]
#![cfg_attr(feature = "upload", doc = "    .await?;")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "// Begin a streaming upload with an optional tracking name"
)]
#![cfg_attr(feature = "upload", doc = "let stream = commit")]
#![cfg_attr(
    feature = "upload",
    doc = "    .upload_stream(Some(\"generated-data.bin\".into()), Sha256Policy::Compute)"
)]
#![cfg_attr(feature = "upload", doc = "    .await?;")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(
    feature = "upload",
    doc = "// Feed data in chunks — could come from a network socket, a generator, etc."
)]
#![cfg_attr(
    feature = "upload",
    doc = "for chunk in vec![b\"hello \".to_vec(), b\"world\".to_vec()] {"
)]
#![cfg_attr(feature = "upload", doc = "    stream.write(chunk).await?;")]
#![cfg_attr(feature = "upload", doc = "}")]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(feature = "upload", doc = "// Finalise the stream and get per-file metadata")]
#![cfg_attr(feature = "upload", doc = "let meta = stream.finish().await?;")]
#![cfg_attr(
    feature = "upload",
    doc = "println!(\"hash: {}, size: {:?}\", meta.xet_info.hash, meta.xet_info.file_size);"
)]
#![cfg_attr(feature = "upload", doc = "")]
#![cfg_attr(feature = "upload", doc = "// Commit all uploads in this group")]
#![cfg_attr(feature = "upload", doc = "let report = commit.commit().await?;")]
#![cfg_attr(feature = "upload", doc = "# Ok(())")]
#![cfg_attr(feature = "upload", doc = "# }")]
#![cfg_attr(feature = "upload", doc = "```")]
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
//!
//! ## WASM availability
//!
//! On `wasm32-unknown-unknown` the surface is a strict subset:
//!
//! - **Async only** — `_blocking` variants are non-wasm (wasm cannot block the host thread).
//! - **No filesystem entrypoints** — `upload_from_path`, `XetFileDownloadGroup`, and `XetFileDownload` are non-wasm;
//!   use `upload_bytes` / `upload_stream` and `XetDownloadStreamGroup` instead.
//! - **No external tokio handle** — `XetSessionBuilder::with_tokio_handle` is non-wasm.
//!
//! Doc links resolve on every target, but `_blocking` and path-based names
//! point at items that don't exist on wasm; the per-method `#[cfg(...)]`
//! attributes are the authoritative target gate.
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
#[cfg(test)]
#[cfg(feature = "upload")]
mod test_utils;
#[cfg(feature = "upload")]
mod upload_commit;
#[cfg(feature = "upload")]
mod upload_file_handle;
#[cfg(feature = "upload")]
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
#[cfg(feature = "upload")]
pub use upload_commit::{XetCommitReport, XetFileMetadata, XetUploadCommit, XetUploadCommitBuilder};
#[cfg(feature = "upload")]
pub use upload_file_handle::XetFileUpload;
#[cfg(feature = "upload")]
pub use upload_stream_handle::XetStreamUpload;
#[cfg(feature = "upload")]
pub use xet_data::deduplication::DeduplicationMetrics;
#[cfg(feature = "upload")]
pub use xet_data::processing::Sha256Policy;
pub use xet_data::processing::XetFileInfo;
pub use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, ShardUploadProgressReport};
pub use xet_runtime::config::XetConfig;
pub use xet_runtime::utils::UniqueId;
