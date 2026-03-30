# API Update: xet_session upload/download API polish (2026-03-16 → 2026-03-26)

## Overview

This update is a comprehensive API redesign of `xet_pkg::xet_session`.  It
introduces a new `TaskRuntime` layer with hierarchical cancellation tokens,
replaces the old task-handle / group-state model with per-handle types
(`XetFileUpload`, `XetStreamUpload`, `XetFileDownload`), and consolidates
error variants.  All public type names now carry a `Xet` prefix for clarity.

---

## Renamed types

| Old name              | New name                |
|-----------------------|-------------------------|
| `UploadCommit`        | `XetUploadCommit`       |
| `FileDownloadGroup`   | `XetDownloadGroup`      |
| `UploadTaskHandle`    | `XetFileUpload`         |
| `DownloadTaskHandle`  | `XetFileDownload`       |
| `TaskHandle`          | *(removed)*             |
| `TaskStatus`          | `XetTaskState`          |
| `FileMetadata`        | `XetFileMetadata`       |
| `DownloadedFile`      | `XetDownloadReport`     |
| `UploadResult`        | *(removed; see below)*  |
| `DownloadResult`      | *(removed; see below)*  |
| `GroupState`           | *(removed; state managed by `TaskRuntime`)* |

## Removed types and modules

- **`tasks.rs`** — deleted entirely.  `TaskHandle`, `TaskStatus`,
  `UploadTaskHandle`, `DownloadTaskHandle` are all gone.
- **`UploadResult`** (`Arc<Result<FileMetadata, SessionError>>`) — removed.
  Per-file metadata is now obtained through `XetFileUpload::finalize_ingestion()`.
- **`DownloadResult`** (`Arc<Result<DownloadedFile, XetError>>`) — removed.
  Per-file download reports come from `XetDownloadGroupReport.downloads`.
- **`DownloadedFile`** — replaced by `XetDownloadReport`.
- **`GroupState`** enum (`Alive`, `Finished`, `Aborted`) — removed from `common.rs`.
  State management is now handled by `TaskRuntime`.
- **`SessionState`** enum (`Alive`, `Aborted`) in session.rs — removed.
  Session-level state is tracked by the root `TaskRuntime`.

## New types

### `XetFileUpload` (was `UploadTaskHandle`)

Handle returned by `XetUploadCommit::upload_from_path` and
`XetUploadCommit::upload_bytes`.

```rust
pub struct XetFileUpload { /* Arc inner + TaskRuntime */ }

impl XetFileUpload {
    pub fn task_id(&self) -> UniqueID;
    pub fn file_path(&self) -> Option<PathBuf>;
    pub fn progress(&self) -> Option<ItemProgressReport>;
    pub fn status(&self) -> Result<XetTaskState, XetError>;
    pub async fn finalize_ingestion(&self) -> Result<XetFileMetadata, XetError>;
    pub fn finalize_ingestion_blocking(&self) -> Result<XetFileMetadata, XetError>;
    pub fn try_finish(&self) -> Option<XetFileMetadata>;
}
```

Key difference: callers now call `finalize_ingestion()` on the handle to get
per-file metadata **before or after** `commit()`.  Previously, metadata was
only available from the `HashMap` returned by `commit()`.

### `XetStreamUpload` (replaces `(TaskHandle, SingleFileCleaner)` pair)

Handle for incremental streaming uploads.  Returned by
`XetUploadCommit::upload_stream` (was `upload_file`).

```rust
pub struct XetStreamUpload { /* Arc inner + TaskRuntime */ }

impl XetStreamUpload {
    pub fn task_id(&self) -> UniqueID;
    pub async fn write(&self, data: impl Into<Bytes>) -> Result<(), XetError>;
    pub fn write_blocking(&self, data: impl Into<Bytes>) -> Result<(), XetError>;
    pub async fn finish(&self) -> Result<XetFileMetadata, XetError>;
    pub fn finish_blocking(&self) -> Result<XetFileMetadata, XetError>;
    pub fn try_finish(&self) -> Option<XetFileMetadata>;
    pub fn progress(&self) -> Option<ItemProgressReport>;
    pub fn status(&self) -> Result<XetTaskState, XetError>;
    pub fn abort(&self);
}
```

Key differences from old `upload_file`:
- `upload_file(name, file_size, sha256)` → `upload_stream(tracking_name, sha256)`
  (`file_size` parameter removed).
- Returns `XetStreamUpload` instead of `(TaskHandle, SingleFileCleaner)`.
- Data is fed via `handle.write(bytes)` instead of `cleaner.add_data(&[u8])`.
- Finalization via `handle.finish()` instead of `cleaner.finish()`.
- **`finish()` must be called before `commit()`**; committing with an
  unfinished stream is an error.

### `XetFileDownload` (was `DownloadTaskHandle`)

Handle returned by `XetDownloadGroup::download_file_to_path`.

```rust
pub struct XetFileDownload { /* Arc inner + TaskRuntime */ }

impl XetFileDownload {
    pub fn task_id(&self) -> UniqueID;
    pub fn file_path(&self) -> PathBuf;
    pub fn progress(&self) -> Option<ItemProgressReport>;
    pub fn status(&self) -> Result<XetTaskState, XetError>;
    pub fn result(&self) -> Option<Result<XetDownloadReport, XetError>>;
    pub async fn finish(&self) -> Result<XetDownloadReport, XetError>;
    pub fn finish_blocking(&self) -> Result<XetDownloadReport, XetError>;
    pub fn cancel(&self);
}
```

### `XetFileMetadata` (was `FileMetadata`)

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XetFileMetadata {
    #[serde(skip)]
    pub task_id: UniqueID,
    pub xet_info: XetFileInfo,          // was separate hash/file_size/sha256 fields
    pub dedup_metrics: DeduplicationMetrics,  // new
    pub tracking_name: Option<String>,
}
```

Key differences from old `FileMetadata`:
- `hash: String`, `file_size: u64`, `sha256: Option<String>` →
  `xet_info: XetFileInfo` (which has `hash`, `file_size: Option<u64>`,
  `sha256: Option<String>`).
- Added `dedup_metrics: DeduplicationMetrics` per file.
- Added `task_id: UniqueID` (skipped in serde).

### `XetCommitReport` (new)

Returned by `XetUploadCommit::commit()`.

```rust
#[derive(Clone, Debug)]
pub struct XetCommitReport {
    pub dedup_metrics: DeduplicationMetrics,
    pub progress: GroupProgressReport,
    pub uploads: HashMap<UniqueID, XetFileMetadata>,
}
```

Previously `commit()` returned `HashMap<UniqueID, UploadResult>`.  Now it
returns this aggregate report with dedup metrics and progress included.

### `XetDownloadGroupReport` (new)

Returned by `XetDownloadGroup::finish()`.

```rust
#[derive(Clone, Debug)]
pub struct XetDownloadGroupReport {
    pub progress: GroupProgressReport,
    pub downloads: HashMap<UniqueID, XetDownloadReport>,
}
```

Previously `finish()` returned `HashMap<UniqueID, DownloadResult>`.

### `XetDownloadReport` (replaces `DownloadedFile`)

```rust
#[derive(Clone, Debug)]
pub struct XetDownloadReport {
    pub task_id: UniqueID,
    pub path: Option<PathBuf>,       // was dest_path: PathBuf
    pub file_info: XetFileInfo,
    pub progress: Option<ItemProgressReport>,  // new
}
```

### `XetTaskState` (replaces `TaskStatus`)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum XetTaskState {
    Running,
    Finalizing,   // new state
    Completed,
    Error(String),
    UserCancelled,
}
```

Differences: added `Finalizing` state; `Failed` → `Error(String)`;
`Queued` removed; `Cancelled` → `UserCancelled`.

### `TaskRuntime` (new, `pub(super)`)

Internal hierarchical runtime that wraps `XetRuntime` with:
- `CancellationToken`-driven subtree cancellation
- State machine (`Running` → `Finalizing` → `Completed` / `Error` / `UserCancelled`)
- Bridge methods: `bridge_async`, `bridge_sync`, `bridge_async_finalizing`,
  `bridge_sync_finalizing`
- Child creation (`child()`) for per-handle runtimes
- Background task state management (`BackgroundTaskState<T>`)

### `BackgroundTaskState<T>` (new, `pub(super)`)

```rust
pub(super) enum BackgroundTaskState<T> {
    Running { join_handle: Option<JoinHandle<Result<T, XetError>>> },
    Success(T),
    Error(String),
}
```

Used by `XetFileUpload` and `XetFileDownload` to track background task lifecycle.

---

## Method signature changes

### `XetUploadCommit` (was `UploadCommit`)

| Old | New |
|-----|-----|
| `upload_from_path(path, sha256) -> Result<UploadTaskHandle>` | `upload_from_path(path, sha256) -> Result<XetFileUpload>` |
| `upload_from_path_blocking(path, sha256) -> Result<UploadTaskHandle>` | `upload_from_path_blocking(path, sha256) -> Result<XetFileUpload>` |
| `upload_bytes(bytes, sha256, name) -> Result<UploadTaskHandle>` | `upload_bytes(bytes, sha256, name) -> Result<XetFileUpload>` |
| `upload_bytes_blocking(bytes, sha256, name) -> Result<UploadTaskHandle>` | `upload_bytes_blocking(bytes, sha256, name) -> Result<XetFileUpload>` |
| `upload_file(name, file_size, sha256) -> Result<(TaskHandle, SingleFileCleaner)>` | `upload_stream(tracking_name, sha256) -> Result<XetStreamUpload>` |
| `upload_file_blocking(name, file_size, sha256) -> Result<(TaskHandle, SingleFileCleaner)>` | `upload_stream_blocking(tracking_name, sha256) -> Result<XetStreamUpload>` |
| `commit(self) -> Result<HashMap<UniqueID, UploadResult>>` | `commit(&self) -> Result<XetCommitReport>` |
| `commit_blocking(self) -> Result<HashMap<UniqueID, UploadResult>>` | `commit_blocking(&self) -> Result<XetCommitReport>` |
| `get_progress() -> Result<GroupProgressReport>` | `progress() -> GroupProgressReport` |
| `get_progress_blocking() -> Result<GroupProgressReport>` | *(removed; use `progress()` directly — it is non-blocking)* |
| *(none)* | `status() -> Result<XetTaskState>` |

Note: `commit()` now takes `&self` instead of `self`.

### `XetDownloadGroup` (was `FileDownloadGroup`)

| Old | New |
|-----|-----|
| `download_file_to_path(info, path) -> Result<DownloadTaskHandle>` | `download_file_to_path(info, path) -> Result<XetFileDownload>` |
| `download_file_to_path_blocking(info, path) -> Result<DownloadTaskHandle>` | `download_file_to_path_blocking(info, path) -> Result<XetFileDownload>` |
| `finish(self) -> Result<HashMap<UniqueID, DownloadResult>>` | `finish(self) -> Result<XetDownloadGroupReport>` |
| `finish_blocking(self) -> Result<HashMap<UniqueID, DownloadResult>>` | `finish_blocking(self) -> Result<XetDownloadGroupReport>` |
| `get_progress() -> Result<GroupProgressReport>` | `progress() -> GroupProgressReport` |
| `get_progress_blocking() -> Result<GroupProgressReport>` | *(removed; use `progress()` directly — it is infallible and non-blocking)* |
| *(none)* | `status() -> Result<XetTaskState>` |

### `XetSession`

| Old | New |
|-----|-----|
| `new_upload_commit() -> Result<UploadCommit>` | `new_upload_commit() -> Result<XetUploadCommit>` |
| `new_upload_commit_blocking() -> Result<UploadCommit>` | `new_upload_commit_blocking() -> Result<XetUploadCommit>` |
| `new_file_download_group() -> Result<FileDownloadGroup>` | `new_file_download_group() -> Result<XetDownloadGroup>` |
| `new_file_download_group_blocking() -> Result<FileDownloadGroup>` | `new_file_download_group_blocking() -> Result<XetDownloadGroup>` |
| *(none)* | `status() -> Result<XetTaskState>` |
| `abort()` — SIGINT shutdown + state abort | `abort()` — cancels TaskRuntime subtree + per-handle abort (no SIGINT) |
| *(none)* | `sigint_abort()` — performs runtime SIGINT shutdown |
| `check_alive() -> Result<()>` | *(removed; state checked by TaskRuntime bridge methods)* |

`XetSession` no longer implements `Deref<Target = XetSessionInner>`.  Internal
fields are accessed via `self.inner`.

### Download streams

| Old | New |
|-----|-----|
| `XetDownloadStream::get_progress()` | `XetDownloadStream::progress()` |
| `XetUnorderedDownloadStream::get_progress()` | `XetUnorderedDownloadStream::progress()` |

Both `XetDownloadStream` and `XetUnorderedDownloadStream` now carry a
`TaskRuntime` and propagate cancellation through it when `cancel()` is called.

Constructor changed:
```rust
// Old
XetDownloadStream::new(inner, download_session, id)
// New
XetDownloadStream::new(inner, download_session, id, task_runtime)
```

---

## Error variant changes (`XetError`)

| Old variant | New variant |
|-------------|-------------|
| `Aborted` | *(removed)* |
| `AlreadyCommitted` | `AlreadyCompleted` |
| `AlreadyFinished` | `AlreadyCompleted` |
| *(none)* | `KeyboardInterrupt` |
| *(none)* | `UserCancelled(String)` |
| *(none)* | `PreviousTaskError(String)` |
| *(none)* | `TaskError(String)` |
| `Cancelled(String)` | `Cancelled(String)` — now only for non-user-initiated cancellation |

### Python mapping

- `XetError::KeyboardInterrupt` → `PyKeyboardInterrupt`
- `XetError::UserCancelled` → `PyRuntimeError`
- `XetError::PreviousTaskError` → `PyRuntimeError`
- `XetError::TaskError` → `PyRuntimeError`
- `XetError::AlreadyCompleted` → `PyRuntimeError`

### RuntimeError

- New variant: `RuntimeError::KeyboardInterrupt`
- `RuntimeError::KeyboardInterrupt` maps to `XetError::KeyboardInterrupt`
  (previously SIGINT mapped to `XetError::Cancelled` via `TaskCanceled`).
- `XetRuntime::bridge_async` and `bridge_sync` now check for SIGINT
  shutdown before dispatching.

---

## Cancellation model

### Old model

- `XetSession` had a `SessionState` (`Alive` / `Aborted`) mutex.
- `UploadCommit` and `FileDownloadGroup` each had a `GroupState` mutex.
- `abort()` set state to `Aborted`, called `runtime.perform_sigint_shutdown()`,
  and propagated state to registered tasks.
- Every public method called `session.check_alive()` and
  `check_accepting_tasks()` before proceeding.

### New model

- `XetSession` owns a root `TaskRuntime` (wraps `XetRuntime` +
  `CancellationToken` + `XetTaskState`).
- `XetUploadCommit` and `XetDownloadGroup` each get a child `TaskRuntime`.
- `XetFileUpload`, `XetFileDownload`, `XetStreamUpload` each get a
  grandchild `TaskRuntime`.
- `abort()` calls `task_runtime.cancel_subtree()` which cancels the
  `CancellationToken` and sets state to `UserCancelled` recursively.
- Bridge methods (`bridge_async`, `bridge_sync`) check the cancellation
  token via `tokio::select!` and return `XetError::UserCancelled` if triggered.
- Background upload/download join handles are wrapped in `tokio::select!`
  observing the child `CancellationToken`, aborting the owned inner join
  handle when cancelled.

---

## Module structure changes (`xet_session`)

### Removed
- `tasks.rs` (module deleted)

### Added
- `task_runtime.rs` — `TaskRuntime`, `BackgroundTaskState`, `XetTaskState`
- `upload_file_handle.rs` — `XetFileUpload`, `XetFileUploadInner`
- `upload_stream_handle.rs` — `XetStreamUpload`, `XetStreamUploadInner`
- `file_download_handle.rs` — `XetFileDownload`, `XetFileDownloadInner`,
  `XetDownloadReport`

### Public re-exports changed

```rust
// Old
pub use file_download_group::{DownloadResult, DownloadedFile, FileDownloadGroup};
pub use tasks::{DownloadTaskHandle, TaskHandle, TaskStatus, UploadTaskHandle};
pub use upload_commit::{FileMetadata, UploadCommit, UploadResult};

// New
pub use file_download_group::{XetDownloadGroup, XetDownloadGroupReport};
pub use file_download_handle::{XetDownloadReport, XetFileDownload};
pub use task_runtime::XetTaskState;
pub use upload_commit::{XetCommitReport, XetFileMetadata, XetUploadCommit};
pub use upload_file_handle::XetFileUpload;
pub use upload_stream_handle::XetStreamUpload;
pub use xet_data::deduplication::DeduplicationMetrics;
```

New public re-export: `DeduplicationMetrics` from `xet_data`.

---

## Data crate changes (`xet_data`)

### `DeduplicationMetrics`

Added `Serialize, Deserialize` derives.

### `SingleFileCleaner`

New method: `pub async fn add_data_from_bytes(&mut self, data: Bytes) -> Result<()>`
— zero-copy variant of `add_data` that accepts `Bytes` directly.

Internal method `add_data_impl` renamed to `add_data_chunk_impl` (now private).

### `FileUploadSession`

- `spawn_upload_from_path` return type changed:
  `Result<(UniqueID, JoinHandle<Result<XetFileInfo>>)>` →
  `Result<(UniqueID, JoinHandle<Result<(XetFileInfo, DeduplicationMetrics)>>)>`
- `spawn_upload_bytes` return type changed similarly.
- `feed_file_to_cleaner` return type changed:
  `Result<XetFileInfo>` → `Result<(XetFileInfo, DeduplicationMetrics)>`

---

## Runtime changes (`xet_runtime`)

### `RuntimeError`

New variant: `RuntimeError::KeyboardInterrupt`.

### `XetRuntime`

- New private method `check_sigint()`.
- `bridge_async` now calls `check_sigint()?` before dispatching.
- `bridge_sync` now calls `check_sigint()?` before dispatching.
- SIGINT shutdown now produces `RuntimeError::KeyboardInterrupt` instead of
  `RuntimeError::TaskCanceled("CTRL-C Cancellation")`.

### Python config

Minor: `self.try_set(&s).map_err(|e| ...)` → `self.try_set(&s).map_err(...)` (closure to fn pointer).

---

## Usage pattern migration

### Uploading files (async)

```rust
// OLD
let commit = session.new_upload_commit().await?;
let handle = commit.upload_from_path("file.bin".into(), Sha256Policy::Compute).await?;
let results = commit.commit().await?;
let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
// meta.hash, meta.file_size, meta.sha256

// NEW
let commit = session.new_upload_commit().await?;
let handle = commit.upload_from_path("file.bin".into(), Sha256Policy::Compute).await?;
let file_meta = handle.finalize_ingestion().await?;
let report = commit.commit().await?;
// file_meta.xet_info.hash, file_meta.xet_info.file_size, file_meta.xet_info.sha256
// report.dedup_metrics, report.progress, report.uploads
```

### Streaming uploads (async)

```rust
// OLD
let (handle, mut cleaner) = commit.upload_file(
    Some("stream.bin".into()), Some(data.len() as u64), Sha256Policy::Compute
).await?;
cleaner.add_data(data).await?;
let (xfi, _metrics) = cleaner.finish().await?;
commit.commit().await?;

// NEW
let handle = commit.upload_stream(Some("stream.bin".into()), Sha256Policy::Compute).await?;
handle.write(&data[..]).await?;
let meta = handle.finish().await?;
commit.commit().await?;
```

### Downloading files (async)

```rust
// OLD
let group = session.new_file_download_group().await?;
let dl_handle = group.download_file_to_path(info, "out/file.bin".into()).await?;
let results = group.finish().await?;
let r = results.get(&dl_handle.task_id).unwrap().as_ref().as_ref().unwrap();
// r.dest_path, r.file_info

// NEW
let group = session.new_file_download_group().await?;
let dl_handle = group.download_file_to_path(info, "out/file.bin".into()).await?;
let report = group.finish().await?;
let r = report.downloads.get(&dl_handle.task_id()).unwrap();
// r.path, r.file_info, r.progress
```

### Constructing XetFileInfo from metadata

```rust
// OLD
let info = XetFileInfo {
    hash: meta.hash.clone(),
    file_size: Some(meta.file_size),
    sha256: meta.sha256.clone(),
};

// NEW
let info = file_meta.xet_info.clone();
```

### Progress tracking

```rust
// OLD
let report = commit.get_progress()?;        // or get_progress_blocking()
let report = group.get_progress()?;          // or get_progress_blocking()
let report = stream.get_progress();

// NEW
let report = commit.progress();              // infallible, non-blocking
let report = group.progress();
let report = stream.progress();
```

### Status checking

```rust
// OLD
if matches!(handle.status(), Ok(TaskStatus::Completed)) { ... }

// NEW
if matches!(handle.status(), Ok(XetTaskState::Completed)) { ... }
// Also: XetTaskState::Running, Finalizing, Error(String), UserCancelled
```

---

## Compatibility notes

- All renames are breaking changes; downstream code must update all type
  references.
- `commit()` / `commit_blocking()` now take `&self` instead of consuming
  `self`; the method uses `TaskRuntime` state to prevent double-commit
  (returns `XetError::AlreadyCompleted`).
- `SingleFileCleaner` is no longer exposed in the public API; all streaming
  interactions go through `XetStreamUpload`.
- Error matching must update: `XetError::Aborted` → check for
  `XetError::UserCancelled(_)` or `XetError::KeyboardInterrupt`.
- `AlreadyCommitted` and `AlreadyFinished` → `AlreadyCompleted`.
- Background task failures now carry messages via `TaskError(String)` or
  `PreviousTaskError(String)` instead of generic `SessionError`.
