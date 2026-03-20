# API Update: Upload Handle Refactor — Inner/Wrapper Pattern, Streaming API (2026-03-18)

## Overview

The upload API has been restructured for clarity and ergonomics:

1. **`UploadTaskHandle` renamed to `UploadFileHandle`** — for file-based uploads (from path or bytes).
2. **New `UploadStreamHandle`** — for incremental streaming uploads via `write()` + `finish()`.
3. **`upload_file` / `upload_file_blocking` renamed to `upload_stream` / `upload_stream_blocking`** — returns `UploadStreamHandle` instead of exposing `SingleFileCleaner`.
4. **`file_size` parameter dropped** from `upload_stream` / `upload_stream_blocking` (size is tracked incrementally).
5. **Inner/wrapper pattern** — `UploadCommit`, `UploadFileHandle`, and `UploadStreamHandle` now wrap `Arc<Inner>`, making them cheaply clonable without external `Arc`.
6. **`FileUploadSession::start_clean`** size parameter changed from `u64` to `Option<u64>`.

---

## Renamed Types

| Old Name | New Name | Notes |
|---|---|---|
| `UploadTaskHandle` | `UploadFileHandle` | Returned by `upload_from_path` and `upload_bytes` |

---

## New Type: `UploadStreamHandle`

Returned by `upload_stream` / `upload_stream_blocking`. Provides:

| Method | Description |
|---|---|
| `write(&self, &[u8])` | Feed data incrementally (async) |
| `write_blocking(&self, &[u8])` | Feed data incrementally (sync) |
| `finish(&self)` | Finalise ingestion, returns `FileMetadata` (async) |
| `finish_blocking(&self)` | Finalise ingestion (sync) |
| `try_finish(&self)` | Non-blocking check if finished |
| `get_progress(&self)` | Per-file progress snapshot |
| `abort(&self)` | Cancel the upload |
| `task_id(&self)` | Unique task identifier |

**`finish()` must be called before `UploadCommit::commit()`** — committing with an unfinished stream is an error.

---

## Renamed Methods

| Old Method | New Method | Notes |
|---|---|---|
| `UploadCommit::upload_file()` | `UploadCommit::upload_stream()` | Returns `UploadStreamHandle` (not `Arc<UploadStreamHandle>`) |
| `UploadCommit::upload_file_blocking()` | `UploadCommit::upload_stream_blocking()` | Returns `UploadStreamHandle` |

---

## Dropped Parameters

| Method | Dropped Parameter | Reason |
|---|---|---|
| `upload_stream` / `upload_stream_blocking` | `file_size: u64` | Size is tracked incrementally via `SingleFileCleaner::add_data` |

---

## Return Type Changes (Arc Removal)

All upload types now wrap `Arc` internally and are cheaply clonable. External `Arc` wrapping is no longer needed.

| Method | Old Return Type | New Return Type |
|---|---|---|
| `XetSession::new_upload_commit()` | `Result<Arc<UploadCommit>, XetError>` | `Result<UploadCommit, XetError>` |
| `XetSession::new_upload_commit_blocking()` | `Result<Arc<UploadCommit>, XetError>` | `Result<UploadCommit, XetError>` |
| `UploadCommit::upload_stream()` | `Result<Arc<UploadStreamHandle>, XetError>` | `Result<UploadStreamHandle, XetError>` |
| `UploadCommit::upload_stream_blocking()` | `Result<Arc<UploadStreamHandle>, XetError>` | `Result<UploadStreamHandle, XetError>` |

---

## Method Receiver Changes

All `UploadCommit` methods now take `&self` instead of `self: &Arc<Self>`:

| Method | Old Receiver | New Receiver |
|---|---|---|
| `upload_from_path` | `self: &Arc<Self>` | `&self` |
| `upload_stream` | `self: &Arc<Self>` | `&self` |
| `upload_bytes` | `self: &Arc<Self>` | `&self` |
| `commit` | `self: Arc<Self>` | `&self` |

---

## `FileUploadSession::start_clean` Signature Change

```rust
// Old
pub fn start_clean(&self, name: Option<Arc<str>>, size: u64, sha256: Sha256Policy)

// New
pub fn start_clean(&self, name: Option<Arc<str>>, size: Option<u64>, sha256: Sha256Policy)
```

Pass `Some(n)` when the total size is known upfront; pass `None` for streaming
uploads where size is accumulated incrementally via `increment_file_size`.

---

## Migration Guide

### Sync callers

```rust
// Old
let commit: Arc<UploadCommit> = session.new_upload_commit_blocking()?;
let handle = commit.upload_file_blocking(Some("name".into()), file_size, sha256)?;
// handle was Arc<UploadStreamHandle>
let progress_clone = Arc::clone(&commit);

// New
let commit: UploadCommit = session.new_upload_commit_blocking()?;
let handle = commit.upload_stream_blocking(Some("name".into()), sha256)?;
// handle is UploadStreamHandle (cheaply clonable)
let progress_clone = commit.clone();
```

### Async callers

```rust
// Old
let commit: Arc<UploadCommit> = session.new_upload_commit().await?;
let handle: Arc<UploadStreamHandle> = commit.upload_file(name, size, sha256).await?;
Arc::clone(&commit).commit().await?;

// New
let commit: UploadCommit = session.new_upload_commit().await?;
let handle: UploadStreamHandle = commit.upload_stream(name, sha256).await?;
commit.commit().await?;
```

### `start_clean` callers (xet_data)

```rust
// Old
let (id, cleaner) = session.start_clean(name, data.len() as u64, sha256)?;

// New
let (id, cleaner) = session.start_clean(name, Some(data.len() as u64), sha256)?;
// For streaming (unknown size):
let (id, cleaner) = session.start_clean(name, None, sha256)?;
```

### Import changes

```rust
// Old
use xet::xet_session::UploadTaskHandle;

// New
use xet::xet_session::UploadFileHandle;
// Also available:
use xet::xet_session::UploadStreamHandle;
```

---

## `SingleFileCleaner` No Longer Exposed

`SingleFileCleaner` is no longer part of the public API. Streaming uploads
are now handled entirely through `UploadStreamHandle::write()` and
`UploadStreamHandle::finish()`.

---

## Files Changed

| File | Change |
|---|---|
| `xet_pkg/src/xet_session/upload_commit.rs` | Inner/wrapper pattern; `UploadCommitInner`, `UploadFileHandleInner`, `UploadStreamHandleInner` (private); wrappers are public with async + blocking methods |
| `xet_pkg/src/xet_session/session.rs` | `Arc<UploadCommit>` → `UploadCommit` in active commits map and factory return types |
| `xet_pkg/src/xet_session/mod.rs` | Exports `UploadFileHandle` and `UploadStreamHandle` (replaces `UploadTaskHandle`) |
| `xet_pkg/examples/example.rs` | `Arc::clone(&commit)` → `commit.clone()` |
| `xet_pkg/examples/example_sync.rs` | `Arc::clone(&commit)` → `commit.clone()` |
| `xet_data/src/processing/file_upload_session.rs` | `start_clean` size param: `u64` → `Option<u64>` |
| `xet_data/src/processing/data_client.rs` | Updated `start_clean` calls to `Some(size)` |
| `xet_data/src/processing/bin/example.rs` | Updated `start_clean` call |
| `xet_data/src/processing/file_download_session.rs` | Updated test `start_clean` call |
| `xet_data/tests/test_full_file_download.rs` | Updated `start_clean` call |
| `xet_data/tests/test_session_resume.rs` | Updated `start_clean` calls |
