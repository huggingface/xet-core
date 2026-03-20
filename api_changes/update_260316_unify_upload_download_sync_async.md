# API Update: Unify UploadCommit and DownloadGroup async/sync types (2026-03-16)

## Overview

`UploadCommitSync` and `DownloadGroupSync` have been removed. Their blocking
methods now live directly on `UploadCommit` and `DownloadGroup` as `_blocking`
suffixed methods. The session factory methods `new_upload_commit_blocking()`
and `new_download_group_blocking()` now return the same types as their async
counterparts.

---

## Removed Types

| Removed Type | Replacement |
|---|---|
| `UploadCommitSync` (was in `xet::xet_session::sync::UploadCommitSync`) | `UploadCommit` with `_blocking` methods |
| `DownloadGroupSync` (was in `xet::xet_session::sync::DownloadGroupSync`) | `DownloadGroup` with `_blocking` methods |

The entire `xet::xet_session::sync` module has been deleted.

---

## Changed Return Types

| Method | Old Return Type | New Return Type |
|---|---|---|
| `XetSession::new_upload_commit_blocking()` | `Result<UploadCommitSync, SessionError>` | `Result<UploadCommit, SessionError>` |
| `XetSession::new_download_group_blocking()` | `Result<DownloadGroupSync, SessionError>` | `Result<DownloadGroup, SessionError>` |

---

## New Blocking Methods on `UploadCommit`

**Note:** These signatures were further updated in `update_260318_upload_handle_refactor.md`.

| New Method | Async Equivalent |
|---|---|
| `upload_from_path_blocking(&self, PathBuf, Sha256Policy) -> Result<UploadFileHandle, XetError>` | `upload_from_path(&self, PathBuf, Sha256Policy).await` |
| `upload_bytes_blocking(&self, Vec<u8>, Sha256Policy, Option<String>) -> Result<UploadFileHandle, XetError>` | `upload_bytes(&self, Vec<u8>, Sha256Policy, Option<String>).await` |
| `upload_stream_blocking(&self, Option<String>, Sha256Policy) -> Result<UploadStreamHandle, XetError>` | `upload_stream(&self, Option<String>, Sha256Policy).await` |
| `commit_blocking(&self) -> Result<CommitReport, XetError>` | `commit(&self).await` |

All blocking methods use `runtime.external_run_async_task()` internally and
**must not be called from within a tokio runtime** (they will panic).

---

## New Blocking Method on `DownloadGroup`

| New Method | Async Equivalent |
|---|---|
| `finish_blocking(self) -> Result<HashMap<Ulid, DownloadResult>, SessionError>` | `finish(self).await` |

`download_file_to_path` and `get_progress` were already synchronous — no
changes needed.

---

## Migration Guide

### Sync callers (old `UploadCommitSync` / `DownloadGroupSync` usage)

```rust
// Old
let commit: UploadCommitSync = session.new_upload_commit_blocking()?;
let handle = commit.upload_from_path(path, sha256)?;
let handle2 = commit.upload_bytes(bytes, sha256, name)?;
let (_h, cleaner) = commit.upload_file(name, size, sha256)?;
let results = commit.commit()?;

let group: DownloadGroupSync = session.new_download_group_blocking()?;
group.download_file_to_path(info, dest)?;
let results = group.finish()?;

// New (see update_260318_upload_handle_refactor.md for latest signatures)
let commit: UploadCommit = session.new_upload_commit_blocking()?;
let handle = commit.upload_from_path_blocking(path, sha256)?;
let handle2 = commit.upload_bytes_blocking(bytes, sha256, name)?;
let stream = commit.upload_stream_blocking(name, sha256)?;
let results = commit.commit_blocking()?;

let group: DownloadGroup = session.new_download_group_blocking()?;
group.download_file_to_path(info, dest)?;  // unchanged — already sync
let results = group.finish_blocking()?;
```

### Async callers

No changes needed. `UploadCommit` and `DownloadGroup` retain all their
existing async methods (`upload_from_path`, `upload_bytes`, `upload_stream`,
`commit`, `finish`). See `update_260318_upload_handle_refactor.md` for
subsequent renames (`upload_file` → `upload_stream`, etc.).

### Import changes

```rust
// Old
use xet::xet_session::{UploadCommitSync, DownloadGroupSync};
use xet::xet_session::sync::{UploadCommitSync, DownloadGroupSync};

// New — remove the imports entirely; use UploadCommit / DownloadGroup instead
use xet::xet_session::{UploadCommit, DownloadGroup};
```

---

## Bug Fix

`UploadCommitSync::upload_from_path` did not call `std::path::absolute()` on
the file path before dispatching, unlike the async `UploadCommit::upload_from_path`.
The new `upload_from_path_blocking` includes the `std::path::absolute()` call,
matching the async version's behavior.

---

## Files Changed

| File | Change |
|---|---|
| `xet_pkg/src/xet_session/upload_commit.rs` | Added `_blocking` methods, updated doc comments |
| `xet_pkg/src/xet_session/download_group.rs` | Added `finish_blocking`, updated doc comments |
| `xet_pkg/src/xet_session/session.rs` | Changed `new_*_blocking()` return types from `*Sync` to unified types |
| `xet_pkg/src/xet_session/mod.rs` | Removed `pub mod sync` and sync type re-exports, updated doc comments |
| `xet_pkg/src/xet_session/sync/` | Entire directory deleted |
| `xet_pkg/examples/example_sync.rs` | Updated to use `_blocking` methods |
