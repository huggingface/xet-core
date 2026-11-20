# Resumable downloads: `XetFileDownloadGroup::start_download_file`; `download_file_to_path` deprecated

**Date**: 2026-11-23
**Crates**: `hf-xet` (`xet_session::XetFileDownloadGroup`), `xet-data` (`FileDownloadSession`, `progress_tracking`,
`telemetry`), `hf_xet` (Python)

## What changed

File downloads can resume from a partial local file. The `hf-xet` API gains new methods that take a
`resume` flag, and the old methods are deprecated without changing their signatures. Group progress
reports and download telemetry count resumed files and files downloaded again after a failed resume.

### New: `XetFileDownloadGroup::start_download_file` / `start_download_file_blocking`

```rust
pub async fn start_download_file(&self, file_info: XetFileInfo, dest_path: PathBuf, resume: bool)
    -> Result<XetFileDownload, XetError>;
pub fn start_download_file_blocking(&self, file_info: XetFileInfo, dest_path: PathBuf, resume: bool)
    -> Result<XetFileDownload, XetError>;
```

The same as `download_file_to_path` / `download_file_to_path_blocking` plus `resume`. Like them, they
return a `XetFileDownload` handle immediately; the download runs in the background and `finish`
waits for it. The name matches the Python method, `XetFileDownloadGroup.start_download_file`.

With `resume = false`, `dest_path` is overwritten, exactly as before.

With `resume = true`:

- A missing or empty `dest_path` is a fresh download.
- A non-empty `dest_path` shorter than `file_info`'s size is treated as a partial download: only the
  bytes after its current length are downloaded and appended.
- A `dest_path` at least as long as `file_info`'s size downloads nothing and goes straight to
  verification. An already-complete file therefore passes without network transfer; a longer one
  fails verification and is downloaded again.
- After the append, the file's xet hash is computed and compared with `file_info`. On a mismatch,
  meaning the local prefix did not belong to this file, a warning is logged and the file is
  downloaded again from scratch, replacing the local content. This hashes the whole file, so a
  resume costs a full local read.

A bad local file is never an error: every mismatch ends in a fresh download.

Progress for a resumed file covers only the downloaded part: its `total_bytes` is the file size
minus the local prefix. When verification fails and the file is downloaded again, the abandoned
attempt's logical bytes are removed from the group totals, so the file is counted once at its full
size; the bytes already fetched for it stay in the transfer totals.

### Deprecated: `download_file_to_path` / `download_file_to_path_blocking`

Signatures and behavior are unchanged; both carry `#[deprecated(since = "1.9.0")]`. They are
equivalent to the new methods with `resume = false`.

Migration:

```rust
// Before
let handle = group.download_file_to_path(info, dest).await?;
let handle = group.download_file_to_path_blocking(info, dest)?;

// After: same behavior
let handle = group.start_download_file(info, dest, false).await?;
let handle = group.start_download_file_blocking(info, dest, false)?;

// After: resume a partial file
let handle = group.start_download_file(info, dest, true).await?;
```

Builds that deny warnings, such as this workspace's `strict` feature, fail on calls to the
deprecated methods until they are migrated. Every caller in this repository has been migrated.

### Python: `start_download_file` gained an optional `resume` argument

```python
group.start_download_file(file_info, dest_path, resume=False)
```

Defaults to `False`, so existing calls are unaffected. `resume=True` behaves as described above.

### Resume counts in `GroupProgressReport`: **breaking for struct literals**

```rust
pub struct GroupProgressReport {
    // ... existing fields ...
    /// Downloads that started from an existing non-empty local file. Always 0 for uploads.
    pub resumed_files: u64,
    /// Resumed downloads that failed hash verification and were redone from scratch.
    /// A subset of `resumed_files`. Always 0 for uploads.
    pub resume_fallback_files: u64,
    pub shard: Option<ShardUploadProgressReport>,
}
```

`GroupProgressReport` is not `#[non_exhaustive]`, so code that builds one with a struct literal must
add both fields (`0` is correct when nothing was resumed), or use `..Default::default()`. In this
repository that was only `xet_pkg/src/legacy/progress_tracking/callback_bridge.rs` (a test helper).

The report is a `pyclass(get_all)`, so Python callers see `report.resumed_files` and
`report.resume_fallback_files` on `XetFileDownloadGroup.progress()`, in progress callbacks, and in
the group's final report. Upload reports carry the same attributes with value `0`.

A missing or empty local file is a fresh download and is not counted, even with `resume = true`.
The counts are group-level, so the fallback's `remove_item` does not affect them.

### Download telemetry: two new keys, schema version 3

| Key | Type | Source |
|---|---|---|
| `resumed_file_count` | `u64` | `GroupProgressReport::resumed_files` |
| `resume_fallback_count` | `u64` | `GroupProgressReport::resume_fallback_files` |

Both appear on every download document (terminal, heartbeat, and the `Drop` path); upload
documents are unchanged. A download document now carries 25 `metrics` keys, pinned by
`DOWNLOAD_KEYS` in `xet_data/src/telemetry/payload.rs`. `TELEMETRY_SCHEMA_VERSION` is bumped from 2
to 3. The `_count` suffix maps to `long` through the `ds-xet-telemetry` dynamic templates.

Byte metrics on a resumed download follow the progress semantics above: `total_bytes` and
`transfer_bytes` exclude the prefix already on disk, and after a fallback the file's logical bytes
are counted once at full size while the abandoned attempt's transfer bytes stay counted.

### `xet-data`: changed in place, no deprecation period

`xet-data` is meant to be used through `hf-xet`, so its changes are not staged:

- `FileDownloadSession::download_file` and `download_file_background` take a third argument,
  `resume: bool`. Pass `false` for the previous behavior.
- `GroupProgress::remove_item(id)` is new. It unregisters an item and subtracts its logical bytes
  from the group totals.
- `GroupProgress::inc_resumed_files()` and `inc_resume_fallback()` are new. `FileDownloadSession`
  calls them; other code should not need to.
- `DownloadMetrics::new` takes the progress report as a second argument:
  `DownloadMetrics::new(common, &progress)`.
