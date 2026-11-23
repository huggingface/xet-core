# Resumable downloads: `XetFileDownloadGroup::start_download_file`; `download_file_to_path` deprecated

**Date**: 2026-11-23
**Crates**: `hf-xet` (`xet_session::XetFileDownloadGroup`), `xet-data` (`FileDownloadSession`, `DataError`),
`hf_xet` (Python)

## What changed

File downloads can resume from a partial local file. The `hf-xet` API gains new methods that take a
`resume` flag, and the old methods are deprecated without changing their signatures.

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
- A non-empty `dest_path` is treated as a partial download: only the bytes after its current length
  are downloaded and appended.
- After the append, the file's xet hash is computed and compared with `file_info`. On a mismatch,
  meaning the local prefix did not belong to this file, a warning is logged and the file is
  downloaded again from scratch. This hashes the whole file, so a resume costs a full local read.
- A file that is already complete reconstructs zero bytes, then is verified like any other resume.

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

### `xet-data`: changed in place, no deprecation period

`xet-data` is meant to be used through `hf-xet`, so its changes are not staged:

- `FileDownloadSession::download_file` and `download_file_background` take a third argument,
  `resume: bool`. Pass `false` for the previous behavior.
- `GroupProgress::remove_item(id)` is new. It unregisters an item and subtracts its logical bytes
  from the group totals.
