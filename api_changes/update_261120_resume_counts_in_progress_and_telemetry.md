# Resume counts in group progress and download telemetry

**Date**: 2026-11-20
**Crates**: `xet-data` (`progress_tracking`, `telemetry`, `FileDownloadSession`), `hf-xet`, `hf_xet` (Python)

## What changed

A download group now reports how many files resumed from an existing local file, and how many of
those failed verification and were downloaded again from scratch. The counts are part of the group
progress report, so callers can read them, and they are sent in download telemetry.

### `GroupProgressReport` gained two fields — **breaking for struct literals**

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

A missing or empty local file is a fresh download and is not counted, even when `resume=True` was
requested. The counts are group-level, so they are unaffected by the fallback re-registering the
file's progress item.

### `GroupProgress` gained two recording methods

`record_resumed_file()` and `record_resume_fallback()`. `FileDownloadSession` calls them; other code
should not need to.

### Download telemetry: two new keys, schema version 3

| Key | Type | Source |
|---|---|---|
| `resumed_file_count` | `u64` | `GroupProgressReport::resumed_files` |
| `resume_fallback_count` | `u64` | `GroupProgressReport::resume_fallback_files` |

Both appear on every download document (terminal, heartbeat, and the `Drop` path); upload
documents are unchanged. A download document now carries 25 `metrics` keys, pinned by
`DOWNLOAD_KEYS` in `xet_data/src/telemetry/payload.rs`. `TELEMETRY_SCHEMA_VERSION` is bumped from 2
to 3. The `_count` suffix maps to `long` through the `ds-xet-telemetry` dynamic templates.

`DownloadMetrics::new` now takes the progress report as a second argument:
`DownloadMetrics::new(common, &progress)`.

### Interpreting byte counts on resumed downloads

A resumed file's `total_bytes` and `transfer_bytes` cover only the part downloaded, not the prefix
already on disk. When a resumed file falls back to a fresh download, the abandoned attempt's logical
bytes are withdrawn from the totals, so `total_bytes` counts that file once at its full size; its
transfer bytes stay counted, because that data was already fetched.
