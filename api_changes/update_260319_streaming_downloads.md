# Streaming download APIs

**Date**: 2026-03-19

## Summary

This update adds first-class stream download APIs in `xet_pkg::xet_session`.

It also renames the session download group type from `DownloadGroup` to
`FileDownloadGroup` and renames the corresponding session constructors.

## `xet_pkg::xet_session` API changes

### Type/constructor renames

- `DownloadGroup` -> `FileDownloadGroup`
- `XetSession::new_download_group()` -> `XetSession::new_file_download_group()`
- `XetSession::new_download_group_blocking()` ->
  `XetSession::new_file_download_group_blocking()`

### New streaming session APIs

- Ordered stream:
  - `XetSession::download_stream(file_info, range)` (async)
  - `XetSession::download_stream_blocking(file_info, range)` (sync)
  - Returns `XetDownloadStream`
- Unordered stream:
  - `XetSession::download_unordered_stream(file_info, range)` (async)
  - `XetSession::download_unordered_stream_blocking(file_info, range)` (sync)
  - Returns `XetUnorderedDownloadStream`

Both stream types support:

- `start()`
- async and blocking next-item APIs
- `cancel()`
- per-stream progress via `get_progress()` (returns `Option<ItemProgressReport>`)

Range semantics are source-file-relative (`Option<Range<u64>>`).

Stream abort callbacks are automatically unregistered on drop to prevent
accumulation in long-lived sessions.

## `xet_data` API/config changes

### New unordered stream path

- `FileDownloadSession::download_unordered_stream(file_info, source_range)`
- `processing::mod` now re-exports `UnorderedDownloadStream`

### `DataWriter` trait contract update

- `finish` now consumes the writer:
  - `async fn finish(self: Box<Self>) -> Result<u64>`

### Stream abort callback API

- `FileDownloadSession::register_stream_abort_callback` now takes `(UniqueID, callback)`.
- New: `FileDownloadSession::unregister_stream_abort_callback(UniqueID)`.
- Callbacks are stored in a `HashMap<UniqueID, _>` instead of a `Vec`.

## Migration notes

- Downstream code must update symbol/method names from `DownloadGroup` /
  `new_download_group*` to `FileDownloadGroup` /
  `new_file_download_group*`.
- Consumers that need chunk-level streaming can migrate from group/file APIs to
  the new stream APIs when appropriate.
