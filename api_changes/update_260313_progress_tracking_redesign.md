# Progress Tracking Redesign

**Date**: 2026-03-13

## Summary

The progress tracking system was redesigned from a push-based callback model to a
poll-based atomic model. All progress counters use `AtomicU64` for lock-free updates,
and progress is polled via snapshot reports rather than pushed through trait callbacks.

## New Core Types (`xet_data::progress_tracking`)

### `UniqueID` (replaces raw `Ulid` for tracking)
- Newtype wrapper over `u64` in `xet_data/src/progress_tracking/unique_id.rs`
- Used for all file-level task identifiers across the system
- Backed by an in-process atomic counter (`AtomicU64`) via `UniqueID::new()`
- Implements `Copy`, `Clone`, `Hash`, `Eq`, `Display`

### `ItemProgress`
- Per-item atomic progress counters: `total_bytes`, `bytes_completed`,
  `transfer_bytes`, `transfer_bytes_completed` (all `AtomicU64`)
- Also holds `id: UniqueID`, `name: Arc<str>`, `size_finalized: AtomicBool`

### `GroupProgress`
- Aggregate atomic counters + `tokio::sync::Mutex<HashMap<UniqueID, Arc<ItemProgress>>>` item map
- Speed calculation via sliding window (`Mutex<VecDeque<SpeedSample>>`)
- Factory method: `new_item(id, name) -> Arc<ItemProgressUpdater>`
- Reporting: `report() -> GroupProgressReport`, `item_report(id)`, `item_reports()`
- Debug verification: `assert_complete()`

### `ItemProgressUpdater`
- Holds `Arc<ItemProgress>` + `Arc<GroupProgress>`
- Methods: `update_item_size(total, is_final)`, `update_transfer_size(total)`,
  `report_bytes_completed(increment)`, `report_transfer_bytes_completed(increment)`
- Aliases: `report_bytes_written()` = `report_bytes_completed()`,
  `report_transfer_progress()` = `report_transfer_bytes_completed()`
- Uses `fetch_update` for safe atomic total updates; `fetch_add` for completions
- Ordering: group atomics updated with `Release` before item atomics
- Contains `debug_assert_le!` checks at every update point

### `GroupProgressReport` / `ItemProgressReport`
- Non-atomic snapshot structs with `#[cfg_attr(feature = "python", pyo3::pyclass(get_all))]`
- `GroupProgressReport`: `total_bytes`, `total_bytes_completed`,
  `total_bytes_completion_rate: Option<f64>`, `total_transfer_bytes`,
  `total_transfer_bytes_completed`, `total_transfer_bytes_completion_rate: Option<f64>`
- `ItemProgressReport`: `item_name: String`, `total_bytes: u64`, `bytes_completed: u64`

## Session API Changes

### `FileUploadSession`
- `new(config)` — no longer takes a progress updater parameter
- `dry_run(config)` — same
- `start_clean(name, size, sha256) -> (UniqueID, SingleFileCleaner)` — creates UniqueID
  internally, returns it; no longer takes `tracking_id: Ulid`
- `upload_files(files_and_sha256)` — iterator yields `(Path, Sha256Policy)` only, no Ulid
- New methods: `report()`, `item_report(id)`, `item_reports()`, `progress()`

### `FileDownloadSession`
- `new(config)` — no longer takes a progress updater parameter
- `from_client(config, client)` — same
- `download_file(file_info, path) -> Result<(UniqueID, u64)>` — returns UniqueID
- `download_to_writer(file_info, range, writer) -> Result<(UniqueID, u64)>` — same
- `download_stream(file_info) -> Result<(UniqueID, DownloadStream)>` — same
- New methods: `report()`, `item_report(id)`, `item_reports()`, `progress()`

### `data_client` functions
- `clean_file(session, path, sha256)` — removed `tracking_id` parameter
- `clean_bytes(session, bytes, sha256)` — removed `tracking_id` parameter
- Upload/download convenience functions that accept legacy callback updaters now
  live under `xet_pkg::legacy::data_client`, where callback updates are bridged
  from the polling model via `CallbackBridge`.

## `xet_pkg` (`hf-xet` crate) Changes

### Removed types from `xet_pkg::xet_session::progress`
- `GroupProgress` (the old push-model implementation)
- `TotalProgressSnapshot`, `FileProgress`, `FileProgressSnapshot`, `ProgressSnapshot`
- `TrackingProgressUpdater` impl for `GroupProgress`

### Updated types
- `TaskHandle.task_id` is now `UniqueID` (was `Ulid`)
- `TaskHandle` no longer holds `group_progress`
- `UploadCommit::get_progress()` is now **async**, returns `GroupProgressReport`
- `UploadCommit::commit()` returns `HashMap<UniqueID, UploadResult>` (was `Ulid`)
- `DownloadGroup::get_progress()` is now **async**, returns `GroupProgressReport`
- `DownloadGroup::finish()` returns `HashMap<UniqueID, DownloadResult>` (was `Ulid`)

### New re-exports from `xet_pkg::xet_session`
- `GroupProgressReport`, `ItemProgressReport`, `UniqueID` (from `xet_data`)

## `xet_data` Upload Tracking Changes

### `CompletionTracker`
- `new(group: Arc<GroupProgress>)` — takes GroupProgress instead of
  `Arc<dyn TrackingProgressUpdater>`
- `register_new_file(updater: Arc<ItemProgressUpdater>, n_bytes)` — takes updater
  instead of `(Ulid, name)`; returns only `CompletionTrackerFileId`
- All internal methods update atomics directly via `ItemProgressUpdater` and
  `GroupProgress` instead of building `ProgressUpdate` structs
- `register_xorb_upload_completion` gracefully handles already-completed xorbs (no panic)

## `git_xet` Changes
- `XetProgressUpdaterWrapper` removed (was unused after session no longer accepts updater)
- `FileUploadSession::new(config)` called without progress updater
- `clean_file` called without tracking_id

## Cargo Feature Changes
- `xet_data/Cargo.toml`: Added `pyo3 = { version = "0.26", optional = true }` and
  `python = ["dep:pyo3"]` feature
- `hf_xet/Cargo.toml`: switched from direct `xet-data` usage to `xet-pkg` legacy
  re-exports (`xet_pkg::legacy::*`) for callback-compatible APIs
- `xet_pkg/Cargo.toml`: removed `ulid`; task IDs now use `UniqueID`

## Legacy Code Still Present
- Legacy callback compatibility is preserved under `xet_pkg::legacy`:
  - `progress_tracking.rs` still defines `TrackingProgressUpdater`,
    `ProgressUpdate`, and `ItemProgressUpdate`
  - `callback_bridge.rs` adapts polling snapshots to legacy callback updates
  - `progress_verification_wrapper.rs` provides debug verification wrappers
  - `data_client.rs` keeps callback-oriented upload/download entry points

These legacy types are candidates for removal once `hf_xet` and `git_xet` fully
transition to the polling model.

## Supersedes / Conflicts
- `update_260316_unify_upload_download_sync_async.md`: blocking result map key
  types are now `UniqueID` for both `commit_blocking` and `finish_blocking`
  (not `Ulid`).

## Memory Ordering Invariants
- **Writes**: Group totals updated with `Release` before item totals. Totals updated
  before completions.
- **Reads** (report generation): Completions read with `Acquire` before totals, ensuring
  `completed <= total` in the snapshot.
- **Atomic read-modify-write**: `fetch_update` used for total updates (not swap+fetch_add)
  to avoid races under contention.
- **Debug asserts**: `debug_assert_le!(completed, total)` at every update and report point.
  `assert_eq!(completed, total)` at completion verification.
