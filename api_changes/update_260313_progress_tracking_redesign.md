# Progress tracking redesign

**Date**: 2026-03-13

## Summary

Progress tracking has been moved to a polling/snapshot model backed by atomics, and
task identifiers now use `UniqueID` (a `u64`-backed ID) instead of `Ulid`.

Legacy callback consumers are still supported through bridge adapters in
`xet_pkg::legacy::progress_tracking`.

## Core type changes

### `UniqueID`

- Canonical ID type is `xet_runtime::utils::UniqueId` (newtype over `u64`).
- `xet_data::progress_tracking` re-exports this as `UniqueID`:
  `pub use xet_runtime::utils::UniqueId as UniqueID`.
- Used as task/file tracking IDs across `xet_data`, `xet_pkg`, `hf_xet`, and `git_xet`.

### `xet_data::progress_tracking`

- Legacy tracker modules were removed (`aggregator`, `download_tracking`, `no_op_tracker`).
- `GroupProgress` and `ItemProgress` are the authoritative progress model.
- `GroupProgress` uses:
  - group-level atomics for totals/completions,
  - `std::sync::Mutex<HashMap<UniqueID, Arc<ItemProgress>>>` for per-item registry.
- `ItemProgressUpdater` provides write APIs:
  - `update_item_size(total, is_final)`
  - `update_transfer_size(total)`
  - `report_bytes_completed(increment)` (alias: `report_bytes_written`)
  - `report_transfer_bytes_completed(increment)` (alias: `report_transfer_progress`)
- Snapshot structs:
  - `GroupProgressReport`
  - `ItemProgressReport`

### Speed estimation model

- Speed estimation is no longer a sliding window.
- `GroupProgress` now uses `speed_tracker::SpeedTracker`, which computes smoothed
  rates using EWMA (`ExpWeightedMovingAvg`).
- Rates are reported as:
  - `total_bytes_completion_rate: Option<f64>`
  - `total_transfer_bytes_completion_rate: Option<f64>`
- Rates remain `None` until a configurable minimum observation count is reached.

## Configuration changes (`xet_runtime::config::data`)

New settings:

- `progress_update_speed_sampling_window: Duration`  
  Env: `HF_XET_DATA_PROGRESS_UPDATE_SPEED_SAMPLING_WINDOW`  
  Meaning: EWMA half-life for speed estimation.
- `progress_update_speed_min_observations: u32`  
  Env: `HF_XET_DATA_PROGRESS_UPDATE_SPEED_MIN_OBSERVATIONS`  
  Meaning: minimum observations before rates are exposed.

## `xet_data::processing` API changes

### `FileUploadSession`

- `new(config)` and `dry_run(config)` no longer accept external progress updater traits.
- `start_clean(...)` no longer accepts `tracking_id`; it returns the generated ID:
  - `start_clean(tracking_name: Option<Arc<str>>, size: u64, sha256: Sha256Policy) -> Result<(UniqueID, SingleFileCleaner)>`
- New session progress accessors:
  - `progress()`
  - `report()`
  - `item_report(id)`
  - `item_reports()`
- New task spawn helpers:
  - `spawn_upload_from_path(...) -> Result<(UniqueID, JoinHandle<Result<XetFileInfo>>)>`
  - `spawn_upload_bytes(...) -> Result<(UniqueID, JoinHandle<Result<XetFileInfo>>)>`

### `FileDownloadSession`

- `new(config)` no longer accepts external progress updater traits.
- `from_client` signature is now:
  - `from_client(client: Arc<dyn Client>) -> Arc<Self>`
- Download calls now return generated IDs:
  - `download_file(...) -> Result<(UniqueID, u64)>`
  - `download_to_writer(...) -> Result<(UniqueID, u64)>`
  - `download_stream(...) -> Result<(UniqueID, DownloadStream)>`
  - `download_file_background(...) -> Result<(UniqueID, JoinHandle<Result<u64>>)>`
- New progress accessors:
  - `report()`
  - `item_report(id)`
  - `item_reports()`

### `xet_data::processing::data_client`

- `clean_file(...)` and `clean_bytes(...)` removed explicit `tracking_id` arguments.
- Callback-oriented upload/download wrappers were moved out of `xet_data` and are
  now provided by `xet_pkg::legacy::data_client`.

## `xet_pkg` API changes

- New `xet_pkg::legacy` module added for callback-compatible APIs.
- `xet_pkg::xet_session::progress` removed; progress/task types live in:
  - `xet_pkg::xet_session::tasks`
  - `xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID}`
- `TaskHandle.task_id` now uses `UniqueID`.
- `UploadCommit::commit` / `commit_blocking` result maps now keyed by `UniqueID`.
- `DownloadGroup::finish` / `finish_blocking` result maps now keyed by `UniqueID`.
- `UploadCommit::get_progress()` and `DownloadGroup::get_progress()` are synchronous
  snapshot reads returning `GroupProgressReport`.

## Legacy compatibility path

- Callback interfaces are preserved in `xet_pkg::legacy::progress_tracking`:
  - `TrackingProgressUpdater`
  - `ProgressUpdate`
  - `ItemProgressUpdate`
  - `GroupProgressCallbackUpdater`
  - `ItemProgressCallbackUpdater`
- `hf_xet` and `git_xet` were migrated to consume the legacy bridge layer
  (`xet_pkg::legacy::*`) instead of direct old `xet_data` callback APIs.

## Supersedes / follow-on

- This change supersedes `Ulid` task/result map keys in session APIs with `UniqueID`.
- Any downstream code matching on `Ulid` task IDs must migrate to `UniqueID`.
