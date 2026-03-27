# API Update: TaskRuntime cancellation propagation and final progress snapshot (2026-03-26)

## Overview

This update tightens cancellation behavior in `xet_pkg::xet_session` so abort
signals propagate through `TaskRuntime` cancellation tokens, and adjusts
download-group reporting so `XetDownloadGroupReport.progress` reflects the
post-finish state.

## Behavioral changes

### Token-driven cancellation propagation

- `XetFileUpload` and `XetFileDownload` cancellation paths now rely on
  `TaskRuntime::cancel_subtree()` token propagation instead of directly mutating
  per-handle state in the cancellation entrypoint.
- Background mapped upload/download tasks now observe the child
  `TaskRuntime` cancellation token and return `XetError::UserCancelled(...)`
  when cancelled.
- During token cancellation, the mapped task aborts its owned inner join handle
  before returning `UserCancelled`, avoiding detached inner task execution.

### Consistent finalizing bridge usage

- `XetFileUpload::finalize_ingestion()` (async) now goes through
  `TaskRuntime::bridge_async_finalizing(...)`.
- `XetFileUpload::finalize_ingestion_blocking()` now goes through
  `TaskRuntime::bridge_sync_finalizing(...)`.
- This aligns async/sync finalize semantics with other finalizing APIs and keeps
  terminal state transitions consistent (`Completed` / `UserCancelled` / `Error`).

### Download-group progress snapshot timing

- `XetDownloadGroup::finish()` and `finish_blocking()` now capture
  `XetDownloadGroupReport.progress` after all download tasks complete.
- `XetDownloadGroupReport.progress` now matches final completion state, rather
  than an early pre-finish snapshot.

## Compatibility notes

- No new symbols were added and no public type signatures changed.
- Callers may observe `UserCancelled` more consistently in cancellation races.
- `XetDownloadGroupReport.progress` should now be treated as a true final report.
