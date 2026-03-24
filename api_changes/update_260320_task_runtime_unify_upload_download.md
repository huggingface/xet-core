## xet_session task runtime unification and download task refactor

This change set completes a structural pass that aligns upload and download orchestration around a shared runtime/cancellation utility and thinner public wrappers.

### Core architectural updates

- Added `TaskRuntime` in `xet_pkg::xet_session`:
  - owns `Arc<XetRuntime>` plus `tokio_util::sync::CancellationToken`,
  - exposes four bridge methods: `bridge_async`, `bridge_sync`, `bridge_async_finalizing`, `bridge_sync_finalizing`,
  - wraps bridged work in `tokio::select!` and returns `UserCancelled` when the cancellation token fires,
  - tracks child runtimes via `Mutex<Vec<Weak<TaskRuntime>>>`,
  - `TaskRuntimeState` is a flat enum: `Running`, `Finalizing`, `Completed`, `Error(String)`, `UserCancelled`.

- SIGINT handling is fully owned by `XetRuntime`:
  - `bridge_sync`/`bridge_async` on `XetRuntime` check `in_sigint_shutdown()` at entry and return `RuntimeError::KeyboardInterrupt`,
  - `check_sigint_shutdown()` returns `RuntimeError::KeyboardInterrupt`.

- Session/commit/group/task cancellation follows a token tree:
  - `XetSession` owns root runtime/token,
  - `UploadCommit` and `FileDownloadGroup` receive child runtimes,
  - per-upload/per-download handles receive child runtimes,
  - aborting a parent cancels descendants and sets `UserCancelled` state.

### Error variant changes in `XetError`

- **Removed**: `Aborted`, `AlreadyCommitted`, `AlreadyFinished`.
- **Added**: `KeyboardInterrupt` (SIGINT, maps to `PyKeyboardInterrupt`), `UserCancelled(String)` (explicit user abort), `PreviousTaskError(String)` (cached error from prior failed operation), `AlreadyCompleted` (replaces all "already committed/finished/finalizing" cases).
- **Removed**: `Clone` derive from `XetError`.
- `RuntimeError::KeyboardInterrupt` added and mapped to `XetError::KeyboardInterrupt`.
- `RuntimeError::TaskCanceled` also maps to `XetError::KeyboardInterrupt` (only source is runtime shutdown).

### Wrapper/inner contract alignment

- Public upload/download methods are thin runtime-bridge wrappers.
- Terminal operations (`commit`, `finish`) use `bridge_async_finalizing`/`bridge_sync_finalizing` which handle the `Running` -> `Finalizing` -> `Completed`/`Error` state transition.
- Non-terminal operations use `bridge_async`/`bridge_sync` which pre-check state and update on error.
- Result caching for upload handles uses `OnceLock<FileMetadata>` (success only); `try_finish()` returns `Option<FileMetadata>`.

### Download bespoke task handles

- Download group task tracking uses bespoke `DownloadTaskHandle` with per-task status, stable task id, per-task result access, and handle-local cancel.

### xet_data boundary extraction

- `FileUploadCoordinator` and `FileDownloadCoordinator` in `xet_data::processing` as data-layer orchestration wrappers.

### Structured tracing

- `info!` for lifecycle events: session/commit/group creation, finish, abort, cancellation.
- `debug!` for incremental operations: stream writes, download chunk retrieval.
- `error!` for unexpected failures (not cancellations/aborts).
- All log entries include relevant IDs (session_id, commit_id, group_id, task_id) and user-provided data.

### Behavioral notes

- User abort produces `XetError::UserCancelled(msg)` (previously `Aborted`).
- SIGINT produces `XetError::KeyboardInterrupt` (previously `Cancelled`).
- Repeated calls to terminal operations produce `XetError::AlreadyCompleted`.
- After an error in a terminal operation, subsequent calls produce `XetError::PreviousTaskError(msg)`.
- `RuntimeMode` checks removed from session code; enforcement is in `XetRuntime::bridge_sync`.
