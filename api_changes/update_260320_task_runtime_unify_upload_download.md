## xet_session task runtime unification and download task refactor

This change set completes a structural pass that aligns upload and download orchestration around a shared runtime/cancellation utility and thinner public wrappers.

### Core architectural updates

- Added `TaskRuntime` in `xet_pkg::xet_session`:
  - owns `Arc<XetRuntime>` plus `tokio_util::sync::CancellationToken`,
  - exposes `bridge_async` and `bridge_sync`,
  - wraps bridged work in `tokio::select!` and returns runtime task-cancel errors when canceled,
  - tracks child runtimes via `Mutex<Vec<Weak<TaskRuntime>>>`,
  - supports recursive state propagation (`Alive`, `Finalizing`, `Finished`, `Aborted`) and subtree cancellation.

- Session/commit/group/task cancellation now follows a token tree:
  - `XetSession` owns root runtime/token,
  - `UploadCommit` and `FileDownloadGroup` receive child runtimes,
  - per-upload/per-download handles receive child runtimes,
  - aborting a parent cancels descendants.

### Wrapper/inner contract alignment

- Public upload/download methods were flattened into thin runtime-bridge wrappers.
- State gating and transition logic moved to inner methods and `TaskRuntime` state.
- Download path now mirrors upload wrapper flow (`Arc` clone + bridge + inner call).

### Download bespoke task handles

- Download group task tracking no longer uses the generic shared task-handle abstraction.
- `FileDownloadGroup` now owns a bespoke `DownloadTaskHandle` with:
  - per-task status,
  - stable task id,
  - per-task result access,
  - handle-local cancel behavior.

### xet_data boundary extraction

- Added `FileUploadCoordinator` and `FileDownloadCoordinator` in `xet_data::processing` as data-layer orchestration wrappers around session primitives.
- `xet_session` upload/download internals now call these coordinator APIs instead of directly wiring all session operations themselves.

### Behavioral notes

- In aborted/canceled races, some API calls that previously returned only `Aborted` may now return cancellation errors (`Cancelled`) when cancellation is observed first by bridged runtime selection.
- Existing roundtrip behavior and runtime-mode compatibility remain preserved by tests.
