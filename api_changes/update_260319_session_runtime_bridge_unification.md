# API Update: Session Runtime Bridge Unification (2026-03-19)

## Overview

`xet::xet_session` now uses a single builder entrypoint (`build`) and a
runtime-level bridge API (`bridge_async` / `bridge_sync`) to route async and
blocking calls consistently across owned and external tokio runtimes.

This is an API-breaking change for callers that used
`XetSessionBuilder::build_async().await`.

## Breaking Changes

### Removed method

- `XetSessionBuilder::build_async()` was removed.

### Changed `build()` behavior

- `XetSessionBuilder::build()` now auto-detects `tokio::runtime::Handle::try_current()`.
- If a suitable tokio runtime is detected (multi-thread + IO + time drivers),
  the session wraps that external runtime.
- If no suitable runtime is detected, the session creates an owned runtime.

This means `build()` called inside a normal tokio async context now produces an
External-mode session (where blocking APIs return `WrongRuntimeMode`).

## Runtime API Additions (`xet_runtime`)

### New public enum

- `xet_runtime::core::RuntimeMode` (re-exported from `xet_runtime::core`):
  - `Owned`
  - `External`

### New/updated bridge methods on `XetRuntime`

- `bridge_async(task_name, fut)`:
  - External mode: awaits directly on caller executor.
  - Owned mode: routes to owned runtime bridge.
- `bridge_sync(fut)`:
  - Owned mode: blocks caller thread and runs future.
  - External mode: returns `RuntimeError::InvalidRuntime`.

### New helper/constructor APIs

- `XetRuntime::handle_meets_requirements(&Handle)` (non-wasm)
- `XetRuntime::from_validated_external(handle, config)` (non-wasm)
- `XetRuntime::mode() -> RuntimeMode`

## Error Mapping Change

- `xet_runtime::RuntimeError` adds `InvalidRuntime(String)`.
- `xet::XetError` now maps `RuntimeError::InvalidRuntime(_)` to
  `XetError::WrongRuntimeMode(_)`.

## Session API Behavior Notes

- Blocking session/commit/group APIs now rely on `runtime.bridge_sync(...)` for
  mode validation instead of local `runtime_mode` checks.
- In External mode, blocking APIs return `WrongRuntimeMode`.
- In Owned mode, blocking APIs continue to work from non-tokio contexts.

## Migration Guide

### Replace `build_async()` calls

```rust
// Old
let session = XetSessionBuilder::new().build_async().await?;

// New
let session = XetSessionBuilder::new().build()?;
```

### Calling blocking APIs from async tokio contexts

- If `build()` runs inside a suitable tokio runtime, the session is External
  mode and blocking methods return `WrongRuntimeMode`.
- Use async methods (`new_upload_commit`, `new_download_group`,
  `upload_*`, `commit`, `finish`) in that context.

## Affected Areas

- `xet_pkg::xet_session::{session, upload_commit, download_group, mod}`
- `xet_runtime::core::{mod, runtime}`
- `xet_runtime::error`
- session/runtime tests and examples updated to `build()`
