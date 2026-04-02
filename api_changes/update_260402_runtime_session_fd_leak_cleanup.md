# API Update: Runtime/session FD lifecycle cleanup (2026-04-02)

## Overview

Currently, a few ownership patterns in runtime/session internals could keep Tokio-backed
resources alive longer than intended, which in turn could keep file descriptors open
across repeated session lifecycles.  This update tightens teardown behavior and breaks
those retention paths.

There are no intended user-facing behavior changes to upload/download semantics.

---

## Public API surface

### Added module in `xet_runtime`

`xet_runtime` now publicly exports:

```rust
pub mod fd_diagnostics;
```

This module provides FD counting/diagnostic helpers used by integration tests.

### No removed/renamed public methods

No public structs/functions were removed or renamed in this update.

---

## Internal merge-sensitive changes

These are internal but are likely to conflict during rebases/merges:

- `xet_pkg::xet_session::XetSessionInner` no longer tracks:
  - `active_upload_commits`
  - `active_file_download_groups`
- Session abort now relies on `TaskRuntime` cancellation-token subtree propagation for
  upload commits and file download groups.
- `xet_pkg::xet_session::XetFileDownloadGroup` now owns a `session: XetSession` field
  directly; `XetFileDownloadGroupInner` no longer stores `session`.
- `xet_runtime::core::XetRuntime` thread-local runtime reference uses `Weak<XetRuntime>`
  instead of `Arc<XetRuntime>` to avoid runtime/thread reference cycles.
- `XetRuntime::Drop` teardown logic is consolidated to one owned-runtime shutdown path.

---

## Notes for downstream maintainers

- If downstream tests or internal forks relied on session-internal registration maps for
  asserting lifecycle state, migrate those checks to observable behavior
  (task status/abort behavior/resource release) instead.
- If you have local debug tooling for FD leaks, prefer the new
  `xet_runtime::fd_diagnostics` helpers.
