# API Update: Split runtime execution from runtime context (2026-04-02)

## Overview

This update splits the old monolithic `xet_runtime::core::XetRuntime` into:

- `XetRuntime`: execution backend (Tokio thread pool or external handle wrapper) with async/sync bridge entry points. Lives in `xet_runtime/src/core/runtime.rs`.
- `XetContext`: lightweight clonable context containing `{ runtime: Arc<XetRuntime>, config: Arc<XetConfig>, common: Arc<XetCommon> }`. Lives in `xet_runtime/src/core/context.rs`.

It also changes default context selection when called inside an existing Tokio runtime.

---

## Breaking Changes

### Runtime type split

- On `origin/main`, `XetRuntime` was the combined threadpool/executor/config/cache wrapper.
- After this update, `XetRuntime` is the pure execution backend.
- `XetContext` is the new clonable context wrapper:
  - `pub runtime: Arc<XetRuntime>`
  - `pub config: Arc<XetConfig>`
  - `pub common: Arc<XetCommon>`

Code that previously accessed config or common state on `XetRuntime`
must now go through an `XetContext` and access `ctx.config`, `ctx.common`,
or `ctx.runtime` for execution methods.

### Context construction behavior

`XetContext::default()` now:

1. Checks the thread-local for an existing owned `XetRuntime` (TLS reuse).
2. Detects a current Tokio handle with `Handle::try_current()` and uses it only if it satisfies runtime requirements (`handle_meets_requirements`).
3. Falls back to creating a new owned `XetRuntime` otherwise.

This means callers running inside a compatible Tokio runtime can receive an External-mode `XetRuntime`
rather than always creating a new owned pool.

### Removed/relocated exports

- `xet_runtime::core::check_sigint_shutdown` free function is no longer exported.
  Use `ctx.check_sigint_shutdown()` (or `ctx.runtime.in_sigint_shutdown()` where needed).
- `xet_runtime::core::xet_config()` is no longer exported.
  Pass `&XetContext`/`&XetConfig` explicitly instead of relying on a process-global accessor.
- `XORB_CUT_THRESHOLD_BYTES` and `XORB_CUT_THRESHOLD_CHUNKS` global constants are removed;
  compute thresholds at the point of use via the explicit config (`ctx.config.xorb.simulation_max_bytes`, etc.).

---

## Migration Guide

Update execution calls and context construction:

```rust
// Before (origin/main)
use xet_runtime::core::XetRuntime;
let rt = XetRuntime::new().unwrap();
rt.bridge_sync(async { /* ... */ })?;

// After
use xet_runtime::core::{XetContext, XetRuntime};
use xet_runtime::config::XetConfig;
let config = XetConfig::new();
let runtime = XetRuntime::new(&config)?;
let ctx = XetContext::new(config, runtime);
ctx.runtime.bridge_sync(async { /* ... */ })?;

// Or use the default constructor:
let ctx = XetContext::default()?;
ctx.runtime.bridge_sync(async { /* ... */ })?;
```

Most call sites now accept `&XetContext` and reach the execution backend via `ctx.runtime`.
Code that assumes `XetContext::default()` always creates an owned runtime should check
`ctx.runtime.mode()` (`RuntimeMode::Owned` vs `RuntimeMode::External`) and adjust
blocking/async usage accordingly.

---

### Logging configuration

- `LoggingConfig::default_to_directory` is removed; use `LoggingConfig::from_directory(&XetConfig, ...)` instead.

### Chunk cache

- `DiskCache::initialize` and related APIs now take explicit `&XetConfig` instead of
  reading it from a global accessor.

### Shared caches

- Subsystem caches (shard file manager, shard file cache, reqwest client) are moved from
  process-global statics into `XetCommon::cache_get_or_create`, scoped to the context lifetime.

---

## Affected Crates

- `xet_runtime` — `context.rs` (`XetContext`), `runtime.rs` (`XetRuntime` execution backend), `common.rs` (shared runtime-scoped caches), `config.rs` (removed global accessor), `mod.rs` (re-exports)
- `xet_core_structures` — shard manager and session directory APIs now take `&XetContext`
- `xet_client` — `http_client`, `RemoteClient`, `RetryWrapper`, `AdaptiveConcurrencyController`, chunk cache, auth, hub client, and simulation clients updated
- `xet_data` — `TranslatorConfig`, file upload/download sessions, file reconstruction, deduplication, shard interface
- `xet_pkg` — `XetSession`, legacy data client, and all session sub-modules thread `&XetContext`
- `git_xet` — LFS agent and token refresher use a process-global `XetContext`
- `hf_xet` — Python bindings use `get_or_init_runtime()` to manage a process-global `Arc<XetContext>`
- `simulation` — upload simulation uses `XetContext::from_external` for host runtime reuse
- `wasm/hf_xet_wasm` — file upload session and cleaner thread `XetContext` context
