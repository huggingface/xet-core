# API Update: Split runtime execution from runtime context (2026-04-02)

## Current layout (follow-up renames)

Types are now `XetContext` (lightweight context: `runtime`, `config`, `common`) and `XetRuntime` (Tokio execution backend). Sources live in `xet_runtime/src/core/context.rs` and `xet_runtime/src/core/runtime.rs`.

## Overview

This update splits the old monolithic `xet_runtime::core::XetRuntime` into:

- `XetThreadpool`: execution backend and async/sync bridge entry points.
- `XetRuntime`: lightweight context containing `{ threadpool, config, common }`.

It also changes default runtime selection when called inside an existing Tokio runtime.

---

## Breaking Changes

### Runtime type split

- On `origin/main`, `XetRuntime` was the threadpool/executor wrapper.
- After this update, that functionality lives in `XetThreadpool`.
- `XetRuntime` is now a clonable context wrapper:
  - `pub threadpool: Arc<XetThreadpool>`
  - `pub config: Arc<XetConfig>`
  - `pub common: Arc<XetCommon>`

Code that previously called executor methods directly on `XetRuntime`
must now call them through `runtime.threadpool`.

### Runtime construction behavior

`XetRuntime::default()` now:

1. Detects a current Tokio handle with `Handle::try_current()`.
2. Uses that handle only if it satisfies runtime requirements (`handle_meets_requirements`).
3. Falls back to creating an owned threadpool otherwise.

This means callers running inside Tokio can receive an External-mode `XetThreadpool`
when the host runtime is compatible, rather than always creating a new owned pool.

### Removed/relocated exports

- `xet_runtime::core::check_sigint_shutdown` free function is no longer exported.
  Use `runtime.check_sigint_shutdown()` (or `runtime.threadpool.in_sigint_shutdown()` where needed).
- `xet_runtime::core::xet_config()` is no longer exported.
  Pass `&XetRuntime`/`&XetConfig` explicitly instead of relying on a process-global accessor.

---

## Migration Guide

Update execution calls and runtime construction:

```rust
// Before (origin/main)
use xet_runtime::core::XetRuntime;
let rt = XetRuntime::new().unwrap();
rt.bridge_sync(async { /* ... */ })?;

// After
use xet_runtime::core::{XetRuntime, XetThreadpool};
let cfg = xet_runtime::config::XetConfig::new();
let threadpool = XetThreadpool::new(&cfg)?;
let runtime = XetRuntime::new(cfg, threadpool);
runtime.threadpool.bridge_sync(async { /* ... */ })?;
```

Most call sites do not need behavioral changes, but code that assumes `XetRuntime::default()`
always creates an owned runtime should check `RuntimeMode` and adjust blocking/async usage
accordingly.

---

### Logging configuration

- `LoggingConfig::default_to_directory` is removed; use `LoggingConfig::from_directory(&XetConfig, ...)` instead.

### Chunk cache

- `DiskCache::initialize` and related APIs now take explicit `&XetConfig` instead of
  reading it from a global accessor.

---

## Affected Crates

- `xet_runtime` — `context.rs` (`XetContext`), `runtime.rs` (`XetRuntime` execution backend), `common.rs` (shared runtime-scoped caches), `config.rs` (removed global accessor), `mod.rs` (re-exports)
- `xet_core_structures` — shard manager and session directory APIs now take `&XetRuntime`
- `xet_client` — `http_client`, `RemoteClient`, `RetryWrapper`, `AdaptiveConcurrencyController`, chunk cache, auth, hub client, and simulation clients updated
- `xet_data` — `TranslatorConfig`, file upload/download sessions, file reconstruction, deduplication, shard interface
- `xet_pkg` — `XetSession`, legacy data client, and all session sub-modules thread `&XetRuntime`
- `git_xet` — LFS agent and token refresher use a process-global `XetRuntime`
- `hf_xet` — Python bindings use `get_or_init_runtime()` to manage a process-global `Arc<XetRuntime>`
- `simulation` — upload simulation uses `XetRuntime::from_external` for host runtime reuse
- `wasm/hf_xet_wasm` — file upload session and cleaner thread `XetRuntime` context
