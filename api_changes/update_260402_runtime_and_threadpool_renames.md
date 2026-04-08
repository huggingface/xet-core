# API Update: Split runtime execution from runtime context (2026-04-02)

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
must now call them through `ctx.threadpool`.

### Runtime construction behavior

`XetRuntime::default()` now:

1. Detects a current Tokio handle with `Handle::try_current()`.
2. Uses that handle only if it satisfies runtime requirements (`handle_meets_requirements`).
3. Falls back to creating an owned threadpool otherwise.

This means callers running inside Tokio can receive an External-mode `XetThreadpool`
when the host runtime is compatible, rather than always creating a new owned pool.

### Removed/relocated exports

- `xet_runtime::core::check_sigint_shutdown` free function is no longer exported.
  Use `ctx.check_sigint_shutdown()` (or `ctx.threadpool.in_sigint_shutdown()` where needed).
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
let ctx = XetRuntime::new(cfg, threadpool);
ctx.threadpool.bridge_sync(async { /* ... */ })?;
```

Most call sites do not need behavioral changes, but code that assumes `XetRuntime::default()`
always creates an owned runtime should check `RuntimeMode` and adjust blocking/async usage
accordingly.

---

## Affected Files

- `xet_runtime/src/core/runtime.rs` (new lightweight context API)
- `xet_runtime/src/core/threadpool.rs` (moved executor behavior)
- `xet_runtime/src/core/mod.rs` (re-exports changed)
- `xet_runtime/src/core/config.rs` (removed global config accessor)
- Representative downstream updates threading `&XetRuntime` through constructors:
  - `xet_core_structures/src/metadata_shard/*`
  - `xet_data/src/file_reconstruction/*`
  - `xet_client/src/common/http_client.rs`
