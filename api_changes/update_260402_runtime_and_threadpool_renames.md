# API Update: Runtime and threadpool type renames plus runtime-selection behavior (2026-04-02)

## Overview

This update renames core runtime types in `xet_runtime` and changes how default runtime
construction behaves when called from inside an existing Tokio runtime.

---

## Breaking Changes

### Type renames

- `XetContext` is renamed to `XetRuntime`.
- `XetRuntime` (old threadpool wrapper type) is renamed to `XetThreadpool`.

Code importing these names from `xet_runtime::core` must be updated.

### Runtime construction behavior

`XetRuntime::default_with_config` now:

1. Detects a current Tokio handle with `Handle::try_current()`.
2. Uses that handle only if it satisfies runtime requirements.
3. Falls back to creating an owned threadpool otherwise.

This means callers running inside Tokio can now receive an External-mode runtime when
the host runtime is compatible, rather than always creating a new owned pool.

---

## Migration Guide

Update type names in imports and annotations:

```rust
// Before
use xet_runtime::core::{XetContext, XetRuntime};

// After
use xet_runtime::core::{XetRuntime, XetThreadpool};
```

Most call sites do not need behavioral changes, but code that assumes `default`/`default_with_config`
always creates an owned runtime should check `RuntimeMode` and adjust blocking/async usage
accordingly.

---

## Affected Files

- `xet_runtime/src/core/runtime.rs`
- `xet_runtime/src/core/threadpool.rs`
- `xet_runtime/src/core/mod.rs`
- `xet_runtime/src/lib.rs`
