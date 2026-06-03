# API Update: `XetSessionBuilder` adopts the `with_...` pattern and gains `with_context` (2026-06-01)

## Overview

`XetSessionBuilder` now uses a uniform `with_...` configuration pattern. The
`new_with_config` constructor is removed in favor of `new().with_config(...)`, and a
new `with_context` method lets a session reuse an existing `XetContext` -- sharing its
runtime, configuration, and cached shard-file managers so the local shard cache is not
re-scanned and re-indexed on every new session.

---

## Breaking Changes

### Removed `XetSessionBuilder::new_with_config`

```rust
// Before
let session = XetSessionBuilder::new_with_config(config).build()?;

// After (with_config accepts XetConfig or Arc<XetConfig>)
let session = XetSessionBuilder::new().with_config(config).build()?;
```

`new()` now takes no configuration and all configuration flows through `with_...`
methods.

---

## New capability: `with_context`

Build a session on top of an existing `XetContext` (e.g. obtained from another session
via the new `XetSession::context`). The new session shares the context's runtime,
config, and `common` state — including the cached `ShardFileManager` — so repeated
sessions avoid re-scanning the shard cache directory from disk.

```rust
let first = XetSessionBuilder::new().build()?;

// Reuses the runtime, config, and shard-file-manager cache from `first`.
let second = XetSessionBuilder::new()
    .with_context(first.context().clone())
    .build()?;
```

When a context is supplied, `with_runtime` is ignored. `with_config` takes precedence:
its configuration replaces the context's while the runtime and shared state (`common`,
including caches) are still reused. Since `common` is still shared, construction-time
settings stored there, such as semaphores, are not re-applied from the replacement
configuration.

```rust
// Reuse the runtime + caches from `a`, but with a tweaked config.
let b = XetSessionBuilder::new()
    .with_context(a.context())
    .with_config(custom_cfg)
    .build()?;
```

`XetContext` and `XetRuntime` are now re-exported from `xet::xet_session`.

### Accessors for recycling

`XetSession` exposes accessors so an existing session's pieces can be reused when
building another session:

```rust
pub fn config(&self) -> Arc<XetConfig>;               // now returns an owned Arc (was &XetConfig)
pub fn context(&self) -> XetContext;                  // new (owned; Arc-backed clone)
pub fn runtime(&self) -> Arc<XetRuntime>;             // new
```

`config()` and `runtime()` return `Arc` clones, and `context()` returns an owned
`XetContext` clone. `with_config` accepts `impl Into<Arc<XetConfig>>`, so it takes either
a `XetConfig` or an `Arc<XetConfig>` (e.g. the value returned by `config()`).

```rust
// Share everything, including the shard-file-manager cache:
let b = XetSessionBuilder::new().with_context(a.context()).build()?;

// Or share just the thread pool (independent config + caches):
let b = XetSessionBuilder::new()
    .with_config(a.config())
    .with_runtime(a.runtime())
    .build()?;
```

---

## Full builder chain

```rust
let session = XetSessionBuilder::new()
    .with_config(cfg)            // overrides the context config if with_context is set
    .with_context(ctx)           // reuse runtime + config + caches
    .with_runtime(runtime)       // ignored if with_context is set
    .build()?;
```

---

## `XetContext` constructors accept `impl Into<Arc<XetConfig>>`

`XetContext::new`, `XetContext::with_config`, and `XetContext::from_external` now take
`impl Into<Arc<XetConfig>>` instead of `XetConfig`. This is source-compatible — existing
callers passing an owned `XetConfig` continue to work — and lets an `Arc<XetConfig>` be
threaded straight through without an extra clone (the context stores `Arc<XetConfig>`).

A new `XetContext::with_new_config(&self, config)` returns a clone of the context with
the configuration replaced while sharing the runtime and `common` state. This backs the
`with_context` + `with_config` precedence in `XetSessionBuilder`.

---

## Affected Files

- `xet_pkg/src/xet_session/session.rs` — builder fields are now `Option`; `new_with_config` removed; `with_config`/`with_context`/`with_runtime` added; `build` resolution updated; `XetSession::context()` / `runtime()` accessors added; `config()` returns `Arc<XetConfig>`
- `xet_pkg/src/xet_session/mod.rs` — re-exports `XetContext` and `XetRuntime`
- `xet_runtime/src/core/context.rs` — `new`/`with_config`/`from_external` accept `impl Into<Arc<XetConfig>>`; adds `with_new_config`
- `hf_xet/src/py_xet_session.rs` — updated to `new().with_config(...)`
