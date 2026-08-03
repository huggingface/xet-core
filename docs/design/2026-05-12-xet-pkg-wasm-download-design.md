# xet_pkg WASM Download Support

**Issue:** [#840](https://github.com/huggingface/xet-core/issues/840)  
**Date:** 2026-05-12  
**Status:** Design approved

## Goal

Make the `hf-xet` crate (`xet_pkg`) compile and function on `wasm32-unknown-unknown` so that `hf-hub` — which depends on it — can build for WASM targets. Download is the immediate priority; upload is deferred to a separate effort.

## Scope

**In scope:**
- `xet_pkg` compiles cleanly for `wasm32-unknown-unknown`
- `XetDownloadStreamGroup` (async streaming bytes) works on WASM
- New `wasm/hf_xet_wasm_download/` crate with `#[wasm_bindgen]` wrapper + manual test page

**Out of scope:**
- Upload on WASM (future PR)
- Removing `hf_xet_wasm` (after stabilization)
- Changes to `xet_runtime`, `xet_client`, `xet_data`, or `xet_core_structures`

## Approach

Add `#[cfg(target_family = "wasm")]` / `#[cfg(not(target_family = "wasm"))]` gates to `xet_pkg`, following the exact pattern already used by the sub-crates. No structural reorganization, no new trait abstractions.

### WASM API surface (what compiles on WASM)

```
XetSessionBuilder::build() → XetSession
XetSession::new_download_stream_group() → XetDownloadStreamGroupBuilder
  .with_endpoint(...)
  .with_token_info(...)
  .with_token_refresh_url(...)
  .build().await → XetDownloadStreamGroup
XetDownloadStreamGroup::download_stream(file_info, range).await → XetDownloadStream
XetDownloadStream::next().await → Option<Bytes>
XetSession::abort()
XetSession::id()
XetSession::config()
```

### Gated away on WASM

- `XetUploadCommit` and all upload methods
- `XetFileDownloadGroup` and `download_file_to_path` (filesystem unavailable)
- All `_blocking` methods (`build_blocking`, `download_stream_blocking`, `blocking_next`, etc.)
- `XetSession::sigint_abort` (no SIGINT on WASM)
- `XetSessionBuilder::with_tokio_handle` (no multi-thread handle on WASM)

## Component Changes

### `xet_pkg/Cargo.toml`

Split the `tokio` dependency into target-specific blocks. WASM needs `sync, rt, time, macros` but not `net`. `tokio-util` (`CancellationToken`) is pure Rust and stays unconditional. No `tokio_with_wasm` in `xet_pkg` — the runtime is the consumer's responsibility.

```toml
# Remove the unconditional tokio line and replace with:
[target.'cfg(not(target_family = "wasm"))'.dependencies]
tokio = { workspace = true, features = ["net", "time"] }

[target.'cfg(target_family = "wasm")'.dependencies]
tokio = { workspace = true, features = ["sync", "rt", "time", "macros"] }
```

`tokio-util` remains as an unconditional dependency.

### `task_runtime.rs` — core change

Add `#[cfg(target_family = "wasm")]` variants of the bridge methods that drop `Send + 'static` bounds and directly await the future with cancellation token handling, bypassing `XetRuntime::bridge_async` (which requires `Send`). On WASM the session is always External-mode anyway, so `XetRuntime::bridge_async` would have just `.await`ed — the WASM variant replicates that behavior.

```rust
// Non-WASM: existing signatures unchanged
#[cfg(not(target_family = "wasm"))]
fn run_inner_async<T, F>(...) -> impl Future + Send + 'static
where F: Future + Send + 'static, T: Send + 'static { ... }

// WASM: no Send, direct await with cancellation
#[cfg(target_family = "wasm")]
fn run_inner_async<T, F>(...) -> impl Future + 'static
where F: Future + 'static, T: 'static { ... }
```

`bridge_sync` and `bridge_sync_finalizing` are gated `#[cfg(not(target_family = "wasm"))]`.

### `session.rs`

- `XetSession::sigint_abort`: `#[cfg(not(target_family = "wasm"))]`
- `XetSessionBuilder::with_tokio_handle`: `#[cfg(not(target_family = "wasm"))]`
- `XetSessionBuilder::build()`: add `#[cfg(target_family = "wasm")]` branch:
  ```rust
  #[cfg(target_family = "wasm")]
  pub fn build(self) -> Result<XetSession, SessionError> {
      let ctx = XetContext::from_external(tokio::runtime::Handle::current(), self.config);
      Ok(XetSession::new(ctx))
  }
  ```

### `upload_commit.rs`

Entire file wrapped in `#[cfg(not(target_family = "wasm"))]`. The two `tokio::spawn` calls inside are covered by this gate.

### `file_download_group.rs`

Entire file wrapped in `#[cfg(not(target_family = "wasm"))]`. The `tokio::spawn` call inside is covered.

### `auth_group_builder.rs`

Gate `build_blocking` with `#[cfg(not(target_family = "wasm"))]`.

### `download_stream_group.rs`

Gate `build_blocking`, `download_stream_blocking`, `download_unordered_stream_blocking`.

### `download_stream_handle.rs`

Gate `blocking_next` on both `XetDownloadStream` and `XetUnorderedDownloadStream`.

### `mod.rs`

Gate re-exports of upload and file-download public types.

## New Crate: `wasm/hf_xet_wasm_download/`

A new `cdylib + rlib` crate that wraps `xet_pkg::XetSession` with `#[wasm_bindgen]` and exposes a download-only JS API. Builds via `wasm-pack build`.

### Exposed JS API

```typescript
class XetSession {
  constructor(endpoint: string, token: string, token_expiry: number): XetSession;
  downloadStream(fileInfo: object, byteRange?: [number, number]): Promise<XetDownloadStream>;
}

class XetDownloadStream {
  next(): Promise<Uint8Array | undefined>;
  cancel(): void;
}
```

`XetFileInfo` is a serializable Rust struct (`serde`); it is passed as a plain JS object (via `serde-wasm-bindgen`), not a typed class. `byteRange` is an optional `[start, end]` pair.

### Structure

```
wasm/hf_xet_wasm_download/
├── Cargo.toml
├── src/
│   ├── lib.rs          # wasm_bindgen exports
│   ├── session.rs      # XetSession wrapper
│   └── stream.rs       # XetDownloadStream wrapper
└── examples/
    └── download.html   # manual test page
```

The `download.html` example connects to a real HF CAS endpoint, fetches a pointer file, and streams the reconstructed file to the browser using the JS API. This serves as the manual integration test.

## Testing

1. **CI compile check** — `cargo check --target wasm32-unknown-unknown -p hf-xet` added to the existing WASM CI job.
2. **`hf_xet_wasm_download` build** — `wasm-pack build wasm/hf_xet_wasm_download` in CI to verify the binding crate builds.
3. **Manual test** — `download.html` for human verification against a real HF endpoint.
4. **Native tests unchanged** — All existing `xet_pkg` tests continue to run as-is.

## Non-changes

- `xet_runtime`, `xet_client`, `xet_data`, `xet_core_structures`: no changes.
- `hf_xet_wasm`: untouched; to be removed in a future cleanup once `hf_xet_wasm_download` stabilizes.
- Error types, public type names, and native behavior: unchanged.
