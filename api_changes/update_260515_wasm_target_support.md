# API Update: WASM target support for the upload/download API (2026-05-15)

## Overview

`xet_pkg::xet_session` now compiles for `wasm32-unknown-unknown`. The public
surface is a strict subset of the native API — see "Wasm-only differences"
below. No native call site changes.

Two browser-facing crates are published from this repo:

- `wasm/hf_xet_wasm_download` — `XetSession` + `XetDownloadStreamGroup` for downloads
- `wasm/hf_xet_wasm_upload` — `XetSession` + `XetUploadCommit` + `XetStreamUpload` for uploads

Both are separate workspaces under `[workspace.exclude]`; each pins
`wasm-bindgen = "=0.2.121"`, `wasm-bindgen-futures = "0.4"`, `js-sys = "0.3"`
independently. The main workspace pins the same versions under
`[workspace.dependencies]` (consumed by `xet_runtime` via `workspace = true`).
**Bump all three Cargo.toml files together** with the
`WASM_BINDGEN_VERSION` in `wasm/*/build_wasm.sh` and the cargo-tools cache
key in `.github/actions/build-wasm/action.yml`.

The previous stale browser crate `wasm/hf_xet_wasm` has been removed.

---

## Wasm-only differences

### Async only (no `_blocking` variants)

Wasm cannot block the host thread, so every `_blocking` method on
`XetSession`, `XetUploadCommit`, `XetFileUpload`, `XetStreamUpload`,
`XetDownloadStreamGroup`, and `XetDownloadStream` is
`#[cfg(not(target_family = "wasm"))]`.

### No filesystem entrypoints

These items are non-wasm-only — wasm cannot read the host filesystem:

- `XetUploadCommit::upload_from_path[_blocking]` — use `upload_bytes` or `upload_stream` instead
- `XetFileDownloadGroup`, `XetFileDownload`, `XetDownloadGroupReport`,
  `XetDownloadReport` — use `XetDownloadStreamGroup` and consume bytes in JS
- `xet_pkg::legacy` (the entire compatibility module re-exported for Python /
  `git_xet`) — filesystem-coupled

### No external tokio handle

`XetSessionBuilder::with_tokio_handle` and
`XetSession::new_from_external_runtime` are non-wasm-only. On wasm the
session uses `tokio_with_wasm::task::spawn_local` / `spawn_blocking`
shims — there is no real worker pool to attach to.

---

## Implementation-level changes consumers should know about

### `XetRuntime::spawn_blocking` is cfg-gated

On wasm, `XetRuntime::spawn_blocking` no longer routes through `self.handle()`
(which is intentionally empty on wasm). It delegates to
`tokio_with_wasm::task::spawn_blocking`, which runs `f` inline and returns a
completed `JoinHandle`. Downstream callers that store the returned
`JoinHandle` by type (`xet_data::processing::sha256::Sha256Generator`) must
import `tokio_with_wasm::alias as tokio` on wasm so the `JoinHandle` /
`JoinError` types resolve correctly per target.

### `DataError::WasmTaskJoinError`

Renamed from `WasmJoinError` for consistency with
`FileReconstructionError::WasmTaskJoinError`. Only added in this PR; no
external callers should exist yet.

### `xet_data::processing::data_client::legacy::clean_bytes`

`clean_bytes` moved out of `data_client` into the new
`data_client::legacy` submodule (along with the rest of the path-coupled
helpers). The `xet_pkg::legacy::clean_bytes` re-export tracks the move.
Native call sites are unchanged.

### `xet_core_structures::metadata_shard::session_directory`

Now `#[cfg(not(target_family = "wasm"))]`. It's filesystem-coupled and only
used by the non-wasm `SessionShardInterface`. The wasm
`SessionShardInterface` (`xet_data::processing::shard_interface::wasm`)
uses an in-memory `MDBInMemoryShard` instead — selected by `target_family`
at the `mod` level.

---

## CI

- `build_and_test-wasm` job in `.github/workflows/ci.yml` builds all three
  wasm crates and runs two headless-Chromium smokes:
  - `wasm/hf_xet_wasm_download/tests/ci-smoke` — anonymous download of a
    pinned public file (`continue-on-error: true` — hub blips don't fail PRs)
  - `wasm/hf_xet_wasm_upload/tests/ci-smoke` — local-only data-prep
    regression guard for `XetRuntime::spawn_blocking` (blocking)

---

## Affected files (high level)

- `xet_runtime/src/core/runtime.rs` — cfg-gated `spawn_blocking`; wasm variant via `tokio_with_wasm`
- `xet_runtime/src/logging/system_monitor/{native,wasm}.rs` — module split
- `xet_runtime/src/utils/mod.rs` — wasm `TemplatedPathBuf` shim
- `xet_runtime/Cargo.toml` — wasm-target deps via `workspace = true`
- `xet_data/src/processing/shard_interface/{native,wasm}.rs` — module split
- `xet_data/src/processing/data_client/legacy.rs` — extracted from `data_client.rs`
- `xet_data/src/error.rs` — `WasmTaskJoinError` variant
- `xet_pkg/src/xet_session/*.rs` — `_blocking` and path methods gated to non-wasm
- `xet_pkg/src/legacy/mod.rs` — `#[cfg(not(target_family = "wasm"))]` at the parent `mod` in `lib.rs`
- `xet_client/src/cas_client/remote_client.rs` — `upload_xorb` unified across targets; wasm body is raw `Bytes`, native is streamed
- `xet_core_structures/src/metadata_shard/mod.rs` — `session_directory` gated to non-wasm
- `wasm/hf_xet_wasm_{download,upload}/` — new browser crates (separate workspaces)
- `wasm/hf_xet_wasm/` — deleted (stale)
