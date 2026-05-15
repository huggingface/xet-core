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

`XetSessionBuilder::with_tokio_handle` is non-wasm-only. On wasm the
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

### `xet_core_structures::metadata_shard::session_directory`

Now `#[cfg(not(target_family = "wasm"))]`. It's filesystem-coupled and only
used by the non-wasm `SessionShardInterface`. The wasm
`SessionShardInterface` (`xet_data::processing::shard_interface::wasm`)
uses an in-memory `MDBInMemoryShard` instead — selected by `target_family`
at the `mod` level.

### `xet_data::processing::data_client` selective gating

`clean_bytes` remains available on wasm — it has no filesystem
dependencies and just feeds the byte slice into the `FileUploadSession`
clean handle. `clean_file`, `hash_files_async`, and the private
`hash_single_file` are `#[cfg(not(target_family = "wasm"))]`: they open
files from disk, and `hash_files_async` additionally routes through
`XetRuntime::spawn_blocking` (which on wasm runs `f` inline anyway —
unsuitable for the parallel-hash use case). Wasm consumers that need
hashing must drive it from JS (e.g. `crypto.subtle.digest`) or feed
bytes into `clean_bytes`.

### `TemplatedPathBuf` on wasm

The native `TemplatedPathBuf` expands `{PID}` and `{TIMESTAMP}`
placeholders at `evaluate()` time (used by `SystemMonitor::follow_process`
for per-process log file paths). On wasm there is no PID and no useful
filesystem, so the wasm shim in `xet_runtime/src/utils/mod.rs` keeps the
API surface the config system needs (`new`, `evaluate`, `template_string`)
but `evaluate` returns the input path unchanged — placeholders are not
expanded. The wasm `SystemMonitor` ignores `log_path` entirely (output
always goes via `tracing::info!`). Config values typed as
`TemplatedPathBuf` will round-trip on wasm but the template literals
inside them are treated as plain paths.

### `RemoteClient::upload_xorb` cross-target behavior

Previously there were two `upload_xorb` impls (a streaming one for
native, a `Bytes`-only one for wasm). They are now a single impl with
cfg-gated request construction:

- **Native**: request body is a `UploadProgressStream` wrapping the xorb
  bytes; the `upload_reporting_block_size` config field controls per-chunk
  progress callback frequency.
- **Wasm**: request body is the raw `Bytes` (reqwest's wasm backend does
  not support streaming request bodies). A single bulk
  `upload_reporter.report_progress(n_transfer_bytes)` fires after the
  upload succeeds, so the user callback and the adaptive-concurrency
  reporter both observe the full byte count exactly once.

Consumers wiring a `ProgressCallback` for uploads should be aware that
on wasm they will see one large progress event at the end of each xorb
rather than the chunked stream of events that native produces.

---

## Patterns to follow when modifying wasm-reachable code

The crates `xet_pkg`, `xet_client`, `xet_data`, `xet_core_structures`, and
`xet_runtime` are all compiled for `wasm32-unknown-unknown`. When touching
code reachable from those crates, keep the wasm build green by following
the patterns this codebase relies on:

- **Conditional `?Send` on `#[async_trait]`** for any trait whose impls
  touch reqwest, the wasm reqwest backend produces `!Send` futures:
  ```rust
  #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
  #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
  ```
  Trait *bounds* on `dyn T` typically stay `Send + Sync`; only the
  *future returned by the method* loses `Send` on wasm. Used in
  `URLProvider`, `DataWriter`, `Client`, `TokenRefresher`, the dedup
  interface, the file-reconstruction writers, and the simulation
  clients (about 25 sites total).

- **`web_time::Instant` instead of `std::time::Instant` /
  `tokio::time::Instant`** on any code path reachable from wasm.
  `std::time::Instant::now()` panics on wasm32; `tokio::time::Instant`
  drags in tokio time drivers. Existing call sites:
  `xet_client/.../adaptive_concurrency/controller.rs`,
  `xet_core_structures/utils/exp_weighted_moving_avg.rs`,
  `xet_core_structures/.../compression_scheme.rs`,
  `xet_data/.../reconstruction_terms/manager.rs`,
  `xet_data/progress_tracking/speed_tracker.rs`,
  `xet_runtime/logging/system_monitor/wasm.rs`.

- **`use tokio_with_wasm::alias as tokio;` at spawn sites.** Native code
  uses real `tokio` directly; wasm code aliases it so that `tokio::spawn`,
  `tokio::task::JoinSet`, `tokio::select!`, and `tokio::sync::*` resolve
  to the wasm-bridged variants backed by
  `wasm_bindgen_futures::spawn_local`. Applied in all files that call
  `tokio::spawn` or build a `JoinSet` — see the existing files under
  `xet_pkg/src/xet_session/`, `xet_data/src/processing/`, and
  `xet_data/src/file_reconstruction/` for the import line. Bare
  `tokio::spawn` in code reachable from wasm will fail to compile (or
  worse, panic at runtime because the spawned future is `!Send`).

- **`XetRuntime::new` is a stub on wasm.** It returns a `XetRuntime`
  with `handle_ref` empty and `RuntimeBackend::OwnedThreadPool { runtime:
  None }` — no tokio runtime is constructed. Calling
  `XetRuntime::handle()` on wasm panics with `"Not initialized with
  handle set."` The session-level bridge variants in
  `xet_pkg::xet_session::task_runtime` `.await` futures directly via
  `wasm_bindgen_futures` and never call back into `XetRuntime`, which is
  why this works. Do not assume `XetRuntime::handle()` /
  `XetRuntime::spawn` are usable on wasm.

---

## CI

- `build_and_test-wasm` job in `.github/workflows/ci.yml`:
  - `cargo +nightly check --target wasm32-unknown-unknown -p hf-xet` —
    compile gate that catches regressions in `xet_pkg` and its transitive
    deps even when nobody touches the wasm crates themselves.
  - Builds all three wasm crates (`hf_xet_thin_wasm`, `hf_xet_wasm_download`,
    `hf_xet_wasm_upload`) via each crate's `build_wasm.sh`.
  - Cargo.lock freshness checks for each wasm crate.
  - Two headless-Chromium smokes:
    - `wasm/hf_xet_wasm_download/tests/ci-smoke` — anonymous download of a
      pinned public file (`continue-on-error: true` — hub blips don't fail PRs).
    - `wasm/hf_xet_wasm_upload/tests/ci-smoke` — local-only data-prep
      regression guard for `XetRuntime::spawn_blocking` (blocking).

---

## Affected files (high level)

- `xet_runtime/src/core/runtime.rs` — wasm stub `XetRuntime::new` (no tokio runtime); cfg-gated `spawn_blocking` (wasm variant via `tokio_with_wasm::task::spawn_blocking`)
- `xet_runtime/src/logging/system_monitor/{native,wasm}.rs` — module split (wasm samples browser `performance.memory` / `navigator.connection`)
- `xet_runtime/src/utils/mod.rs` — wasm `TemplatedPathBuf` shim (`evaluate` is identity, no expansion)
- `xet_runtime/src/config/xet_config.rs` — `XetConfig::validate_usize_bounds` panics if `data.ingestion_block_size` exceeds the target's `usize::MAX`
- `xet_runtime/Cargo.toml` — wasm-target deps via `workspace = true`
- `xet_data/src/processing/shard_interface/{native,wasm}.rs` — module split (wasm uses in-memory `MDBInMemoryShard`)
- `xet_data/src/processing/configurations.rs` — wasm `TranslatorConfig::new` skips filesystem directory creation
- `xet_data/src/processing/data_client.rs` — `clean_bytes` available on wasm; `clean_file`, `hash_files_async`, `hash_single_file` gated to non-wasm
- `xet_data/src/processing/file_upload_session.rs` — `upload_files`, `spawn_upload_from_path`, `feed_file_to_cleaner` gated to non-wasm; `spawn_upload_bytes` routes through `XetRuntime::spawn` on native, `tokio_with_wasm::task::spawn` on wasm
- `xet_data/src/deduplication/file_deduplication.rs` + `xet_data/src/processing/file_upload_session.rs` — `simulation_max_bytes` cast order swapped (min in `u64` space before `as usize`) to avoid 32-bit truncation on wasm
- `xet_data/src/error.rs` — `WasmTaskJoinError` variant
- `xet_pkg/src/xet_session/session.rs` — wasm `XetSessionBuilder::build()` variant; `with_tokio_handle` and `sigint_abort` gated; `new_file_download_group` gated
- `xet_pkg/src/xet_session/task_runtime.rs` — wasm variants of `bridge_async` / `bridge_async_finalizing` / `run_inner_async` that `.await` directly (no `Send` bound); `bridge_sync` and `bridge_sync_finalizing` gated to non-wasm
- `xet_pkg/src/xet_session/{file_download_group,file_download_handle}.rs` — entire modules gated to non-wasm
- `xet_pkg/src/xet_session/{download_stream_group,download_stream_handle,upload_commit,upload_file_handle,upload_stream_handle}.rs` — `_blocking` methods gated to non-wasm
- `xet_pkg/src/legacy/mod.rs` — `#[cfg(not(target_family = "wasm"))]` at the parent `mod` in `lib.rs` (entire compatibility module disappears on wasm; consumers must conditionally import)
- `xet_client/src/cas_client/remote_client.rs` — `upload_xorb` unified across targets; wasm body is raw `Bytes` (no streaming body in wasm reqwest backend), single bulk progress event emitted post-success
- `xet_core_structures/src/metadata_shard/mod.rs` — `session_directory` gated to non-wasm
- `wasm/hf_xet_wasm_{download,upload}/` — new browser crates (separate workspaces)
- `wasm/hf_xet_wasm/` — deleted (stale)
