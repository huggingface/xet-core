# xet_pkg WASM Download Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `hf-xet` (`xet_pkg`) compile for `wasm32-unknown-unknown` with a working async streaming download API, and ship a `hf_xet_wasm_download` crate with JS bindings for manual testing.

**Architecture:** `#[cfg(target_family = "wasm")]` gates added to `xet_pkg` following the pattern used by sub-crates. Upload, blocking methods, file-path download, and SIGINT handling are compiled away on WASM. `TaskRuntime::bridge_async` gets a WASM variant that directly awaits (no `Send` bound, no `XetRuntime::bridge_async` call). A new `wasm/hf_xet_wasm_download/` crate wraps `xet_pkg::XetSession` with `#[wasm_bindgen]`.

**Tech Stack:** Rust, `wasm-bindgen 0.2.100`, `wasm-pack`, `serde-wasm-bindgen`, nightly Rust + `-Z build-std`.

---

## File Map

| File | Action | What changes |
|---|---|---|
| `xet_pkg/Cargo.toml` | Modify | Split `tokio` into target-specific deps |
| `xet_pkg/src/xet_session/task_runtime.rs` | Modify | WASM variants of bridge methods; gate sync bridges |
| `xet_pkg/src/xet_session/session.rs` | Modify | WASM `build()` path; gate `sigint_abort`, `with_tokio_handle` |
| `xet_pkg/src/xet_session/mod.rs` | Modify | Gate upload/file-download module includes and re-exports |
| `xet_pkg/src/xet_session/download_stream_group.rs` | Modify | Gate `build_blocking`, `download_stream_blocking`, `download_unordered_stream_blocking` |
| `xet_pkg/src/xet_session/download_stream_handle.rs` | Modify | Gate `blocking_next` on both stream types |
| `wasm/hf_xet_wasm_download/Cargo.toml` | Create | New crate manifest |
| `wasm/hf_xet_wasm_download/rust-toolchain.toml` | Create | Nightly + wasm32 target |
| `wasm/hf_xet_wasm_download/build_wasm.sh` | Create | Build script |
| `wasm/hf_xet_wasm_download/src/lib.rs` | Create | wasm_bindgen exports |
| `wasm/hf_xet_wasm_download/src/session.rs` | Create | `XetSession` JS wrapper |
| `wasm/hf_xet_wasm_download/src/stream.rs` | Create | `XetDownloadStream` JS wrapper |
| `wasm/hf_xet_wasm_download/examples/download.html` | Create | Manual test page |
| `Cargo.toml` (root) | Modify | Exclude new crate from workspace |
| `.github/actions/build-wasm/action.yml` | Modify | Add WASM compile check + new crate build |

---

## Task 1: Split tokio dependency in `xet_pkg/Cargo.toml`

**Files:**
- Modify: `xet_pkg/Cargo.toml`

- [ ] **Step 1: Replace the unconditional tokio line with target-specific blocks**

In `xet_pkg/Cargo.toml`, find:
```toml
tokio = { workspace = true, features = ["net", "time"] }
tokio-util = { workspace = true }
```

Replace with:
```toml
tokio-util = { workspace = true }

[target.'cfg(not(target_family = "wasm"))'.dependencies]
tokio = { workspace = true, features = ["net", "time"] }

[target.'cfg(target_family = "wasm")'.dependencies]
tokio = { workspace = true, features = ["sync", "rt", "time", "macros"] }
```

- [ ] **Step 2: Verify native tests still compile and pass**

```bash
cargo test -p hf-xet 2>&1 | tail -5
```
Expected: all tests pass, zero compilation errors.

- [ ] **Step 3: Commit**

```bash
git add xet_pkg/Cargo.toml
git commit -m "feat(wasm): split tokio dependency into target-specific blocks in xet_pkg"
```

---

## Task 2: Add WASM bridge variants to `task_runtime.rs`

**Files:**
- Modify: `xet_pkg/src/xet_session/task_runtime.rs`

This is the core change. The `bridge_async` methods require `F: Future + Send + 'static` — WASM futures are `!Send`. We add WASM variants that drop `Send` and directly await the future rather than routing through `XetRuntime::bridge_async`. The `bridge_sync` and `bridge_sync_finalizing` methods don't exist on WASM (no blocking).

- [ ] **Step 1: Mark the `runtime` field as dead code on WASM**

Find the `TaskRuntime` struct definition (around line 63):
```rust
pub(super) struct TaskRuntime {
    runtime: Arc<XetRuntime>,
    cancellation_token: CancellationToken,
```

Replace with:
```rust
pub(super) struct TaskRuntime {
    #[cfg_attr(target_family = "wasm", allow(dead_code))]
    runtime: Arc<XetRuntime>,
    cancellation_token: CancellationToken,
```

- [ ] **Step 2: Replace `run_inner_async` with cfg-gated dual versions**

Find and replace the entire `run_inner_async` method (lines ~157–181):
```rust
    fn run_inner_async<T, F>(
        &self,
        task_name: &'static str,
        fut: F,
    ) -> impl Future<Output = Result<T, XetError>> + Send + 'static
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        let token = self.cancellation_token.clone();
        let runtime = self.runtime.clone();
        async move {
            runtime
                .bridge_async(task_name, async move {
                    tokio::select! {
                        _ = token.cancelled() => Err(XetError::UserCancelled(
                            format!("{task_name} cancelled by user"),
                        )),
                        result = fut => result,
                    }
                })
                .await
                .map_err(XetError::from)?
        }
    }
```

Replace with:
```rust
    #[cfg(not(target_family = "wasm"))]
    fn run_inner_async<T, F>(
        &self,
        task_name: &'static str,
        fut: F,
    ) -> impl Future<Output = Result<T, XetError>> + Send + 'static
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        let token = self.cancellation_token.clone();
        let runtime = self.runtime.clone();
        async move {
            runtime
                .bridge_async(task_name, async move {
                    tokio::select! {
                        _ = token.cancelled() => Err(XetError::UserCancelled(
                            format!("{task_name} cancelled by user"),
                        )),
                        result = fut => result,
                    }
                })
                .await
                .map_err(XetError::from)?
        }
    }

    #[cfg(target_family = "wasm")]
    fn run_inner_async<T, F>(
        &self,
        task_name: &'static str,
        fut: F,
    ) -> impl Future<Output = Result<T, XetError>> + 'static
    where
        F: Future<Output = Result<T, XetError>> + 'static,
        T: 'static,
    {
        let token = self.cancellation_token.clone();
        async move {
            tokio::select! {
                _ = token.cancelled() => Err(XetError::UserCancelled(
                    format!("{task_name} cancelled by user"),
                )),
                result = fut => result,
            }
        }
    }
```

- [ ] **Step 3: Replace `bridge_async` with cfg-gated dual versions**

Find and replace the entire `bridge_async` method (lines ~183–194):
```rust
    pub(super) async fn bridge_async<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_state(task_name)?;
        let result = self.run_inner_async(task_name, fut).await;
        if let Err(ref e) = result {
            self.update_state_on_error(e)?;
        }
        result
    }
```

Replace with:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub(super) async fn bridge_async<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_state(task_name)?;
        let result = self.run_inner_async(task_name, fut).await;
        if let Err(ref e) = result {
            self.update_state_on_error(e)?;
        }
        result
    }

    #[cfg(target_family = "wasm")]
    pub(super) async fn bridge_async<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + 'static,
        T: 'static,
    {
        self.check_state(task_name)?;
        let result = self.run_inner_async(task_name, fut).await;
        if let Err(ref e) = result {
            self.update_state_on_error(e)?;
        }
        result
    }
```

- [ ] **Step 4: Gate `bridge_sync` with `#[cfg(not(target_family = "wasm"))]`**

Find the line:
```rust
    pub(super) fn bridge_sync<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
```

Add the attribute on the line immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub(super) fn bridge_sync<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
```

- [ ] **Step 5: Replace `bridge_async_finalizing` with cfg-gated dual versions**

Find and replace the entire `bridge_async_finalizing` method (lines ~220–241):
```rust
    pub(super) async fn bridge_async_finalizing<T, F>(
        &self,
        task_name: &'static str,
        allow_repeat: bool,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.transition_to_finalizing(task_name, allow_repeat)?;

        let result = self.run_inner_async(task_name, fut).await;
        match &result {
            Ok(_) => self.set_state(XetTaskState::Completed)?,
            Err(XetError::UserCancelled(_)) => {
                self.set_state(XetTaskState::UserCancelled)?;
            },
            Err(e) => self.set_state(XetTaskState::Error(e.to_string()))?,
        }
        result
    }
```

Replace with:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub(super) async fn bridge_async_finalizing<T, F>(
        &self,
        task_name: &'static str,
        allow_repeat: bool,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.transition_to_finalizing(task_name, allow_repeat)?;

        let result = self.run_inner_async(task_name, fut).await;
        match &result {
            Ok(_) => self.set_state(XetTaskState::Completed)?,
            Err(XetError::UserCancelled(_)) => {
                self.set_state(XetTaskState::UserCancelled)?;
            },
            Err(e) => self.set_state(XetTaskState::Error(e.to_string()))?,
        }
        result
    }

    #[cfg(target_family = "wasm")]
    pub(super) async fn bridge_async_finalizing<T, F>(
        &self,
        task_name: &'static str,
        allow_repeat: bool,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + 'static,
        T: 'static,
    {
        self.transition_to_finalizing(task_name, allow_repeat)?;

        let result = self.run_inner_async(task_name, fut).await;
        match &result {
            Ok(_) => self.set_state(XetTaskState::Completed)?,
            Err(XetError::UserCancelled(_)) => {
                self.set_state(XetTaskState::UserCancelled)?;
            },
            Err(e) => self.set_state(XetTaskState::Error(e.to_string()))?,
        }
        result
    }
```

- [ ] **Step 6: Gate `bridge_sync_finalizing` with `#[cfg(not(target_family = "wasm"))]`**

Find the line:
```rust
    pub(super) fn bridge_sync_finalizing<T, F>(
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub(super) fn bridge_sync_finalizing<T, F>(
```

- [ ] **Step 7: Verify native tests still pass**

```bash
cargo test -p hf-xet 2>&1 | tail -5
```
Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add xet_pkg/src/xet_session/task_runtime.rs
git commit -m "feat(wasm): add WASM-compatible bridge variants to TaskRuntime"
```

---

## Task 3: Gate session builder and sigint_abort in `session.rs`

**Files:**
- Modify: `xet_pkg/src/xet_session/session.rs`

- [ ] **Step 1: Gate `with_tokio_handle` method**

Find the line:
```rust
    pub fn with_tokio_handle(self, handle: tokio::runtime::Handle) -> Self {
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn with_tokio_handle(self, handle: tokio::runtime::Handle) -> Self {
```

- [ ] **Step 2: Replace `build` with cfg-gated dual versions**

Find and replace the entire `build` method (lines ~171–188):
```rust
    pub fn build(self) -> Result<XetSession, SessionError> {
        #[cfg(feature = "fd-track")]
        let _fd_scope = track_fd_scope("XetSessionBuilder::build");

        let ctx = if let Some(h) = self.tokio_handle {
            info!("XetSession using explicitly provided tokio handle");
            XetContext::from_external(h, self.config)
        } else {
            XetContext::with_config(self.config)?
        };

        let session = XetSession::new(ctx);
        info!("Session created, session_id={}", session.inner.id);
        #[cfg(feature = "fd-track")]
        report_fd_count("XetSessionBuilder::build complete");
        Ok(session)
    }
```

Replace with:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn build(self) -> Result<XetSession, SessionError> {
        #[cfg(feature = "fd-track")]
        let _fd_scope = track_fd_scope("XetSessionBuilder::build");

        let ctx = if let Some(h) = self.tokio_handle {
            info!("XetSession using explicitly provided tokio handle");
            XetContext::from_external(h, self.config)
        } else {
            XetContext::with_config(self.config)?
        };

        let session = XetSession::new(ctx);
        info!("Session created, session_id={}", session.inner.id);
        #[cfg(feature = "fd-track")]
        report_fd_count("XetSessionBuilder::build complete");
        Ok(session)
    }

    #[cfg(target_family = "wasm")]
    pub fn build(self) -> Result<XetSession, SessionError> {
        let ctx = XetContext::from_external(tokio::runtime::Handle::current(), self.config);
        let session = XetSession::new(ctx);
        info!("Session created, session_id={}", session.inner.id);
        Ok(session)
    }
```

- [ ] **Step 3: Gate `sigint_abort`**

Find the line:
```rust
    pub fn sigint_abort(&self) -> Result<(), SessionError> {
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn sigint_abort(&self) -> Result<(), SessionError> {
```

- [ ] **Step 4: Gate `new_upload_commit` method on session**

Find the line:
```rust
    pub fn new_upload_commit(&self) -> Result<XetUploadCommitBuilder, SessionError> {
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn new_upload_commit(&self) -> Result<XetUploadCommitBuilder, SessionError> {
```

- [ ] **Step 5: Gate `new_file_download_group` method on session**

Find the line:
```rust
    pub fn new_file_download_group(&self) -> Result<XetFileDownloadGroupBuilder, SessionError> {
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn new_file_download_group(&self) -> Result<XetFileDownloadGroupBuilder, SessionError> {
```

- [ ] **Step 6: Verify native tests still pass**

```bash
cargo test -p hf-xet 2>&1 | tail -5
```
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add xet_pkg/src/xet_session/session.rs
git commit -m "feat(wasm): gate upload/file-download/sigint/handle methods in XetSessionBuilder"
```

---

## Task 4: Gate upload and file-download modules in `mod.rs`

**Files:**
- Modify: `xet_pkg/src/xet_session/mod.rs`

The upload and file-download modules use `tokio::spawn` and filesystem access — gate the entire modules rather than individual methods.

- [ ] **Step 1: Gate the `mod` declarations**

Find:
```rust
mod file_download_group;
mod file_download_handle;
```
and:
```rust
mod upload_commit;
mod upload_file_handle;
mod upload_stream_handle;
```

Replace with:
```rust
#[cfg(not(target_family = "wasm"))]
mod file_download_group;
#[cfg(not(target_family = "wasm"))]
mod file_download_handle;
```
and:
```rust
#[cfg(not(target_family = "wasm"))]
mod upload_commit;
#[cfg(not(target_family = "wasm"))]
mod upload_file_handle;
#[cfg(not(target_family = "wasm"))]
mod upload_stream_handle;
```

- [ ] **Step 2: Gate the `pub use` re-exports**

Find:
```rust
pub use file_download_group::{XetDownloadGroupReport, XetFileDownloadGroup, XetFileDownloadGroupBuilder};
pub use file_download_handle::{XetDownloadReport, XetFileDownload};
```
and:
```rust
pub use upload_commit::{XetCommitReport, XetFileMetadata, XetUploadCommit, XetUploadCommitBuilder};
pub use upload_file_handle::XetFileUpload;
pub use upload_stream_handle::XetStreamUpload;
```

Replace with:
```rust
#[cfg(not(target_family = "wasm"))]
pub use file_download_group::{XetDownloadGroupReport, XetFileDownloadGroup, XetFileDownloadGroupBuilder};
#[cfg(not(target_family = "wasm"))]
pub use file_download_handle::{XetDownloadReport, XetFileDownload};
```
and:
```rust
#[cfg(not(target_family = "wasm"))]
pub use upload_commit::{XetCommitReport, XetFileMetadata, XetUploadCommit, XetUploadCommitBuilder};
#[cfg(not(target_family = "wasm"))]
pub use upload_file_handle::XetFileUpload;
#[cfg(not(target_family = "wasm"))]
pub use upload_stream_handle::XetStreamUpload;
```

- [ ] **Step 3: Also gate the module-level doc comment references (optional cleanup)**

The long doc comment at the top of `mod.rs` references upload APIs. Those references won't compile-fail on WASM (they're comments), but consider wrapping the upload doc sections with `cfg_attr` if `rustdoc` complains. Skip this step if `cargo check` succeeds without it.

- [ ] **Step 4: Verify native tests still pass**

```bash
cargo test -p hf-xet 2>&1 | tail -5
```
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add xet_pkg/src/xet_session/mod.rs
git commit -m "feat(wasm): gate upload and file-download modules from WASM compilation"
```

---

## Task 5: Gate blocking methods in download modules

**Files:**
- Modify: `xet_pkg/src/xet_session/download_stream_group.rs`
- Modify: `xet_pkg/src/xet_session/download_stream_handle.rs`

- [ ] **Step 1: Gate `build_blocking` in `download_stream_group.rs`**

Find the line:
```rust
    pub fn build_blocking(self) -> Result<XetDownloadStreamGroup, XetError> {
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn build_blocking(self) -> Result<XetDownloadStreamGroup, XetError> {
```

- [ ] **Step 2: Gate `download_stream_blocking` in `download_stream_group.rs`**

Find the line:
```rust
    pub fn download_stream_blocking(
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn download_stream_blocking(
```

- [ ] **Step 3: Gate `download_unordered_stream_blocking` in `download_stream_group.rs`**

Find the line:
```rust
    pub fn download_unordered_stream_blocking(
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn download_unordered_stream_blocking(
```

- [ ] **Step 4: Gate `blocking_next` on `XetDownloadStream` in `download_stream_handle.rs`**

Find the line (the one inside `impl XetDownloadStream`, around line 77):
```rust
    pub fn blocking_next(&mut self) -> Result<Option<Bytes>, SessionError> {
        debug!(stream_id = %self.id, "Download stream next");
        self.inner.blocking_next().map_err(|e| SessionError::from(DataError::from(e)))
    }
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&mut self) -> Result<Option<Bytes>, SessionError> {
        debug!(stream_id = %self.id, "Download stream next");
        self.inner.blocking_next().map_err(|e| SessionError::from(DataError::from(e)))
    }
```

- [ ] **Step 5: Gate `blocking_next` on `XetUnorderedDownloadStream`**

Find the second `blocking_next` implementation (inside `impl XetUnorderedDownloadStream`, around line 179):
```rust
    pub fn blocking_next(&mut self) -> Result<Option<(u64, Bytes)>, SessionError> {
```

Add the attribute immediately before it:
```rust
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&mut self) -> Result<Option<(u64, Bytes)>, SessionError> {
```

- [ ] **Step 6: Verify native tests still pass**

```bash
cargo test -p hf-xet 2>&1 | tail -5
```
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add xet_pkg/src/xet_session/download_stream_group.rs xet_pkg/src/xet_session/download_stream_handle.rs
git commit -m "feat(wasm): gate blocking download methods from WASM compilation"
```

---

## Task 6: Verify `hf-xet` compiles for WASM

**Files:** none (verification only)

- [ ] **Step 1: Add the wasm32 target if not present**

```bash
rustup target add wasm32-unknown-unknown --toolchain nightly
```

- [ ] **Step 2: Run cargo check for WASM**

```bash
cargo +nightly check \
  --target wasm32-unknown-unknown \
  -p hf-xet \
  -Z build-std=std,panic_abort \
  --config 'build.rustflags=["-C", "target-feature=+atomics,+bulk-memory,+mutable-globals", "--cfg", "getrandom_backend=\"wasm_js\""]'
```

Expected: exits 0, no errors. Fix any remaining compile errors before proceeding — they are likely missing `#[cfg]` gates on methods that reference gated types.

- [ ] **Step 3: Re-run native tests to confirm nothing regressed**

```bash
cargo test -p hf-xet 2>&1 | tail -5
```
Expected: all tests pass.

- [ ] **Step 4: Commit (if any fixes were made in Step 2)**

```bash
git add -p
git commit -m "fix(wasm): resolve remaining WASM compile errors in xet_pkg"
```

---

## Task 7: Scaffold `wasm/hf_xet_wasm_download` crate

**Files:**
- Create: `wasm/hf_xet_wasm_download/Cargo.toml`
- Create: `wasm/hf_xet_wasm_download/rust-toolchain.toml`
- Modify: `Cargo.toml` (root)

- [ ] **Step 1: Create `wasm/hf_xet_wasm_download/Cargo.toml`**

```toml
[package]
name = "hf_xet_wasm_download"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
hf-xet = { path = "../../xet_pkg" }

bytes = "1"
js-sys = "0.3"
serde-wasm-bindgen = "0.6"
wasm-bindgen = "=0.2.100"
wasm-bindgen-futures = "0.4"

[package.metadata.docs.rs]
targets = ["wasm32-unknown-unknown"]
```

- [ ] **Step 2: Create `wasm/hf_xet_wasm_download/rust-toolchain.toml`**

```toml
[toolchain]
channel = "nightly"
components = ["rust-src", "rustfmt"]
targets = ["wasm32-unknown-unknown"]
```

- [ ] **Step 3: Exclude the new crate from the root workspace**

In `Cargo.toml` (root), find:
```toml
exclude = [
    "simulation/chunk_cache_bench",
    "hf_xet",
    "wasm/hf_xet_wasm",
    "wasm/hf_xet_thin_wasm",
]
```

Replace with:
```toml
exclude = [
    "simulation/chunk_cache_bench",
    "hf_xet",
    "wasm/hf_xet_wasm",
    "wasm/hf_xet_thin_wasm",
    "wasm/hf_xet_wasm_download",
]
```

- [ ] **Step 4: Commit**

```bash
git add wasm/hf_xet_wasm_download/Cargo.toml wasm/hf_xet_wasm_download/rust-toolchain.toml Cargo.toml
git commit -m "feat(wasm): scaffold hf_xet_wasm_download crate"
```

---

## Task 8: Implement `lib.rs` and `stream.rs`

**Files:**
- Create: `wasm/hf_xet_wasm_download/src/lib.rs`
- Create: `wasm/hf_xet_wasm_download/src/stream.rs`

- [ ] **Step 1: Create `src/lib.rs`**

```rust
#[cfg(not(target_family = "wasm"))]
compile_error!("hf_xet_wasm_download is only for the wasm32-unknown-unknown target");

mod session;
mod stream;

pub use session::XetSession;
pub use stream::XetDownloadStream;
```

- [ ] **Step 2: Create `src/stream.rs`**

```rust
use wasm_bindgen::prelude::*;
use xet::xet_session::XetDownloadStream as InnerStream;

#[wasm_bindgen(js_name = "XetDownloadStream")]
pub struct XetDownloadStream {
    inner: InnerStream,
}

impl XetDownloadStream {
    pub(crate) fn new(inner: InnerStream) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = "XetDownloadStream")]
impl XetDownloadStream {
    /// Returns the next chunk as a `Uint8Array`, or `undefined` when the stream is complete.
    #[wasm_bindgen]
    pub async fn next(&mut self) -> Result<JsValue, JsValue> {
        match self
            .inner
            .next()
            .await
            .map_err(|e| JsValue::from_str(&format!("{e:?}")))?
        {
            Some(bytes) => Ok(js_sys::Uint8Array::from(bytes.as_ref()).into()),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Cancels the in-progress download.
    #[wasm_bindgen]
    pub fn cancel(&mut self) {
        self.inner.cancel();
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add wasm/hf_xet_wasm_download/src/lib.rs wasm/hf_xet_wasm_download/src/stream.rs
git commit -m "feat(wasm): add lib.rs and XetDownloadStream wrapper for hf_xet_wasm_download"
```

---

## Task 9: Implement `session.rs` wrapper

**Files:**
- Create: `wasm/hf_xet_wasm_download/src/session.rs`

- [ ] **Step 1: Create `src/session.rs`**

```rust
use std::ops::Range;

use wasm_bindgen::prelude::*;
use xet::xet_session::{XetFileInfo, XetSession as InnerSession, XetSessionBuilder};

use crate::stream::XetDownloadStream;

fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

/// WASM-facing session for streaming downloads from the Xet CAS server.
///
/// Create one with `new(endpoint, token, tokenExpiry)`, then call
/// `downloadStream(fileInfo)` to begin streaming a file.
#[wasm_bindgen(js_name = "XetSession")]
pub struct XetSession {
    inner: InnerSession,
    endpoint: String,
    token: String,
    token_expiry: u64,
}

#[wasm_bindgen(js_class = "XetSession")]
impl XetSession {
    /// Create a new session.
    ///
    /// - `endpoint`: CAS server URL, e.g. `"https://cas-server.xethub.com"`
    /// - `token`: CAS access token string
    /// - `tokenExpiry`: token expiry as a Unix timestamp (seconds). Pass `0` to use no expiry.
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: String, token: String, token_expiry: f64) -> Result<XetSession, JsValue> {
        let session = XetSessionBuilder::new().build().map_err(js_err)?;
        Ok(Self {
            inner: session,
            endpoint,
            token,
            token_expiry: token_expiry as u64,
        })
    }

    /// Begin streaming a file described by `fileInfo`.
    ///
    /// `fileInfo` must be a plain JS object matching the `XetFileInfo` shape:
    /// `{ hash: string, fileSize: number, ... }`.
    ///
    /// `byteRangeStart` and `byteRangeEnd` are optional; when both are provided,
    /// only that byte range is downloaded.
    #[wasm_bindgen(js_name = "downloadStream")]
    pub async fn download_stream(
        &self,
        file_info: JsValue,
        byte_range_start: Option<f64>,
        byte_range_end: Option<f64>,
    ) -> Result<XetDownloadStream, JsValue> {
        let file_info: XetFileInfo =
            serde_wasm_bindgen::from_value(file_info).map_err(js_err)?;

        let range: Option<Range<u64>> = match (byte_range_start, byte_range_end) {
            (Some(start), Some(end)) => Some(start as u64..end as u64),
            _ => None,
        };

        let group = self
            .inner
            .new_download_stream_group()
            .map_err(js_err)?
            .with_endpoint(&self.endpoint)
            .with_token_info(self.token.clone(), self.token_expiry)
            .build()
            .await
            .map_err(js_err)?;

        let stream = group
            .download_stream(file_info, range)
            .await
            .map_err(js_err)?;

        Ok(XetDownloadStream::new(stream))
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add wasm/hf_xet_wasm_download/src/session.rs
git commit -m "feat(wasm): add XetSession JS wrapper for hf_xet_wasm_download"
```

---

## Task 10: Create build script and manual test page

**Files:**
- Create: `wasm/hf_xet_wasm_download/build_wasm.sh`
- Create: `wasm/hf_xet_wasm_download/examples/download.html`

- [ ] **Step 1: Create `build_wasm.sh`**

```sh
#!/bin/sh
set -ex

WASM_BINDGEN_VERSION="0.2.100"

if command -v wasm-bindgen >/dev/null 2>&1; then
    INSTALLED="$(wasm-bindgen --version | awk '{print $2}')"
else
    INSTALLED=""
fi

if [ "$INSTALLED" != "$WASM_BINDGEN_VERSION" ]; then
    cargo install -f wasm-bindgen-cli --version "$WASM_BINDGEN_VERSION"
fi

TARGET_RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals \
  --cfg getrandom_backend=\"wasm_js\"" \
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS="$TARGET_RUSTFLAGS" \
cargo +nightly build \
    --target wasm32-unknown-unknown \
    --release \
    -Z build-std=std,panic_abort

wasm-bindgen \
    target/wasm32-unknown-unknown/release/hf_xet_wasm_download.wasm \
    --out-dir ./pkg/ \
    --typescript \
    --target web
```

Make it executable:
```bash
chmod +x wasm/hf_xet_wasm_download/build_wasm.sh
```

- [ ] **Step 2: Create `examples/download.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>hf_xet_wasm_download — manual test</title>
  <style>
    body { font-family: monospace; max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
    label { display: block; margin: 0.5rem 0 0.2rem; }
    input, textarea { width: 100%; box-sizing: border-box; padding: 0.3rem; }
    textarea { height: 6rem; }
    button { margin-top: 1rem; padding: 0.5rem 1rem; }
    #log { margin-top: 1rem; white-space: pre-wrap; background: #f4f4f4; padding: 1rem; min-height: 4rem; }
  </style>
</head>
<body>
  <h1>XetSession download test</h1>
  <p>Build the WASM package first: <code>./build_wasm.sh</code>, then serve this
  directory with any static server, e.g. <code>python3 -m http.server 8080</code>
  and open <code>http://localhost:8080/examples/download.html</code>.</p>

  <label>CAS endpoint</label>
  <input id="endpoint" value="https://cas-server.xethub.com" />

  <label>CAS token</label>
  <input id="token" placeholder="paste CAS JWT here" />

  <label>Token expiry (Unix seconds, 0 = no expiry)</label>
  <input id="expiry" type="number" value="0" />

  <label>XetFileInfo (JSON)</label>
  <textarea id="fileInfo" placeholder='{"hash":"...","fileSize":...}'></textarea>

  <button id="downloadBtn">Download</button>
  <button id="saveBtn" style="display:none">Save file</button>

  <div id="log">Ready.</div>

  <script type="module">
    import init, { XetSession } from '../pkg/hf_xet_wasm_download.js';

    await init();

    let downloadedBytes = null;

    document.getElementById('downloadBtn').addEventListener('click', async () => {
      const log = document.getElementById('log');
      const saveBtn = document.getElementById('saveBtn');
      log.textContent = 'Starting download...\n';
      saveBtn.style.display = 'none';
      downloadedBytes = null;

      try {
        const endpoint = document.getElementById('endpoint').value.trim();
        const token = document.getElementById('token').value.trim();
        const expiry = parseFloat(document.getElementById('expiry').value);
        const fileInfo = JSON.parse(document.getElementById('fileInfo').value);

        const session = new XetSession(endpoint, token, expiry);
        const stream = await session.downloadStream(fileInfo);

        const chunks = [];
        let totalBytes = 0;
        while (true) {
          const chunk = await stream.next();
          if (chunk === undefined) break;
          chunks.push(chunk);
          totalBytes += chunk.byteLength;
          log.textContent += `  received ${chunk.byteLength} bytes (total: ${totalBytes})\n`;
        }

        downloadedBytes = new Blob(chunks);
        log.textContent += `\nDone — ${totalBytes} bytes total.`;
        saveBtn.style.display = 'inline';
      } catch (e) {
        log.textContent += `\nERROR: ${e}`;
      }
    });

    document.getElementById('saveBtn').addEventListener('click', () => {
      if (!downloadedBytes) return;
      const url = URL.createObjectURL(downloadedBytes);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'download.bin';
      a.click();
      URL.revokeObjectURL(url);
    });
  </script>
</body>
</html>
```

- [ ] **Step 3: Commit**

```bash
git add wasm/hf_xet_wasm_download/build_wasm.sh wasm/hf_xet_wasm_download/examples/download.html
git commit -m "feat(wasm): add build script and manual test page for hf_xet_wasm_download"
```

---

## Task 11: Build and smoke-test the new crate

**Files:** none (build verification)

- [ ] **Step 1: Run the build script**

```bash
cd wasm/hf_xet_wasm_download
./build_wasm.sh
```

Expected: exits 0. Creates `pkg/` directory containing `hf_xet_wasm_download.js`, `hf_xet_wasm_download.d.ts`, `hf_xet_wasm_download_bg.wasm`.

Fix any compile errors before proceeding. Common issues:
- Missing `use` statements in `session.rs` / `stream.rs`
- `XetFileInfo` field names differ from JSON (check `xet_data/src/processing/xet_file.rs` for field names)
- wasm-bindgen version mismatch with `wasm-bindgen-cli`

- [ ] **Step 2: Commit the generated Cargo.lock**

The other WASM crates commit their `Cargo.lock`. Do the same:
```bash
cd wasm/hf_xet_wasm_download
git add Cargo.lock
git commit -m "chore: add Cargo.lock for hf_xet_wasm_download"
```

- [ ] **Step 3: Serve the example page and manually verify**

```bash
cd wasm/hf_xet_wasm_download
python3 -m http.server 8080
# open http://localhost:8080/examples/download.html
```

Fill in a real HF CAS endpoint, a valid token, and a `XetFileInfo` JSON blob (obtain from a Hub pointer file). Click Download and verify bytes stream in. Click Save to write to disk and verify the file is intact.

---

## Task 12: Update CI to check WASM compilation and build the new crate

**Files:**
- Modify: `.github/actions/build-wasm/action.yml`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add `hf-xet` WASM compile check and `hf_xet_wasm_download` build to the action**

In `.github/actions/build-wasm/action.yml`, after the existing `Build hf_xet_wasm` step, add:

```yaml
    - name: Check hf-xet compiles for wasm32-unknown-unknown
      shell: bash
      run: |
        CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals --cfg getrandom_backend=\"wasm_js\"" \
        cargo +nightly check \
          --target wasm32-unknown-unknown \
          -p hf-xet \
          -Z build-std=std,panic_abort
    - name: Build hf_xet_wasm_download
      shell: bash
      working-directory: wasm/hf_xet_wasm_download
      run: |
        ./build_wasm.sh
```

- [ ] **Step 2: Add Cargo.lock freshness check for `hf_xet_wasm_download` in the CI workflow**

In `.github/workflows/ci.yml`, find the `build_and_test-wasm` job steps. After the existing `hf_xet_wasm` Cargo.lock check, add:

```yaml
      - name: Check hf_xet_wasm_download Cargo.lock has no uncommitted changes
        working-directory: wasm/hf_xet_wasm_download
        shell: bash
        run: |
          test -z "$(git status --porcelain Cargo.lock)" || (echo "hf_xet_wasm_download Cargo.lock has uncommitted changes!" && exit 1)
```

- [ ] **Step 3: Commit**

```bash
git add .github/actions/build-wasm/action.yml .github/workflows/ci.yml
git commit -m "ci: add hf-xet WASM compile check and hf_xet_wasm_download build"
```

---

## Task 13: Final verification

- [ ] **Step 1: Run the full native test suite**

```bash
cargo test -p hf-xet 2>&1 | tail -20
```
Expected: all tests pass, no regressions.

- [ ] **Step 2: Run the WASM compile check one more time end-to-end**

```bash
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals --cfg getrandom_backend=\"wasm_js\"" \
cargo +nightly check \
  --target wasm32-unknown-unknown \
  -p hf-xet \
  -Z build-std=std,panic_abort
```
Expected: clean.

- [ ] **Step 3: Rebuild `hf_xet_wasm_download`**

```bash
cd wasm/hf_xet_wasm_download && ./build_wasm.sh
```
Expected: clean.

- [ ] **Step 4: Open a draft PR**

```bash
git push -u origin HEAD
gh pr create --draft \
  --title "feat(wasm): make hf-xet compile for wasm32 with streaming download support" \
  --body "$(cat <<'EOF'
Closes #840

## Changes

- `xet_pkg/Cargo.toml`: split `tokio` into target-specific dependency blocks
- `task_runtime.rs`: WASM variants of `bridge_async`/`bridge_async_finalizing` (no `Send` bound, direct await); gate `bridge_sync`/`bridge_sync_finalizing`
- `session.rs`: WASM `build()` path via `Handle::current()`; gate `sigint_abort`, `with_tokio_handle`, `new_upload_commit`, `new_file_download_group`
- `mod.rs`: gate upload and file-download module includes and re-exports
- `download_stream_group.rs` / `download_stream_handle.rs`: gate all `_blocking` methods
- New crate `wasm/hf_xet_wasm_download`: `#[wasm_bindgen]` wrapper over `xet_pkg::XetSession` exposing async streaming download to JS, with a manual test HTML page

## Testing

- All existing native `hf-xet` tests pass
- `cargo check --target wasm32-unknown-unknown -p hf-xet` passes in CI
- `wasm/hf_xet_wasm_download` builds via `wasm-pack`
- Manual browser test via `examples/download.html`
EOF
)"
```
