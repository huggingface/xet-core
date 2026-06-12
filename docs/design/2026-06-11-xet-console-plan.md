# xet-console Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Feature-gated (`console`) live-state observability for xet-core: a per-process axum server on port 6660 exposing every `XetSession`'s upload commits (files/xorbs/shards/dedup), download groups (files/terms/prefetch), and adaptive concurrency monitors as JSON.

**Architecture:** Live-state registry (spec approach A). Instrumented components hold cfg-gated `Option<Arc<XxxConsole>>` cells defined in a new `xet_runtime::console` module and update them inline; a process-global registry holds weak refs to live state plus bounded rings of completed summaries; axum handlers snapshot on demand. Feature chain `xet_pkg/console → xet-data/console → xet-client/console → xet-runtime/console`. The console must never break the transfer: every console failure degrades to `tracing::warn!`.

**Tech Stack:** Rust, axum 0.8 (already a workspace dep), serde/serde_json, tokio current-thread runtime on a dedicated `std::thread`, reqwest (tests).

**Spec:** `docs/design/2026-06-11-xet-console-design.md`. The TUI client is a separate follow-up plan.

---

## Codebase facts you need (verified, with locations)

- Crate dirs use underscores (`xet_runtime/`), package names use hyphens (`xet-runtime`). All four crates depend on `xet-runtime`.
- `XetContext` (`xet_runtime/src/core/context.rs:17`) = `{ runtime: Arc<XetRuntime>, config: Arc<XetConfig>, common: Arc<XetCommon> }`, `Clone`. A fresh `XetContext` (and thus a fresh `XetCommon`) is created per `XetSession` in `XetSessionBuilder::build()` (`xet_pkg/src/xet_session/session.rs:180-196`) **before** `XetSession::new(ctx)` runs. Commits/groups clone the session's ctx via `create_translator_config`, so `ctx.common` is effectively session-scoped → it is where the session's console scope lives.
- `XetCommon` (`xet_runtime/src/core/common.rs:7`) has `file_ingestion_semaphore`, `file_download_semaphore`, and is constructed in `XetCommon::new(config: &XetConfig)` (`common.rs:36`).
- `UniqueId` (`xet_runtime/src/utils/unique_id.rs`) = `pub struct UniqueId(pub u64)` with `UniqueId::new()` from a global atomic.
- `XetSession::new` (`xet_pkg/src/xet_session/session.rs:238`) sets `id: Uuid::now_v7()`. `XetSessionInner` must NOT hold child commit/group references (documented circular-dependency hazard at `session.rs:31-42`) — the console registry's weak refs respect this.
- `FileUploadSession` (`xet_data/src/processing/file_upload_session.rs:38-60`) holds `ctx`, `client`, `shard_interface: SessionShardInterface`, `completion_tracker: Arc<CompletionTracker>`, `progress: Arc<GroupProgress>`, `deduplication_metrics: Mutex<DeduplicationMetrics>`, `xorb_upload_tasks: Mutex<JoinSet<…>>`. Xorb upload spawn site with permit + `ProgressCallback` is `register_new_xorb` (~line 306-399). `finalize_impl` (~line 545) drains xorbs, then `shard_interface.upload_and_register_session_shards()`.
- `SingleFileCleaner` (`xet_data/src/processing/file_cleaner.rs:56`) holds `file_id: CompletionTrackerFileId`, `session: Arc<FileUploadSession>`; `finish_inner` (~line 219) computes the final file hash: `let (file_hash, …) = self.dedup_manager_fut.await?.finalize(metadata_ext);` and builds `XetFileInfo { hash: file_hash.hex(), … }`.
- `FileDeduper` (`xet_data/src/deduplication/file_deduplication.rs:58`) owns `deduplication_metrics: DeduplicationMetrics` (a `Copy` type, `dedup_metrics.rs`), merged per block at line ~289.
- `CompletionTracker` (`xet_data/src/progress_tracking/upload_tracking.rs`): `register_new_file(updater, n_bytes) -> CompletionTrackerFileId`, `register_dependencies(&[FileXorbDependency])` where `FileXorbDependency { file_id: u64, xorb_hash: MerkleHash, n_bytes: u64, is_external: bool }`, `register_xorb_upload_completion(xorb_hash)`.
- `SessionShardInterface` (`xet_data/src/processing/shard_interface.rs:30`): `add_uploaded_xorb_block` stages xorb metadata (in-memory + periodic flush), `upload_and_register_session_shards` (~line 239) consolidates via `consolidate_shards_in_directory` → `shard_list` of `si` with `si.shard_hash`, spawns `upload_shard` per shard.
- `GroupProgress` / `ItemProgress` (`xet_data/src/progress_tracking/progress_types.rs:12-75`): atomics + `report() -> GroupProgressReport { total_bytes, total_bytes_completed, total_bytes_completion_rate: Option<f64>, total_transfer_bytes, total_transfer_bytes_completed, total_transfer_bytes_completion_rate: Option<f64> }`, `item_reports() -> HashMap<UniqueId, ItemProgressReport { item_name, total_bytes, bytes_completed }>`.
- `FileDownloadSession` (`xet_data/src/processing/file_download_session.rs:28`): `new(config: Arc<TranslatorConfig>, chunk_cache)`, `download_file_background` acquires `file_download_semaphore` then calls `download_file_with_id` → `setup_reconstructor(file_info, None, Some(progress_updater))`.
- `ReconstructionTermManager` (`xet_data/src/file_reconstruction/reconstruction_terms/manager.rs:26-41`): `prefetch_queue: VecDeque<JoinHandle<RawFetchedFileTerms>>` where `RawFetchedFileTerms = Result<Option<(Vec<FileTerm>, u64, u64)>>`; `prefetch_block` (~line 234) spawns `retrieve_file_term_block` per byte-range block; `next_file_terms` (~line 93) pops + updates `completion_rate_estimator`. **The fetch unit is a block that resolves to `Vec<FileTerm>`** — console models blocks with states, listing resolved terms within. `FileTerm { byte_range: FileRange, xorb_chunk_range: ChunkRange, …, xorb_block: Arc<XorbBlock>, … }` (`file_term.rs:22`).
- `AdaptiveConcurrencyController` (`xet_client/src/cas_client/adaptive_concurrency/controller.rs:261-285`): holds `concurrency_semaphore: Arc<AdjustableSemaphore>` (AdjustableSemaphore is defined in **xet_runtime** `utils/adjustable_semaphore.rs` — the console monitor cell can hold it directly), `logging_tag: &'static str`, `adjustment_disabled`. Constructors `new(ctx, logging_tag, concurrency, concurrency_bounds, …)`, `new_upload`, `new_download` (tags in use: "upload", "download", "local_uploads", "memory_uploads" — instantiated in `remote_client.rs:99-100`, `simulation/local_client.rs:331`, `simulation/memory_client.rs:87`). Limit adjustment sites: increase ~lines 522-555 (`increment_total_permits(1)`), decrease ~lines 557-591 (`decrement_total_permits(1)`). Exported model types `CCSuccessModelState { success_ratio, success_ratio_thresholds: (f64,f64), recommended_adjustment: i8 }` and `CCLatencyModelState { predicted_max_rtt, prediction_max_rtt_standard_error, predicted_bandwidth }` (lines 29-41).
- Test infra: `xet_pkg/tests/test_xet_session.rs` — `XetSessionBuilder::new().build().unwrap()`, endpoint `format!("local://{}", dir.display())`, `session.new_upload_commit().unwrap().with_endpoint(&endpoint).build().await.unwrap()`, `commit.upload_bytes(data, Sha256Policy::Compute, Some(name.into()))`, `handle.finalize_ingestion().await`, `commit.commit().await`, `session.new_file_download_group().unwrap().with_endpoint(&endpoint).build().await.unwrap()`, `group.download_file_to_path(meta.xet_info.clone(), dest).await`, `group.finish().await`. Tests use `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`.
- CI: `.github/workflows/ci.yml` — test step at ~line 92 runs `cargo test --verbose --no-fail-fast --features "strict simulation git-xet-for-integration-test"`; lint at ~line 83 runs `cargo clippy -r -- -D warnings`.
- `serial_test` is a workspace dev-dependency (use `#[serial]` for tests that set `XET_CONSOLE_PORT`).

## Conventions for all tasks

- Every console field/call site is gated `#[cfg(feature = "console")]`. Cells are `Option<Arc<XxxConsole>>` — `None` when no session scope exists (e.g. bare `FileUploadSession` in an existing test). Hook call sites are 1-3 lines; never let a console error propagate: there are no `?` on console paths.
- Naming: live cells end in `Console` (`SessionConsole`, `UploadCommitConsole`, `FileUploadConsole`, `XorbConsole`, `DownloadGroupConsole`, `DownloadFileConsole`, `FetchBlockConsole`, `MonitorConsole`); wire types are nouns/`…Snapshot` in `model.rs`.
- Timestamps: `crate::console::now_ms()` (epoch millis). IDs on the wire: `u64` (from `UniqueId.0`) for entities, uuid string for sessions, hex strings for hashes.
- Run all commands from the repo root `/Users/assafvayner/hf/xet-core`.
- Commit after every green test run. Use `git add <specific files>`, never `git add -A`.

---

### Task 1: Feature plumbing across the four crates

**Files:**
- Modify: `xet_runtime/Cargo.toml`
- Modify: `xet_client/Cargo.toml`
- Modify: `xet_data/Cargo.toml`
- Modify: `xet_pkg/Cargo.toml`
- Modify: `xet_runtime/src/lib.rs`
- Create: `xet_runtime/src/console/mod.rs`

- [ ] **Step 1: Add the `console` feature and optional axum dep to xet_runtime**

In `xet_runtime/Cargo.toml`, add to the `[features]` section:

```toml
console = ["dep:axum"]
```

Add to `[dependencies]` (next to the other optional deps like `console-subscriber`):

```toml
axum = { workspace = true, optional = true }
```

In the `[target.'cfg(not(target_family = "wasm"))'.dependencies]` tokio entry, add `"net"` to the features list (axum's listener needs the net driver; tokio features are additive and already unified workspace-wide via reqwest):

```toml
tokio = { workspace = true, features = [
    "time",
    "rt",
    "macros",
    "sync",
    "test-util",
    "io-util",
    "rt-multi-thread",
    "net",
] }
```

- [ ] **Step 2: Chain the feature through the other three crates**

`xet_client/Cargo.toml` `[features]`:

```toml
console = ["xet-runtime/console"]
```

`xet_data/Cargo.toml` `[features]`:

```toml
console = ["xet-runtime/console", "xet-client/console"]
```

`xet_pkg/Cargo.toml` `[features]`:

```toml
console = ["xet-runtime/console", "xet-client/console", "xet-data/console"]
```

- [ ] **Step 3: Create the gated module skeleton**

Create `xet_runtime/src/console/mod.rs`:

```rust
//! xet-console: feature-gated live observability for xet sessions.
//!
//! See docs/design/2026-06-11-xet-console-design.md. Everything in this module
//! follows one rule: a console failure must never affect the host transfer.

use std::time::{SystemTime, UNIX_EPOCH};

/// Epoch milliseconds, server-stamped on every snapshot.
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
```

In `xet_runtime/src/lib.rs`, after the existing `pub mod` lines:

```rust
#[cfg(feature = "console")]
pub mod console;
```

- [ ] **Step 4: Verify both build configurations**

Run: `cargo build -p xet-runtime && cargo build -p xet-pkg --features console`
Expected: both succeed. Then run: `cargo test -p xet-runtime --features console` — expected: existing tests still pass (no new tests yet).

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/Cargo.toml xet_client/Cargo.toml xet_data/Cargo.toml xet_pkg/Cargo.toml xet_runtime/src/lib.rs xet_runtime/src/console/mod.rs
git commit -m "feat(console): add console feature chain and module skeleton"
```

---

### Task 2: Timestamped ring buffer

**Files:**
- Create: `xet_runtime/src/console/ring.rs`
- Modify: `xet_runtime/src/console/mod.rs`

- [ ] **Step 1: Write the failing tests**

Create `xet_runtime/src/console/ring.rs` with the tests first (implementation stubbed by the type below in step 3 — write the whole file in one go but run tests before implementing `push` if you want the strict red step; at minimum confirm the test names fail to compile without the impl):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_only_last_capacity_items() {
        let ring = TimestampedRing::new(3);
        for i in 0..5u64 {
            ring.push(i);
        }
        let items: Vec<u64> = ring.snapshot().into_iter().map(|(_, v)| v).collect();
        assert_eq!(items, vec![2, 3, 4]);
    }

    #[test]
    fn ring_snapshot_is_oldest_first_with_timestamps() {
        let ring = TimestampedRing::new(8);
        ring.push("a");
        ring.push("b");
        let snap = ring.snapshot();
        assert_eq!(snap.len(), 2);
        assert!(snap[0].0 <= snap[1].0, "timestamps must be non-decreasing");
        assert_eq!(snap[0].1, "a");
    }

    #[test]
    fn empty_ring_snapshots_empty() {
        let ring: TimestampedRing<u32> = TimestampedRing::new(4);
        assert!(ring.snapshot().is_empty());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-runtime --features console console::ring`
Expected: compile error — `TimestampedRing` not defined.

- [ ] **Step 3: Implement**

Add above the tests in `ring.rs`:

```rust
use std::collections::VecDeque;
use std::sync::Mutex;

use super::now_ms;

/// Bounded ring of (epoch_ms, value) pairs. All console history ("recent
/// completions", concurrency-limit changes) goes through this type, which is
/// what keeps console memory independent of transfer size.
pub struct TimestampedRing<T> {
    capacity: usize,
    items: Mutex<VecDeque<(u64, T)>>,
}

impl<T: Clone> TimestampedRing<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub fn push(&self, value: T) {
        let Ok(mut items) = self.items.lock() else {
            return; // poisoned: drop the sample, never propagate
        };
        if items.len() == self.capacity {
            items.pop_front();
        }
        items.push_back((now_ms(), value));
    }

    /// Oldest-first copy.
    pub fn snapshot(&self) -> Vec<(u64, T)> {
        self.items.lock().map(|i| i.iter().cloned().collect()).unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.items.lock().map(|i| i.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
```

In `console/mod.rs` add: `pub mod ring;`

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-runtime --features console console::ring`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/src/console/ring.rs xet_runtime/src/console/mod.rs
git commit -m "feat(console): bounded timestamped ring buffer"
```

---

### Task 3: Wire model types

The single source of truth for the API contract. Server handlers return these; the future TUI deserializes them.

**Files:**
- Create: `xet_runtime/src/console/model.rs`
- Modify: `xet_runtime/src/console/mod.rs`

- [ ] **Step 1: Write the failing serde tests**

Create `xet_runtime/src/console/model.rs` starting with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enums_serialize_snake_case() {
        assert_eq!(serde_json::to_string(&FileUploadState::AwaitingXorbs).unwrap(), "\"awaiting_xorbs\"");
        assert_eq!(serde_json::to_string(&TermState::Fetching).unwrap(), "\"fetching\"");
        assert_eq!(serde_json::to_string(&XorbState::Uploading).unwrap(), "\"uploading\"");
        assert_eq!(serde_json::to_string(&SessionState::Ended).unwrap(), "\"ended\"");
    }

    #[test]
    fn upload_commit_detail_round_trips() {
        let detail = UploadCommitDetail {
            as_of: 1,
            id: 7,
            state: UploadCommitState::Active,
            created_at: 0,
            endpoint: Some("local://x".into()),
            progress: Some(ProgressSnapshot::default()),
            dedup: DedupSnapshot::default(),
            files: vec![],
            completed_files: vec![],
            file_counts: FileCounts::default(),
            xorbs: XorbsSnapshot::default(),
            shards: vec![],
        };
        let json = serde_json::to_string(&detail).unwrap();
        let back: UploadCommitDetail = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, 7);
        assert!(matches!(back.state, UploadCommitState::Active));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-runtime --features console console::model`
Expected: compile error — types not defined.

- [ ] **Step 3: Implement the model**

Fill `model.rs` above the tests. Every type: `#[derive(Debug, Clone, Serialize, Deserialize)]`; every enum additionally `#[serde(rename_all = "snake_case")]`; structs that serve as zero-states also derive `Default`.

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResponse {
    pub service: String,   // "xet-console"
    pub version: String,   // env!("CARGO_PKG_VERSION")
    pub pid: u32,
    pub argv: Vec<String>,
    pub start_time_ms: u64,
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub as_of: u64,
    pub pid: u32,
    pub argv: Vec<String>,
    pub start_time_ms: u64, // console server start, not process start
    pub version: String,
    pub n_active_sessions: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionState { Active, Ended }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String, // uuid
    pub state: SessionState,
    pub created_at: u64,
    pub n_upload_commits: usize,
    pub n_download_groups: usize,
    pub n_monitors: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionsResponse {
    pub as_of: u64,
    pub sessions: Vec<SessionSummary>,
    pub ended_sessions: Vec<SessionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub as_of: u64,
    pub id: String,
    pub state: SessionState,
    pub created_at: u64,
    /// Selected config values captured at registration (concurrency settings,
    /// runtime kind, worker threads) as display strings.
    pub config: Vec<(String, String)>,
    pub monitors: Vec<MonitorSnapshot>,
    pub upload_commits: Vec<UploadCommitSummary>,
    pub ended_upload_commits: Vec<UploadCommitDetail>,
    pub download_groups: Vec<DownloadGroupSummary>,
    pub ended_download_groups: Vec<DownloadGroupDetail>,
}

// ---- concurrency monitors ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SuccessModelSnapshot {
    pub success_ratio: f64,
    pub thresholds: (f64, f64),
    pub recommended_adjustment: i8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencyModelSnapshot {
    pub predicted_max_rtt_ms: f64,
    pub rtt_standard_error_ms: f64,
    pub predicted_bandwidth_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorSnapshot {
    pub tag: String,
    pub total_permits: usize,
    pub active_permits: usize,
    pub available_permits: usize,
    pub bounds: (usize, usize),
    pub adjustment_enabled: bool,
    pub bytes_sent: u64,
    pub success: Option<SuccessModelSnapshot>,
    pub latency: Option<LatencyModelSnapshot>,
    /// (epoch_ms, limit) — one entry per adjustment.
    pub limit_history: Vec<(u64, usize)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyResponse {
    pub as_of: u64,
    pub session_id: String,
    pub monitors: Vec<MonitorSnapshot>,
}

// ---- shared progress/dedup ----

/// Mirrors xet_data's GroupProgressReport (which xet_runtime cannot depend on).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProgressSnapshot {
    pub total_bytes: u64,
    pub bytes_completed: u64,
    pub rate_bps: Option<f64>,
    pub transfer_bytes: u64,
    pub transfer_bytes_completed: u64,
    pub transfer_rate_bps: Option<f64>,
}

/// Mirrors xet_data's DeduplicationMetrics byte fields.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DedupSnapshot {
    pub total_bytes: u64,
    pub deduped_bytes: u64,
    pub new_bytes: u64,
    pub deduped_bytes_by_global_dedup: u64,
    pub defrag_prevented_dedup_bytes: u64,
    pub xorb_bytes_uploaded: u64,
    pub shard_bytes_uploaded: u64,
}

// ---- upload side ----

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UploadCommitState { Active, Committing, Completed, Aborted }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCommitSummary {
    pub id: u64,
    pub state: UploadCommitState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub n_files_in_flight: usize,
    pub n_files_completed: u64,
    pub progress: Option<ProgressSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileUploadState {
    Queued, Chunking, Processed, AwaitingXorbs, AwaitingShard, Complete, Failed, Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorbDepSnapshot {
    pub xorb_hash: String,
    pub n_bytes: u64,
    pub uploaded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUploadSnapshot {
    pub id: u64,
    pub name: String,
    pub size: Option<u64>,
    pub state: FileUploadState,
    pub bytes_chunked: u64,
    pub n_chunks: u64,
    pub file_hash: Option<String>,
    pub sha256: Option<String>,
    pub dedup: Option<DedupSnapshot>,
    pub xorb_deps: Vec<XorbDepSnapshot>,
    pub shard_uploaded: bool,
    pub created_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileCounts {
    pub in_flight: usize,
    pub completed: u64,
    pub failed: u64,
    pub aborted: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum XorbState { Formed, Queued, Uploading, Uploaded, Failed }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorbSnapshot {
    pub hash: String,
    pub state: XorbState,
    pub raw_bytes: u64,
    pub serialized_bytes: u64,
    pub bytes_transferred: u64,
    pub n_files: usize,
    pub created_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct XorbCounts {
    pub formed: u64,
    pub uploaded: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct XorbsSnapshot {
    pub in_flight: Vec<XorbSnapshot>,
    pub counts: XorbCounts,
    /// (epoch_ms of completion, snapshot) — recent ring.
    pub recent: Vec<(u64, XorbSnapshot)>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShardState { Staging, Uploading, Uploaded }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshot {
    pub hash: Option<String>, // None for the live staging accumulator entry
    pub state: ShardState,
    pub n_xorbs: usize,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCommitDetail {
    pub as_of: u64,
    pub id: u64,
    pub state: UploadCommitState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub progress: Option<ProgressSnapshot>,
    pub dedup: DedupSnapshot,
    pub files: Vec<FileUploadSnapshot>,
    /// (epoch_ms of completion, final snapshot) — recent ring; ?files=all
    /// returns everything still retained here plus in-flight.
    pub completed_files: Vec<(u64, FileUploadSnapshot)>,
    pub file_counts: FileCounts,
    pub xorbs: XorbsSnapshot,
    pub shards: Vec<ShardSnapshot>,
}

// ---- download side ----

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadGroupKind { Files, Stream }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadGroupState { Active, Finished, Aborted }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadGroupSummary {
    pub id: u64,
    pub kind: DownloadGroupKind,
    pub state: DownloadGroupState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub n_files_in_flight: usize,
    pub n_files_completed: u64,
    pub progress: Option<ProgressSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileDownloadState { Queued, Reconstructing, Complete, Failed, Aborted }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TermState { Enqueued, Fetching, Fetched, Consumed }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermInfo {
    pub xorb_hash: String,
    pub chunk_range: (u32, u32),
    pub byte_range: (u64, u64),
}

/// One prefetch fetch-block (the queue unit in ReconstructionTermManager).
/// Resolves to one or more terms once fetched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermBlockSnapshot {
    pub block_id: u64,
    pub byte_range: (u64, u64),
    pub state: TermState,
    pub terms: Vec<TermInfo>, // populated on Fetched
    pub created_at: u64,
    pub fetched_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PrefetchSnapshot {
    pub queue_depth: usize,
    pub prefetched_byte_position: u64,
    pub active_byte_position: u64,
    pub completion_rate_bps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDownloadSnapshot {
    pub id: u64,
    pub name: String,
    pub file_hash: Option<String>,
    pub requested_range: Option<(u64, u64)>,
    pub total_bytes: u64,
    pub bytes_completed: u64,
    pub state: FileDownloadState,
    pub prefetch: Option<PrefetchSnapshot>,
    pub term_blocks: Vec<TermBlockSnapshot>,
    pub consumed_blocks: u64,
    pub created_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadGroupDetail {
    pub as_of: u64,
    pub id: u64,
    pub kind: DownloadGroupKind,
    pub state: DownloadGroupState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub progress: Option<ProgressSnapshot>,
    pub files: Vec<FileDownloadSnapshot>,
    pub completed_files: Vec<(u64, FileDownloadSnapshot)>,
    pub file_counts: FileCounts,
}

// ---- snapshot (agent one-shot) ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionFull {
    pub detail: SessionDetail,
    pub upload_commit_details: Vec<UploadCommitDetail>,
    pub download_group_details: Vec<DownloadGroupDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub as_of: u64,
    pub process: ProcessInfo,
    pub sessions: Vec<SessionFull>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
```

In `console/mod.rs` add: `pub mod model;`

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-runtime --features console console::model`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/src/console/model.rs xet_runtime/src/console/mod.rs
git commit -m "feat(console): wire model types (API contract)"
```

---

### Task 4: Upload-side live-state cells

`SessionConsole` (the per-session scope), `UploadCommitConsole`, `FileUploadConsole`, `XorbConsole`, shard tracking. Pure in-memory state machines — no server, no instrumentation yet, fully unit-testable.

**Files:**
- Create: `xet_runtime/src/console/state.rs`
- Modify: `xet_runtime/src/console/mod.rs`

- [ ] **Step 1: Write the failing tests**

Create `xet_runtime/src/console/state.rs` with this test module at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::console::model::*;

    fn scope() -> Arc<SessionConsole> {
        SessionConsole::new("test-session".to_string(), vec![])
    }

    #[test]
    fn file_walks_the_upload_state_machine() {
        let commit = UploadCommitConsole::new(Some(&scope()), Some("local://x".into()));
        let f = commit.new_file(12, "model.bin", Some(4096));
        assert_eq!(f.snapshot().state, FileUploadState::Queued);
        f.set_state(FileUploadState::Chunking);
        f.add_chunked_bytes(1024, 2);
        f.set_hash("abcd".into(), None);
        f.set_state(FileUploadState::Processed);
        let snap = f.snapshot();
        assert_eq!(snap.bytes_chunked, 1024);
        assert_eq!(snap.n_chunks, 2);
        assert_eq!(snap.file_hash.as_deref(), Some("abcd"));
    }

    #[test]
    fn xorb_dep_completion_moves_file_to_awaiting_shard() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        let f = commit.new_file(1, "a", None);
        commit.register_file_xorb_dep(1, "x1".into(), 100, false);
        commit.register_file_xorb_dep(1, "x2".into(), 100, true); // external = already uploaded
        f.set_state(FileUploadState::AwaitingXorbs);
        commit.xorb_uploaded("x1", true);
        let snap = f.snapshot();
        assert!(snap.xorb_deps.iter().all(|d| d.uploaded));
        assert_eq!(snap.state, FileUploadState::AwaitingShard);
    }

    #[test]
    fn completed_files_fold_into_ring_and_counts() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        for i in 0..5 {
            let f = commit.new_file(i, "f", None);
            f.set_state(FileUploadState::Complete);
            commit.retire_file(i);
        }
        let detail = commit.snapshot(true);
        assert_eq!(detail.files.len(), 0);
        assert_eq!(detail.file_counts.completed, 5);
        assert_eq!(detail.completed_files.len(), 5);
    }

    #[test]
    fn xorb_lifecycle_and_recent_ring() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        commit.xorb_formed("h1".into(), 1000, 900);
        commit.xorb_state("h1", XorbState::Uploading);
        commit.xorb_transfer("h1", 450);
        commit.xorb_uploaded("h1", true);
        let detail = commit.snapshot(false);
        assert!(detail.xorbs.in_flight.is_empty());
        assert_eq!(detail.xorbs.counts.uploaded, 1);
        assert_eq!(detail.xorbs.recent.len(), 1);
    }

    #[test]
    fn drop_of_active_commit_records_aborted_summary_in_session() {
        let s = scope();
        {
            let commit = UploadCommitConsole::new(Some(&s), None);
            commit.new_file(1, "f", None);
            drop(commit);
        }
        let ended = s.ended_upload_commits();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, UploadCommitState::Aborted);
    }

    #[test]
    fn finalized_commit_records_completed_summary() {
        let s = scope();
        let commit = UploadCommitConsole::new(Some(&s), None);
        commit.set_state(UploadCommitState::Committing);
        commit.finalize(UploadCommitState::Completed);
        drop(commit);
        let ended = s.ended_upload_commits();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, UploadCommitState::Completed);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-runtime --features console console::state`
Expected: compile errors — types not defined.

- [ ] **Step 3: Implement the cells**

Implementation in `state.rs` above the tests. Constants: `RECENT_RING_CAPACITY: usize = 256`, `ENDED_COMMITS_CAPACITY: usize = 64`. Key shapes (write these exactly; bodies follow mechanically from the tests):

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::model::{self, *};
use super::now_ms;
use super::ring::TimestampedRing;

pub const RECENT_RING_CAPACITY: usize = 256;
pub const ENDED_COMMITS_CAPACITY: usize = 64;

pub type ProgressProvider = Box<dyn Fn() -> ProgressSnapshot + Send + Sync>;
/// id -> (total_bytes, bytes_completed); installed by xet_data, reads GroupProgress.
pub type ItemBytesProvider = Box<dyn Fn() -> HashMap<u64, (u64, u64)> + Send + Sync>;

pub struct SessionConsole {
    pub id: String,
    pub created_at: u64,
    pub config: Vec<(String, String)>,
    monitors: Mutex<Vec<Arc<MonitorConsole>>>,        // MonitorConsole defined in Task 5
    upload_commits: Mutex<Vec<Weak<UploadCommitConsole>>>,
    ended_upload_commits: Mutex<TimestampedRing<UploadCommitDetail>>,
    download_groups: Mutex<Vec<Weak<DownloadGroupConsole>>>, // Task 5
    ended_download_groups: Mutex<TimestampedRing<DownloadGroupDetail>>,
}
```

`SessionConsole` API: `new(id, config) -> Arc<Self>`; `live_upload_commits() -> Vec<Arc<UploadCommitConsole>>` (upgrade + prune dead weaks in place); `ended_upload_commits() -> Vec<UploadCommitDetail>`; `record_ended_commit(detail)`; equivalents for download groups and monitors; `summary(state: SessionState) -> SessionSummary`. (Add the monitor/download methods as stubs returning empty in this task if you want to defer to Task 5 — but the fields must exist now so Task 5 only adds the cell types.)

```rust
pub struct UploadCommitConsole {
    pub id: u64, // UniqueId.0 allocated here
    pub created_at: u64,
    pub endpoint: Option<String>,
    state: Mutex<UploadCommitState>,
    progress_provider: Mutex<Option<ProgressProvider>>,
    dedup: Mutex<DedupSnapshot>,
    files: Mutex<HashMap<u64, Arc<FileUploadConsole>>>,
    completed_files: TimestampedRing<FileUploadSnapshot>,
    file_counts: Mutex<FileCounts>,
    // ct_file_id (CompletionTracker id) -> console file id, set by instrumentation
    ct_file_map: Mutex<HashMap<u64, u64>>,
    xorbs: Mutex<HashMap<String, Arc<XorbConsole>>>,
    xorb_counts: Mutex<XorbCounts>,
    recent_xorbs: TimestampedRing<XorbSnapshot>,
    // xorb_hash -> file ids depending on it (for n_files + dep completion fan-out)
    xorb_files: Mutex<HashMap<String, Vec<u64>>>,
    shards: Mutex<Vec<ShardSnapshot>>,
    staging: Mutex<(usize, u64)>, // (n_xorbs staged, approx size)
    session: Weak<SessionConsole>,
    finalized: Mutex<bool>,
}
```

`UploadCommitConsole` API (all `&self`, all lock-poison-tolerant — on poison, return defaults / skip):

- `new(scope: Option<&Arc<SessionConsole>>, endpoint: Option<String>) -> Arc<Self>` — allocates `crate::utils::unique_id::UniqueId::new().0`, registers a `Weak` into the scope's `upload_commits` when `Some`.
- `set_state(UploadCommitState)`, `set_progress_provider(ProgressProvider)`, `set_dedup(DedupSnapshot)` (overwrite — instrumentation pushes the merged aggregate).
- `new_file(id: u64, name: &str, size: Option<u64>) -> Arc<FileUploadConsole>`; `map_ct_file(ct_id: u64, file_id: u64)`.
- `register_file_xorb_dep(ct_or_file_id: u64, xorb_hash: String, n_bytes: u64, is_external: bool)` — resolves through `ct_file_map` (falls back to treating the id as a console file id, which the tests use), appends `XorbDepSnapshot { uploaded: is_external, .. }` to the file, records the file id in `xorb_files`, bumps the xorb's `n_files` if a `XorbConsole` exists.
- `xorb_formed(hash, raw_bytes, serialized_bytes)`, `xorb_state(hash, XorbState)`, `xorb_transfer(hash, completed_bytes)` (stores max, monotonic), `xorb_uploaded(hash, success: bool)` — moves the cell out of `xorbs` into `recent_xorbs` + counts, marks matching deps `uploaded` on every file in `xorb_files[hash]`, and for each file currently `AwaitingXorbs` with all deps uploaded, advances it to `AwaitingShard`.
- `shard_staging(n_xorbs, size)` (overwrite), `shard_discovered(hash, size)` (push `Uploading`), `shard_uploaded(hash)`, `all_shards_uploaded()` — flips every shard to `Uploaded`, sets `shard_uploaded = true` and state `Complete` on all in-flight files, retiring them.
- `retire_file(id)` — remove from `files`, push final snapshot into `completed_files`, bump the matching `file_counts` bucket by final state.
- `finalize(state: UploadCommitState)` — set state, set `finalized`, push `self.snapshot(true)` into the session's ended ring exactly once.
- `snapshot(include_completed: bool) -> UploadCommitDetail` and `summary() -> UploadCommitSummary` (calls the progress provider if set).
- `impl Drop`: if not finalized → `finalize(UploadCommitState::Aborted)`.

```rust
pub struct FileUploadConsole {
    pub id: u64,
    pub name: String,
    pub created_at: u64,
    size: Mutex<Option<u64>>,
    state: Mutex<FileUploadState>,
    bytes_chunked: AtomicU64,
    n_chunks: AtomicU64,
    file_hash: Mutex<Option<String>>,
    sha256: Mutex<Option<String>>,
    dedup: Mutex<Option<DedupSnapshot>>,
    xorb_deps: Mutex<Vec<XorbDepSnapshot>>,
    shard_uploaded: Mutex<bool>,
    finished_at: Mutex<Option<u64>>,
}
```

API: `set_state`, `add_chunked_bytes(n: u64, chunks: u64)`, `set_hash(hash: String, sha256: Option<String>)`, `set_dedup(DedupSnapshot)`, `set_size(u64)`, `snapshot() -> FileUploadSnapshot`. `XorbConsole` mirrors `XorbSnapshot` with `AtomicU64` transfer + `Mutex<XorbState>`.

In `console/mod.rs` add: `pub mod state;`

- [ ] **Step 4: Run tests until green**

Run: `cargo test -p xet-runtime --features console console::state`
Expected: 6 passed. Also run `cargo build -p xet-runtime` (feature OFF) — must still compile untouched.

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/src/console/state.rs xet_runtime/src/console/mod.rs
git commit -m "feat(console): upload-side live state cells"
```

---

### Task 5: Download-side and monitor live-state cells

**Files:**
- Modify: `xet_runtime/src/console/state.rs`

- [ ] **Step 1: Write the failing tests**

Append to the test module in `state.rs`:

```rust
    #[test]
    fn term_block_lifecycle() {
        let group = DownloadGroupConsole::new(Some(&scope()), DownloadGroupKind::Files, None);
        let f = group.new_file(3, "out.bin", Some("ffff".into()), None);
        let b = f.new_term_block(0, (0, 1 << 20));
        assert_eq!(b.snapshot().state, TermState::Enqueued);
        b.set_state(TermState::Fetching);
        b.resolved(vec![TermInfo { xorb_hash: "aa".into(), chunk_range: (0, 4), byte_range: (0, 65536) }]);
        assert_eq!(b.snapshot().state, TermState::Fetched);
        f.consume_term_block(0);
        let snap = f.snapshot(&HashMap::new());
        assert_eq!(snap.consumed_blocks, 1);
        assert!(snap.term_blocks.is_empty());
    }

    #[test]
    fn download_file_bytes_come_from_items_provider() {
        let group = DownloadGroupConsole::new(Some(&scope()), DownloadGroupKind::Files, None);
        let f = group.new_file(9, "x", None, None);
        f.set_state(FileDownloadState::Reconstructing);
        let mut items = HashMap::new();
        items.insert(9u64, (1000u64, 250u64));
        let snap = f.snapshot(&items);
        assert_eq!(snap.total_bytes, 1000);
        assert_eq!(snap.bytes_completed, 250);
    }

    #[test]
    fn group_drop_while_active_is_aborted() {
        let s = scope();
        {
            let g = DownloadGroupConsole::new(Some(&s), DownloadGroupKind::Stream, None);
            g.new_file(1, "f", None, None);
        }
        let ended = s.ended_download_groups();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, DownloadGroupState::Aborted);
        assert_eq!(ended[0].kind, DownloadGroupKind::Stream);
    }

    #[test]
    fn monitor_snapshot_reads_semaphore_gauges_and_history() {
        let sem = crate::utils::adjustable_semaphore::AdjustableSemaphore::new(4, (1, 16));
        let m = MonitorConsole::new("upload".into(), sem, (1, 16), true);
        m.record_limit(5);
        m.record_limit(6);
        m.set_success_model(SuccessModelSnapshot { success_ratio: 0.9, thresholds: (0.8, 0.5), recommended_adjustment: 1 });
        m.set_bytes_sent(1234);
        let snap = m.snapshot();
        assert_eq!(snap.tag, "upload");
        assert_eq!(snap.limit_history.len(), 2);
        assert_eq!(snap.bytes_sent, 1234);
        assert!(snap.success.is_some());
        assert!(snap.total_permits >= 1);
    }
```

Note: check `AdjustableSemaphore::new`'s exact signature and the import path (`xet_runtime/src/utils/adjustable_semaphore.rs`; `XetCommon::new` at `core/common.rs:40-44` constructs one as `AdjustableSemaphore::new(base, (base, limit))` — mirror that). If `new` returns `Arc<Self>` adjust the test accordingly.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-runtime --features console console::state`
Expected: compile errors for the new types.

- [ ] **Step 3: Implement**

Add to `state.rs`:

```rust
pub struct MonitorConsole {
    pub tag: String,
    semaphore: Arc<crate::utils::adjustable_semaphore::AdjustableSemaphore>,
    bounds: (usize, usize),
    adjustment_enabled: bool,
    bytes_sent: AtomicU64,
    success: Mutex<Option<SuccessModelSnapshot>>,
    latency: Mutex<Option<LatencyModelSnapshot>>,
    limit_history: TimestampedRing<usize>,
}
```

API: `new(tag, semaphore, bounds, adjustment_enabled) -> Arc<Self>`, `record_limit(usize)`, `set_success_model(...)`, `set_latency_model(...)`, `set_bytes_sent(u64)` (absolute — the controller's counter is already cumulative), `snapshot() -> MonitorSnapshot` reading `semaphore.total_permits() / available_permits() / active_permits()` live (cast as in `controller.rs:389-401`). Add `SessionConsole::new_monitor(tag, semaphore, bounds, adjustment_enabled) -> Arc<MonitorConsole>` which stores the Arc in `monitors` and returns it, plus `SessionConsole::monitor_snapshots() -> Vec<MonitorSnapshot>`.

```rust
pub struct DownloadGroupConsole {
    pub id: u64,
    pub created_at: u64,
    pub endpoint: Option<String>,
    kind: Mutex<DownloadGroupKind>,
    state: Mutex<DownloadGroupState>,
    progress_provider: Mutex<Option<ProgressProvider>>,
    items_provider: Mutex<Option<ItemBytesProvider>>,
    files: Mutex<HashMap<u64, Arc<DownloadFileConsole>>>,
    completed_files: TimestampedRing<FileDownloadSnapshot>,
    file_counts: Mutex<FileCounts>,
    session: Weak<SessionConsole>,
    finalized: Mutex<bool>,
}

pub struct DownloadFileConsole {
    pub id: u64,
    pub name: String,
    pub created_at: u64,
    file_hash: Mutex<Option<String>>,
    requested_range: Mutex<Option<(u64, u64)>>,
    state: Mutex<FileDownloadState>,
    queue_depth: std::sync::atomic::AtomicUsize,
    prefetched_pos: AtomicU64,
    active_pos: AtomicU64,
    completion_rate: Mutex<Option<f64>>,
    term_blocks: Mutex<HashMap<u64, Arc<TermBlockConsole>>>,
    consumed_blocks: AtomicU64,
    finished_at: Mutex<Option<u64>>,
}

pub struct TermBlockConsole {
    pub block_id: u64,
    pub byte_range: (u64, u64),
    pub created_at: u64,
    state: Mutex<TermState>,
    terms: Mutex<Vec<TermInfo>>,
    fetched_at: Mutex<Option<u64>>,
}
```

APIs mirror the upload side: `DownloadGroupConsole::{new, set_kind, set_state, set_progress_provider, set_items_provider, new_file(id, name, hash, range) -> Arc<DownloadFileConsole>, retire_file(id), finalize(state), snapshot(include_completed) -> DownloadGroupDetail, summary()}` with `Drop` → `Aborted` if unfinalized. `DownloadFileConsole::{set_state, set_prefetch(queue_depth, prefetched, active, rate), new_term_block(block_id, byte_range) -> Arc<TermBlockConsole>, consume_term_block(block_id)}` (consume removes from map + bumps `consumed_blocks`); `snapshot(items: &HashMap<u64,(u64,u64)>)` pulls `(total_bytes, bytes_completed)` from the provider map by `id`. `TermBlockConsole::{set_state, resolved(Vec<TermInfo>)}` (resolved sets state `Fetched`, `fetched_at`, stores terms). Fill in the `SessionConsole` download-group methods stubbed in Task 4, and complete `SessionConsole::detail(state) -> SessionDetail` + `full(state) -> SessionFull` composing monitors + live/ended commits and groups (these are what the server serves in Tasks 7/14).

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-runtime --features console console::state`
Expected: 10 passed (6 from Task 4 + 4 new).

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/src/console/state.rs
git commit -m "feat(console): download-side and concurrency monitor state cells"
```

---

### Task 6: Process-global registry

**Files:**
- Create: `xet_runtime/src/console/registry.rs`
- Modify: `xet_runtime/src/console/mod.rs`

- [ ] **Step 1: Write the failing tests**

Create `registry.rs` with tests:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::console::model::SessionState;

    #[test]
    fn register_list_and_end_session() {
        let reg = Arc::new(ConsoleRegistry::default()); // tests use a local instance, not the global
        let handle = reg.register_session("sess-1".to_string(), vec![]);
        assert_eq!(reg.session_summaries().0.len(), 1);
        assert_eq!(reg.session("sess-1").unwrap().id, "sess-1");
        drop(handle);
        let (active, ended) = reg.session_summaries();
        assert!(active.is_empty());
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, SessionState::Ended);
    }

    #[test]
    fn ended_sessions_are_bounded() {
        let reg = Arc::new(ConsoleRegistry::default());
        for i in 0..(ENDED_SESSIONS_CAPACITY + 4) {
            let handle = reg.register_session(format!("s{i}"), vec![]);
            drop(handle);
        }
        assert_eq!(reg.session_summaries().1.len(), ENDED_SESSIONS_CAPACITY);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-runtime --features console console::registry`
Expected: compile error.

- [ ] **Step 3: Implement**

```rust
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};

use super::model::{SessionState, SessionSummary};
use super::ring::TimestampedRing;
use super::state::SessionConsole;

pub const ENDED_SESSIONS_CAPACITY: usize = 16;

pub struct ConsoleRegistry {
    sessions: RwLock<HashMap<String, Weak<SessionConsole>>>,
    ended_sessions: Mutex<TimestampedRing<SessionSummary>>,
}

impl Default for ConsoleRegistry {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            ended_sessions: Mutex::new(TimestampedRing::new(ENDED_SESSIONS_CAPACITY)),
        }
    }
}

static CONSOLE_REGISTRY: LazyLock<Arc<ConsoleRegistry>> = LazyLock::new(|| Arc::new(ConsoleRegistry::default()));

pub fn registry() -> Arc<ConsoleRegistry> {
    CONSOLE_REGISTRY.clone()
}

/// Owns a registered session scope. Held by XetCommon.console_session; on drop
/// (session graph released) it reports the ended summary and prunes the weak entry.
pub struct SessionHandle {
    pub scope: Arc<SessionConsole>,
    registry: Option<Arc<ConsoleRegistry>>, // None for detached test scopes
}

impl SessionHandle {
    /// Test-only scope with no registry reporting.
    pub fn detached(scope: Arc<SessionConsole>) -> Self {
        Self { scope, registry: None }
    }
}

impl Drop for SessionHandle {
    fn drop(&mut self) {
        let Some(reg) = &self.registry else { return };
        reg.record_ended_session(self.scope.summary(SessionState::Ended));
        if let Ok(mut s) = reg.sessions.write() {
            s.remove(&self.scope.id);
        }
    }
}
```

`ConsoleRegistry` API (all on `impl ConsoleRegistry`): `register_session(self: &Arc<Self>, id: String, config: Vec<(String, String)>) -> SessionHandle` (creates the `SessionConsole`, stores a `Weak`, prunes dead entries, returns the handle with `registry: Some(self.clone())`); `session(id: &str) -> Option<Arc<SessionConsole>>`; `live_sessions() -> Vec<Arc<SessionConsole>>`; `session_summaries() -> (Vec<SessionSummary>, Vec<SessionSummary>)` (active, ended — ended from the ring with `state: Ended`); `record_ended_session(SessionSummary)`. No `Drop` on `SessionConsole` itself — the handle owns end-of-life.

In `console/mod.rs` add: `pub mod registry;`

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-runtime --features console console::registry`
Expected: 2 passed (and `console::state` still green).

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/src/console/registry.rs xet_runtime/src/console/mod.rs
git commit -m "feat(console): process-global session registry with bounded ended-session ring"
```

---

### Task 7: The axum server

**Files:**
- Create: `xet_runtime/src/console/server.rs`
- Create: `xet_runtime/tests/console_server.rs`
- Modify: `xet_runtime/src/console/mod.rs`
- Modify: `xet_runtime/Cargo.toml` (dev-deps if reqwest/serial_test not already present — reqwest is a normal dep already; add `serial_test = { workspace = true }` to `[dev-dependencies]`)

- [ ] **Step 1: Write the failing integration test**

Create `xet_runtime/tests/console_server.rs`:

```rust
#![cfg(feature = "console")]

use serial_test::serial;
use xet_runtime::console::registry::registry;
use xet_runtime::console::server;

#[test]
#[serial]
fn server_serves_index_process_and_sessions() {
    // SAFETY-NOTE: env mutation is why this test is #[serial].
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") }; // ephemeral for tests
    server::ensure_started();
    let addr = server::bound_addr().expect("server should have bound an ephemeral port");

    let _session = registry().register_session("itest-session".into(), vec![]);

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let base = format!("http://{addr}");
        let index: serde_json::Value =
            reqwest::get(format!("{base}/")).await.unwrap().json().await.unwrap();
        assert_eq!(index["service"], "xet-console");
        assert!(index["endpoints"].as_array().unwrap().len() >= 9);

        let process: serde_json::Value =
            reqwest::get(format!("{base}/api/v1/process")).await.unwrap().json().await.unwrap();
        assert_eq!(process["pid"], std::process::id());
        assert!(process["as_of"].as_u64().unwrap() > 0);

        let sessions: serde_json::Value =
            reqwest::get(format!("{base}/api/v1/sessions")).await.unwrap().json().await.unwrap();
        let listed = sessions["sessions"].as_array().unwrap();
        assert!(listed.iter().any(|s| s["id"] == "itest-session"));

        let detail = reqwest::get(format!("{base}/api/v1/sessions/itest-session")).await.unwrap();
        assert!(detail.status().is_success());

        let missing = reqwest::get(format!("{base}/api/v1/sessions/nope")).await.unwrap();
        assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    });
}
```

(If the env API in the workspace edition requires no `unsafe`, drop the block; match whatever `std::env::set_var` requires under edition 2024.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-runtime --features console --test console_server`
Expected: compile error — `server` module missing.

- [ ] **Step 3: Implement the server**

`xet_runtime/src/console/server.rs`:

```rust
use std::net::SocketAddr;
use std::sync::OnceLock;

use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use tracing::{info, warn};

use super::model::*;
use super::now_ms;
use super::registry::registry;

pub const DEFAULT_CONSOLE_PORT: u16 = 6660;

static STARTED: OnceLock<()> = OnceLock::new();
static BOUND_ADDR: OnceLock<Option<SocketAddr>> = OnceLock::new();
static SERVER_START_MS: OnceLock<u64> = OnceLock::new();

/// Idempotent. Spawns the console server on a dedicated thread with its own
/// current-thread runtime, deliberately independent of any session's runtime.
/// Strict port policy: bind XET_CONSOLE_PORT (default 6660, 0 = ephemeral);
/// on failure, warn once and run without a console. Loopback only.
pub fn ensure_started() {
    STARTED.get_or_init(|| {
        SERVER_START_MS.get_or_init(now_ms);
        let port = match std::env::var("XET_CONSOLE_PORT") {
            Ok(v) => match v.parse::<u16>() {
                Ok(p) => p,
                Err(_) => {
                    warn!("xet-console: invalid XET_CONSOLE_PORT={v}, console disabled");
                    let _ = BOUND_ADDR.set(None);
                    return;
                },
            },
            Err(_) => DEFAULT_CONSOLE_PORT,
        };
        let std_listener = match std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)) {
            Ok(l) => l,
            Err(e) => {
                warn!("xet-console: failed to bind 127.0.0.1:{port}: {e}; console disabled");
                let _ = BOUND_ADDR.set(None);
                return;
            },
        };
        let addr = match std_listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                warn!("xet-console: no local addr: {e}; console disabled");
                let _ = BOUND_ADDR.set(None);
                return;
            },
        };
        if std_listener.set_nonblocking(true).is_err() {
            warn!("xet-console: failed to set nonblocking; console disabled");
            let _ = BOUND_ADDR.set(None);
            return;
        }
        let _ = BOUND_ADDR.set(Some(addr));
        info!("xet-console listening on http://{addr}");
        std::thread::Builder::new()
            .name("xet-console-server".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        warn!("xet-console: runtime build failed: {e}");
                        return;
                    },
                };
                rt.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(std_listener) {
                        Ok(l) => l,
                        Err(e) => {
                            warn!("xet-console: listener conversion failed: {e}");
                            return;
                        },
                    };
                    if let Err(e) = axum::serve(listener, router()).await {
                        warn!("xet-console: server exited: {e}");
                    }
                });
            })
            .map_err(|e| warn!("xet-console: thread spawn failed: {e}"))
            .ok();
    });
}

/// Some(addr) once bound; None if startup failed or never attempted.
pub fn bound_addr() -> Option<SocketAddr> {
    BOUND_ADDR.get().copied().flatten()
}

fn router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/v1/process", get(process_info))
        .route("/api/v1/sessions", get(sessions))
        .route("/api/v1/sessions/{sid}", get(session_detail))
        .route("/api/v1/sessions/{sid}/uploads", get(uploads))
        .route("/api/v1/sessions/{sid}/uploads/{cid}", get(upload_detail))
        .route("/api/v1/sessions/{sid}/downloads", get(downloads))
        .route("/api/v1/sessions/{sid}/downloads/{gid}", get(download_detail))
        .route("/api/v1/sessions/{sid}/concurrency", get(concurrency))
        .route("/api/v1/snapshot", get(snapshot))
}
```

Handlers (same file): each is a small async fn returning `Json<T>` or `(StatusCode, Json<ErrorResponse>)`. `index` lists the ten endpoint paths and pid; `process_info` fills `ProcessInfo { as_of: now_ms(), pid: std::process::id(), argv: std::env::args().collect(), start_time_ms: *SERVER_START_MS.get().unwrap_or(&0), version: env!("CARGO_PKG_VERSION").into(), n_active_sessions: registry().live_sessions().len() }`; `sessions` returns `SessionsResponse`; `session_detail`/`uploads`/`upload_detail`/`downloads`/`download_detail`/`concurrency` resolve `registry().session(&sid)` (404 `ErrorResponse { error: format!("no session {sid}") }` when absent, 404 for unknown commit/group ids) and call the `SessionConsole`/cell snapshot methods from Tasks 4-5. `upload_detail` and `download_detail` accept `Query<HashMap<String, String>>` and pass `include_completed = true` always; when `files=all` is absent they still include the rings (the rings ARE the bound — `?files=all` is accepted and currently equivalent, kept for API stability). `snapshot` composes `SnapshotResponse` from `SessionConsole::full(...)` for every live session, honoring `?session={sid}` filter. A shared helper keeps handlers tiny:

```rust
fn with_session<T: serde::Serialize>(
    sid: &str,
    f: impl FnOnce(std::sync::Arc<super::state::SessionConsole>) -> Option<T>,
) -> Result<Json<T>, (StatusCode, Json<ErrorResponse>)> {
    registry()
        .session(sid)
        .and_then(f)
        .map(Json)
        .ok_or_else(|| (StatusCode::NOT_FOUND, Json(ErrorResponse { error: format!("not found under session {sid}") })))
}
```

In `console/mod.rs` add: `pub mod server;`

- [ ] **Step 4: Run the integration test**

Run: `cargo test -p xet-runtime --features console --test console_server`
Expected: PASS. Also `cargo clippy -p xet-runtime --features console -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/src/console/server.rs xet_runtime/tests/console_server.rs xet_runtime/src/console/mod.rs xet_runtime/Cargo.toml
git commit -m "feat(console): axum server on dedicated thread, strict port policy"
```

---

### Task 8: Session wiring — XetCommon scope cell + XetSession registration

**Files:**
- Modify: `xet_runtime/src/core/common.rs`
- Modify: `xet_pkg/src/xet_session/session.rs:238-249` (`XetSession::new`)
- Create: `xet_pkg/tests/test_console.rs`

- [ ] **Step 1: Add the scope cell to XetCommon**

In `xet_runtime/src/core/common.rs`, add to the struct (`common.rs:7`):

```rust
    /// xet-console scope for the session owning this XetCommon. Set once by
    /// XetSession::new when the console feature is active; read by components
    /// (upload/download sessions, concurrency controllers) via ctx.common.
    #[cfg(feature = "console")]
    pub console_session: std::sync::OnceLock<crate::console::registry::SessionHandle>,
```

and to `XetCommon::new` (`common.rs:36`):

```rust
            #[cfg(feature = "console")]
            console_session: std::sync::OnceLock::new(),
```

Add a convenience accessor on `XetCommon`:

```rust
#[cfg(feature = "console")]
impl XetCommon {
    /// The console scope, if this XetCommon belongs to a console-registered session.
    pub fn console_scope(&self) -> Option<&std::sync::Arc<crate::console::state::SessionConsole>> {
        self.console_session.get().map(|h| &h.scope)
    }
}
```

(`SessionHandle.scope` must be `pub` — it is, per Task 6.)

- [ ] **Step 2: Register the session in XetSession::new**

In `xet_pkg/src/xet_session/session.rs`, `XetSession::new` (line ~238) currently builds `inner` (which generates the uuid inline) and returns. Restructure so the id exists before inner construction, then register:

```rust
fn new(ctx: XetContext) -> Self {
    let task_runtime = TaskRuntime::new_root(ctx.runtime.clone());
    let id = Uuid::now_v7();
    #[cfg(feature = "console")]
    {
        use xet_runtime::console::{registry::registry, server};
        server::ensure_started();
        let config = vec![
            ("max_concurrent_file_ingestion".to_string(), ctx.config.data.max_concurrent_file_ingestion.to_string()),
            ("max_concurrent_file_downloads".to_string(), ctx.config.data.max_concurrent_file_downloads.to_string()),
        ];
        let _ = ctx.common.console_session.set(registry().register_session(id.to_string(), config));
    }
    Self {
        inner: Arc::new(XetSessionInner {
            ctx,
            task_runtime,
            #[cfg(not(target_family = "wasm"))]
            active_download_stream_groups: Mutex::new(HashMap::new()),
            id,
        }),
    }
}
```

The exact config field names: confirm with `grep -rn "max_concurrent_file" xet_runtime/src/config/` and use what `XetCommon::new` reads (`common.rs:38-39`). Also append a best-effort `("runtime", …)` entry describing the runtime (owned vs external, worker threads) if `XetRuntime` exposes that cheaply — check `xet_runtime/src/core/runtime.rs` for an existing accessor; skip the entry if none exists (the spec places runtime kind in the per-session config snapshot). The lifecycle is automatic: when the session's graph drops, `XetCommon` drops, the `SessionHandle` drops, and the registry records the ended summary — exactly the spec's "ended sessions retained, bounded".

- [ ] **Step 3: Write the failing end-to-end registration test**

Create `xet_pkg/tests/test_console.rs`:

```rust
#![cfg(feature = "console")]

use serial_test::serial;
use xet_pkg::xet_session::XetSessionBuilder; // match the import used in xet_pkg/tests/test_xet_session.rs

#[test]
#[serial]
fn session_appears_in_console_and_ends_on_drop() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let session = XetSessionBuilder::new().build().unwrap();
    let addr = xet_runtime::console::server::bound_addr().expect("console server bound");
    let base = format!("http://{addr}");

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let sessions: serde_json::Value =
        rt.block_on(async { reqwest::get(format!("{base}/api/v1/sessions")).await.unwrap().json().await.unwrap() });
    assert!(
        sessions["sessions"].as_array().unwrap().iter().any(|s| s["state"] == "active"),
        "expected at least one active session, got: {sessions}"
    );

    drop(session);
    let sessions: serde_json::Value =
        rt.block_on(async { reqwest::get(format!("{base}/api/v1/sessions")).await.unwrap().json().await.unwrap() });
    assert!(
        sessions["ended_sessions"].as_array().unwrap().iter().any(|s| s["state"] == "ended"),
        "expected an ended session, got: {sessions}"
    );
}
```

Add to `xet_pkg/Cargo.toml` `[dev-dependencies]` if missing: `serial_test = { workspace = true }`, `reqwest = { workspace = true }`, `serde_json = { workspace = true }` (check first — several are likely present; `tokio` and `tempfile` already are, given existing tests). Check the exact `XetSessionBuilder` import path at the top of `xet_pkg/tests/test_xet_session.rs` and mirror it.

Note: the session drop → ended transition relies on the session graph releasing `XetCommon`. If the assertion is flaky because background drops are asynchronous, poll the endpoint for up to ~2s before failing.

- [ ] **Step 4: Run**

Run: `cargo test -p xet-pkg --features console --test test_console`
Expected: PASS. Then run the full existing suite to prove no regression with the feature off: `cargo test -p xet-pkg --test test_xet_session`
Expected: all pass, console code not even compiled.

- [ ] **Step 5: Commit**

```bash
git add xet_runtime/src/core/common.rs xet_pkg/src/xet_session/session.rs xet_pkg/tests/test_console.rs xet_pkg/Cargo.toml
git commit -m "feat(console): register XetSession scopes; lazily start server"
```

---

### Task 9: Concurrency monitor instrumentation (xet_client)

**Files:**
- Modify: `xet_client/src/cas_client/adaptive_concurrency/controller.rs`
- Modify: `xet_runtime/tests/console_server.rs` (extend) or assert via xet_data test in Task 10 — primary unit test lives next to the controller.

- [ ] **Step 1: Write the failing test**

In `controller.rs`'s existing `#[cfg(test)]` module (or a new one), add:

```rust
#[cfg(all(test, feature = "console"))]
mod console_tests {
    use super::*;
    use xet_runtime::console::state::SessionConsole;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_registers_monitor_in_session_scope() {
        let ctx = XetContext::with_config(XetConfig::default()).unwrap(); // mirror how other tests in this file build a ctx — copy their helper if one exists
        let scope = SessionConsole::new("monitor-test".into(), vec![]);
        // Simulate XetSession wiring: install a handle-less scope for the test.
        ctx.common.console_session_for_test(scope.clone());

        let _controller = AdaptiveConcurrencyController::new_upload(ctx, "upload");
        let monitors = scope.monitor_snapshots();
        assert_eq!(monitors.len(), 1);
        assert_eq!(monitors[0].tag, "upload");
        assert!(monitors[0].total_permits > 0);
    }
}
```

This needs a test-only setter since `console_session` holds a `SessionHandle`, not a bare scope. Add to `xet_runtime/src/core/common.rs`:

```rust
#[cfg(feature = "console")]
impl XetCommon {
    /// Test-only: install a scope without a registry handle.
    pub fn console_session_for_test(&self, scope: std::sync::Arc<crate::console::state::SessionConsole>) {
        let _ = self.console_session.set(crate::console::registry::SessionHandle::detached(scope));
    }
}
```

and a `SessionHandle::detached(scope) -> SessionHandle` constructor in `registry.rs` whose `Drop` skips registry reporting (store `registry: Option<Arc<ConsoleRegistry>>`).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-client --features console console_tests`
Expected: compile error (no monitor field / no registration).

- [ ] **Step 3: Instrument the controller**

In `AdaptiveConcurrencyController` (struct at `controller.rs:261`):

```rust
    #[cfg(feature = "console")]
    monitor: Option<std::sync::Arc<xet_runtime::console::state::MonitorConsole>>,
```

In `new(...)` (line ~288), after the semaphore is constructed and before `Arc::new(Self { … })`:

```rust
        #[cfg(feature = "console")]
        let monitor = ctx.common.console_scope().map(|scope| {
            scope.new_monitor(
                logging_tag.to_string(),
                concurrency_semaphore.clone(),
                concurrency_bounds,
                !adjustment_disabled,
            )
        });
```

and include `#[cfg(feature = "console")] monitor,` in the struct literal. (`new_fixed` flows through `new`-equivalent construction — verify; if it builds the struct separately, add the same block there with `adjustment_disabled = true`.)

Hook the adjustment sites:

- Increase (~`controller.rs:522-555`), right after `state_lg.last_adjustment_time = Instant::now();` inside the `if predicted_rtt < target_rtt_secs` branch:

```rust
                #[cfg(feature = "console")]
                if let Some(m) = &self.monitor {
                    m.record_limit(new_concurrency_actual as usize);
                }
```

- Decrease (~`controller.rs:557-591`), after `state_lg.last_adjustment_time = Instant::now();`:

```rust
            #[cfg(feature = "console")]
            if let Some(m) = &self.monitor {
                m.record_limit(new_concurrency as usize);
            }
```

- Model push: in the same function where `model_state: CCSuccessModelState` is computed (the code that reads `model_state.recommended_adjustment` at line ~522 — the computation is a few lines above), add right after it:

```rust
        #[cfg(feature = "console")]
        if let Some(m) = &self.monitor {
            m.set_success_model(xet_runtime::console::model::SuccessModelSnapshot {
                success_ratio: model_state.success_ratio,
                thresholds: model_state.success_ratio_thresholds,
                recommended_adjustment: model_state.recommended_adjustment,
            });
            m.set_bytes_sent(state_lg.bytes_sent_so_far);
        }
```

- Latency push: find where `CCLatencyModelState` is constructed (`grep -n "CCLatencyModelState {" xet_client/src/cas_client/adaptive_concurrency/controller.rs` — it exists; the type is pub and built from `rtt_predictor`). Tee at that construction site:

```rust
        #[cfg(feature = "console")]
        if let Some(m) = &self.monitor {
            m.set_latency_model(xet_runtime::console::model::LatencyModelSnapshot {
                predicted_max_rtt_ms: latency_state.predicted_max_rtt,
                rtt_standard_error_ms: latency_state.prediction_max_rtt_standard_error,
                predicted_bandwidth_bps: latency_state.predicted_bandwidth,
            });
        }
```

(bind the constructed value to `latency_state` if it isn't already named; if it is only built inside a logging branch, hoist the construction so the console sees it whenever it's computed — do NOT compute it more often than today.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-client --features console`
Expected: new test passes, all existing controller tests pass. Also `cargo build -p xet-client` (feature off) — clean.

- [ ] **Step 5: Commit**

```bash
git add xet_client/src/cas_client/adaptive_concurrency/controller.rs xet_runtime/src/core/common.rs xet_runtime/src/console/registry.rs xet_runtime/src/console/state.rs
git commit -m "feat(console): instrument adaptive concurrency controllers"
```

---

### Task 10: Upload commit instrumentation (xet_data) + uploads endpoints live

**Files:**
- Modify: `xet_data/src/processing/file_upload_session.rs`
- Modify: `xet_pkg/src/xet_session/upload_commit.rs` (abort hook)
- Create: `xet_data/tests/console_common/mod.rs`
- Create: `xet_data/tests/test_console_upload.rs`

- [ ] **Step 1: Write the failing test**

Create the shared helper module and the test file — both use the same `TestEnvironment` infrastructure as `xet_data/tests/test_full_file_download.rs` (read its imports and copy them; it provides `env.config` and `env.base_dir`):

```rust
#![cfg(feature = "console")]

// ---- xet_data/tests/console_common/mod.rs (shared by both console test files) ----
#![cfg(feature = "console")]
use std::sync::Arc;

use xet_runtime::console::registry::registry;
use xet_runtime::console::state::SessionConsole;
// + the TestEnvironment / TranslatorConfig / FileUploadSession / Sha256Policy /
//   XetFileInfo imports copied from xet_data/tests/test_full_file_download.rs

/// Simulate the session scope that xet_pkg normally installs (Task 8).
fn install_scope(config: &TranslatorConfig) -> Arc<SessionConsole> {
    let id = format!("itest-{}", xet_runtime::utils::unique_id::UniqueId::new().0);
    let handle = registry().register_session(id, vec![]);
    let scope = handle.scope.clone();
    config.ctx.common.console_session.set(handle).ok();
    scope
}

/// Write `size` random bytes to a file and ingest it; returns its XetFileInfo.
async fn upload_random_file(session: &Arc<FileUploadSession>, dir: &std::path::Path, size: usize) -> XetFileInfo {
    use rand::RngCore;
    let path = dir.join(format!("file_{size}.bin"));
    let mut data = vec![0u8; size];
    rand::rng().fill_bytes(&mut data);
    std::fs::write(&path, &data).unwrap();
    let (_id, handle) = session.spawn_upload_from_path(path, Sha256Policy::Compute).await.unwrap();
    handle.await.unwrap().unwrap().0
}

// ---- xet_data/tests/test_console_upload.rs ----
mod console_common;
use console_common::{install_scope, upload_random_file};
use serial_test::serial;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn upload_commit_lifecycle_visible_in_console() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();

    // Live commit appears under the scope.
    let commits = scope.live_upload_commits();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].summary().state, xet_runtime::console::model::UploadCommitState::Active);

    // Upload a file and finalize.
    let _xfi = upload_random_file(&upload_session, &env.base_dir, 1 << 20).await;
    upload_session.finalize().await.unwrap();

    // Commit is finalized: moved to ended ring with Completed state and dedup totals.
    assert!(scope.live_upload_commits().is_empty());
    let ended = scope.ended_upload_commits();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].state, xet_runtime::console::model::UploadCommitState::Completed);
    assert!(ended[0].dedup.total_bytes > 0);
    assert!(ended[0].progress.is_some());
}
```

Add `serial_test = { workspace = true }` and `rand = { workspace = true }` to `xet_data` `[dev-dependencies]` if missing (check `env.base_dir`'s type on `TestEnvironment` — adjust the helper's parameter to match).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_upload`
Expected: compile/assert failure — no console wiring exists.

- [ ] **Step 3: Instrument FileUploadSession**

Struct field (`file_upload_session.rs:38`):

```rust
    #[cfg(feature = "console")]
    pub(crate) console: Option<std::sync::Arc<xet_runtime::console::state::UploadCommitConsole>>,
```

In `FileUploadSession::new`, after `progress` is created and before the struct literal (endpoint: read the same field `create_remote_client` reads — `config.session.endpoint`):

```rust
        #[cfg(feature = "console")]
        let console = ctx.common.console_scope().map(|scope| {
            let c = xet_runtime::console::state::UploadCommitConsole::new(
                Some(scope),
                Some(config.session.endpoint.clone()),
            );
            let p = progress.clone();
            c.set_progress_provider(Box::new(move || {
                let r = p.report();
                xet_runtime::console::model::ProgressSnapshot {
                    total_bytes: r.total_bytes,
                    bytes_completed: r.total_bytes_completed,
                    rate_bps: r.total_bytes_completion_rate,
                    transfer_bytes: r.total_transfer_bytes,
                    transfer_bytes_completed: r.total_transfer_bytes_completed,
                    transfer_rate_bps: r.total_transfer_bytes_completion_rate,
                }
            }));
            c
        });
```

(If `config.session.endpoint` is an `Option` or differently named, mirror exactly what `create_remote_client` at `xet_data/src/processing/remote_client_interface.rs:8-39` accesses.)

Dedup aggregate push — find where per-file metrics merge into `self.deduplication_metrics` (`register_single_file_clean_completion`; grep `deduplication_metrics.lock` in this file). After each merge:

```rust
        #[cfg(feature = "console")]
        if let Some(c) = &self.console {
            let m = *self.deduplication_metrics.lock().await;
            c.set_dedup(dedup_snapshot_from(&m));
        }
```

with a small free fn in this file:

```rust
#[cfg(feature = "console")]
fn dedup_snapshot_from(m: &DeduplicationMetrics) -> xet_runtime::console::model::DedupSnapshot {
    xet_runtime::console::model::DedupSnapshot {
        total_bytes: m.total_bytes,
        deduped_bytes: m.deduped_bytes,
        new_bytes: m.new_bytes,
        deduped_bytes_by_global_dedup: m.deduped_bytes_by_global_dedup,
        defrag_prevented_dedup_bytes: m.defrag_prevented_dedup_bytes,
        xorb_bytes_uploaded: m.xorb_bytes_uploaded,
        shard_bytes_uploaded: m.shard_bytes_uploaded,
    }
}
```

Finalize hooks in `finalize_impl` (~line 545): at entry (right after the `finalized.swap` guard):

```rust
        #[cfg(feature = "console")]
        if let Some(c) = &self.console {
            c.set_state(xet_runtime::console::model::UploadCommitState::Committing);
        }
```

at the end, right before `Ok((metrics, all_file_info, report))`:

```rust
        #[cfg(feature = "console")]
        if let Some(c) = &self.console {
            c.set_dedup(dedup_snapshot_from(&metrics));
            c.finalize(xet_runtime::console::model::UploadCommitState::Completed);
        }
```

Abort: `UploadCommitConsole::Drop` already records Aborted for never-finalized commits (Task 4), which covers `XetUploadCommit::abort` (`upload_commit.rs:306`) when the `FileUploadSession` drops. For a *prompt* abort signal, also add in `XetUploadCommit::abort`, after `cancel_subtree()`:

```rust
        #[cfg(feature = "console")]
        if let Some(c) = &self.inner.upload_session.console {
            c.set_state(xet_runtime::console::model::UploadCommitState::Aborted);
        }
```

(`console` is `pub(crate)` in xet_data — xet_pkg is a different crate, so instead add a tiny pub method on `FileUploadSession`: `#[cfg(feature = "console")] pub fn console_mark_aborted(&self)` that does the set_state, and call that.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_upload`
Expected: PASS. Run existing upload tests for regression: `cargo test -p xet-data --features simulation processing`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add xet_data/src/processing/file_upload_session.rs xet_pkg/src/xet_session/upload_commit.rs xet_data/tests/console_common/mod.rs xet_data/tests/test_console_upload.rs xet_data/Cargo.toml
git commit -m "feat(console): upload commit lifecycle, progress provider, dedup aggregate"
```

---

### Task 11: Per-file upload pipeline instrumentation

**Files:**
- Modify: `xet_data/src/processing/file_upload_session.rs` (`start_clean`)
- Modify: `xet_data/src/processing/file_cleaner.rs`
- Modify: `xet_data/src/deduplication/file_deduplication.rs` (metrics accessor)
- Modify: `xet_data/src/progress_tracking/progress_types.rs` (ItemProgressUpdater id accessor)
- Modify: `xet_data/tests/test_console_upload.rs`

- [ ] **Step 1: Write the failing test**

Add to `test_console_upload.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn file_states_progress_through_pipeline() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
    let commit = scope.live_upload_commits().pop().unwrap();

    // >= 1 MiB so chunking is observable
    let _xfi = upload_random_file(&upload_session, &env.base_dir, 4 << 20).await;

    // After ingestion (pre-finalize): the file must be past Chunking with hash known.
    let detail = commit.snapshot(true);
    let all_files: Vec<_> = detail.files.iter().cloned()
        .chain(detail.completed_files.iter().map(|(_, f)| f.clone()))
        .collect();
    assert_eq!(all_files.len(), 1);
    let f = &all_files[0];
    assert!(f.file_hash.is_some(), "hash must be known after processing");
    assert!(f.bytes_chunked > 0);
    assert!(f.n_chunks > 0);
    assert!(f.dedup.is_some());
    assert!(
        matches!(f.state, FileUploadState::Processed | FileUploadState::AwaitingXorbs | FileUploadState::AwaitingShard | FileUploadState::Complete),
        "unexpected state: {:?}", f.state
    );

    upload_session.finalize().await.unwrap();
    let ended = scope.ended_upload_commits().pop().unwrap();
    let (_, f) = ended.completed_files.last().unwrap();
    assert_eq!(f.state, FileUploadState::Complete);
    assert!(f.shard_uploaded);
}
```

(Import `FileUploadState` from `xet_runtime::console::model`.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_upload file_states`
Expected: FAIL — no files tracked.

- [ ] **Step 3: Add the two tiny accessors**

`xet_data/src/deduplication/file_deduplication.rs` (struct at line 58 owns `deduplication_metrics`):

```rust
    /// Running metrics merged so far (console instrumentation).
    pub fn current_metrics(&self) -> DeduplicationMetrics {
        self.deduplication_metrics
    }
```

`xet_data/src/progress_tracking/progress_types.rs`, on `impl ItemProgressUpdater`:

```rust
    pub fn item_id(&self) -> UniqueId {
        self.item.id
    }
```

- [ ] **Step 4: Hook start_clean and the cleaner**

In `file_upload_session.rs::start_clean` (the fn returning `(UniqueId, SingleFileCleaner)` — used at `file_upload_session.rs:248`), where the id, tracking name, size, and the `ItemProgressUpdater`/`CompletionTrackerFileId` are created, add:

```rust
        #[cfg(feature = "console")]
        let file_console = self.console.as_ref().map(|c| {
            let fc = c.new_file(id.0, tracking_name.as_deref().unwrap_or("<unnamed>"), file_size);
            c.map_ct_file(file_id /* CompletionTrackerFileId as u64 */, id.0);
            fc
        });
```

(`CompletionTrackerFileId` — check its definition via `grep -n "type CompletionTrackerFileId\|struct CompletionTrackerFileId" xet_data/src`; if it's a newtype, take its inner u64 the same way as `UniqueId.0`.) Pass `file_console` into `SingleFileCleaner` — add the field:

```rust
    #[cfg(feature = "console")]
    console: Option<std::sync::Arc<xet_runtime::console::state::FileUploadConsole>>,
```

In `SingleFileCleaner::add_data_chunk_impl` (the per-block path under `add_data_from_bytes`, `file_cleaner.rs:141`), at the top:

```rust
        #[cfg(feature = "console")]
        if let Some(fc) = &self.console {
            fc.set_state(xet_runtime::console::model::FileUploadState::Chunking); // idempotent
            fc.add_chunked_bytes(data.len() as u64, 0);
        }
```

and where chunks are handed to the deduper (the `deduper_process_chunks` call site — chunk count is the slice length there), bump chunks + refresh dedup:

```rust
        #[cfg(feature = "console")]
        if let Some(fc) = &self.console {
            fc.add_chunked_bytes(0, chunks.len() as u64);
        }
```

Live per-file dedup needs the deduper, which lives behind `dedup_manager_fut` and is only owned at await points; refresh it wherever the code already awaits the deduper to process chunks (inside `deduper_process_chunks` after the await):

```rust
        #[cfg(feature = "console")]
        if let Some(fc) = &self.console {
            fc.set_dedup(crate::processing::file_upload_session::dedup_snapshot_from(&deduper.current_metrics()));
        }
```

(make `dedup_snapshot_from` `pub(crate)`; bind `deduper` to whatever local the awaited manager is called there.)

In `finish_inner` (`file_cleaner.rs:219`), right after `let file_info = XetFileInfo { … }`:

```rust
        #[cfg(feature = "console")]
        if let Some(fc) = &self.console {
            fc.set_hash(file_info.hash.clone(), file_info.sha256.clone());
            fc.set_size(deduplication_metrics.total_bytes);
            fc.set_dedup(crate::processing::file_upload_session::dedup_snapshot_from(&deduplication_metrics));
            fc.set_state(xet_runtime::console::model::FileUploadState::Processed);
        }
```

Note `finish_inner` consumes `self` but `self.console` is moved out before the `dedup_manager_fut.await` if borrow issues arise — clone the `Option<Arc<…>>` up front (`let console = self.console.clone();`).

- [ ] **Step 5: Hook dependency registration and state advancement**

Find call sites: `grep -rn "register_dependencies" xet_data/src --include='*.rs'`. At each site inside `FileUploadSession` (or reachable from it where `self.console` is available), after the existing call:

```rust
        #[cfg(feature = "console")]
        if let Some(c) = &self.console {
            for dep in &dependencies {
                c.register_file_xorb_dep(dep.file_id, dep.xorb_hash.hex(), dep.n_bytes, dep.is_external);
            }
        }
```

If a call site lives outside `FileUploadSession` (e.g. inside `CompletionTracker` callers in the aggregation path), prefer hooking the single choke point where the `Vec<FileXorbDependency>` is built and the session is in scope. Also extend `UploadCommitConsole::register_file_xorb_dep` (Task 4) so that after appending a dep to a file in `Processed` state it advances it to `AwaitingXorbs` — adjust/extend the Task 4 unit test if its expectations differ.

`AwaitingXorbs → AwaitingShard` already happens inside `UploadCommitConsole::xorb_uploaded` (Task 4); the xorb hook lands in Task 12, so for this task the test's accepted-states set covers the gap.

- [ ] **Step 6: Run tests**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_upload`
Expected: both tests pass (the `shard_uploaded` assertion will still fail — that flip lands in Task 12's `all_shards_uploaded`; if so, mark that single assertion `// enabled in Task 12` and re-enable it there. If you prefer strict TDD, move that assertion into Task 12's test instead).

- [ ] **Step 7: Commit**

```bash
git add xet_data/src/processing/file_upload_session.rs xet_data/src/processing/file_cleaner.rs xet_data/src/deduplication/file_deduplication.rs xet_data/src/progress_tracking/progress_types.rs xet_data/tests/test_console_upload.rs
git commit -m "feat(console): per-file upload pipeline states, live dedup, hash on processed"
```

---

### Task 12: Xorb and shard instrumentation

**Files:**
- Modify: `xet_data/src/processing/file_upload_session.rs` (`register_new_xorb`, ~line 306-399)
- Modify: `xet_data/src/processing/shard_interface.rs`
- Modify: `xet_data/tests/test_console_upload.rs`

- [ ] **Step 1: Write the failing test**

Add to `test_console_upload.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn xorbs_and_shards_tracked_through_upload() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);
    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();

    // Random bytes are novel to the fresh local CAS, so xorbs must be formed.
    let _xfi = upload_random_file(&upload_session, &env.base_dir, 8 << 20).await;
    upload_session.finalize().await.unwrap();

    let ended = scope.ended_upload_commits().pop().unwrap();
    assert!(ended.xorbs.counts.formed >= 1, "novel data must form at least one xorb");
    assert_eq!(ended.xorbs.counts.formed, ended.xorbs.counts.uploaded + ended.xorbs.counts.failed);
    assert!(ended.xorbs.in_flight.is_empty());
    assert!(!ended.xorbs.recent.is_empty());
    assert!(ended.shards.iter().all(|s| s.state == ShardState::Uploaded));
    let (_, f) = ended.completed_files.last().unwrap();
    assert!(f.shard_uploaded);
    assert!(!f.xorb_deps.is_empty());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_upload xorbs_and_shards`
Expected: FAIL — counts are zero.

- [ ] **Step 3: Hook register_new_xorb**

In `file_upload_session.rs::register_new_xorb` (verbatim context at lines ~365-399): the fn already has `xorb_hash`, `raw_num_bytes`, the permit acquisition, the `ProgressCallback`, and the spawned task.

At fn entry (xorb is fully formed when this is called):

```rust
        #[cfg(feature = "console")]
        if let Some(c) = &self.console {
            c.xorb_formed(xorb_hash.hex(), raw_num_bytes, 0); // serialized size arrives via callback total
            c.xorb_state(&xorb_hash.hex(), xet_runtime::console::model::XorbState::Queued);
        }
```

Tee the existing progress callback (it currently only feeds `completion_tracker`); extend the closure body:

```rust
        #[cfg(feature = "console")]
        let console_xorb = self.console.clone();
        let progress_callback: ProgressCallback = Arc::new(move |delta, completed, total| {
            let raw_delta = (delta * raw_num_bytes).checked_div(total).unwrap_or(0);
            if raw_delta > 0 {
                completion_tracker
                    .clone()
                    .register_xorb_upload_progress_background(xorb_hash, raw_delta);
            }
            #[cfg(feature = "console")]
            if let Some(c) = &console_xorb {
                c.xorb_transfer_with_total(&xorb_hash.hex(), completed, total);
            }
        });
```

(`xorb_transfer_with_total(hash, completed, total)` — extend `XorbConsole` so the first callback also records `serialized_bytes = total`; replaces the Task 4 `xorb_transfer` or delegates to it.) Inside the spawned task, first line:

```rust
                #[cfg(feature = "console")]
                if let Some(c) = &session.console {
                    c.xorb_state(&xorb_hash.hex(), xet_runtime::console::model::XorbState::Uploading);
                }
```

and replace the bare `?` on `upload_xorb` with explicit failure capture:

```rust
                let upload_result = session
                    .client
                    .upload_xorb(&cas_prefix, xorb_obj, Some(progress_callback), upload_permit)
                    .await;
                #[cfg(feature = "console")]
                if let Some(c) = &session.console {
                    c.xorb_uploaded(&xorb_hash.hex(), upload_result.is_ok());
                }
                let n_bytes_transmitted = upload_result?;
```

(Mind hash-string allocations: compute `let xorb_hex = xorb_hash.hex();` once per closure/task where used.)

- [ ] **Step 4: Hook the shard interface**

`SessionShardInterface` needs access to the commit console. It is constructed by/owned by `FileUploadSession` — add a cfg-gated field mirrored from the session at construction (find `SessionShardInterface::new` via `grep -n "fn new" xet_data/src/processing/shard_interface.rs`; thread `Option<Arc<UploadCommitConsole>>` as a parameter set right after the session console is created in `FileUploadSession::new` — if construction order makes that awkward, give `SessionShardInterface` a `OnceLock<Arc<UploadCommitConsole>>` set via `pub(crate) fn set_console(…)` immediately after both exist):

```rust
    #[cfg(feature = "console")]
    console: std::sync::OnceLock<std::sync::Arc<xet_runtime::console::state::UploadCommitConsole>>,
```

Hooks:

- `add_uploaded_xorb_block` (~line 196-222), inside the lock after `xorb_shard.add_xorb_block(...)`:

```rust
        #[cfg(feature = "console")]
        if let Some(c) = self.console.get() {
            c.shard_staging(xorb_shard.num_xorb_entries(), 0);
        }
```

- `upload_and_register_session_shards` (~line 239): after `consolidate_shards_in_directory` produces `shard_list`, before the spawn loop:

```rust
        #[cfg(feature = "console")]
        if let Some(c) = self.console.get() {
            for si in &shard_list {
                c.shard_discovered(si.shard_hash.hex(), 0);
            }
        }
```

inside each spawned task after the successful `upload_shard`:

```rust
                #[cfg(feature = "console")]
                if let Some(c) = &console_for_task {
                    c.shard_uploaded(&shard_hex);
                }
```

(clone `Option<Arc<…>>` + precompute `shard_hex = si.shard_hash.hex()` before the `move` closure), and after the join loop completes successfully, before the staged-shard cleanup:

```rust
        #[cfg(feature = "console")]
        if let Some(c) = self.console.get() {
            c.all_shards_uploaded();
        }
```

`all_shards_uploaded` (Task 4) flips remaining shard states, sets `shard_uploaded = true` on all files, advances in-flight files to `Complete`, and retires them. Re-enable the deferred `shard_uploaded` assertion from Task 11 if it was parked.

- [ ] **Step 5: Run tests**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_upload`
Expected: all three tests pass.

- [ ] **Step 6: Commit**

```bash
git add xet_data/src/processing/file_upload_session.rs xet_data/src/processing/shard_interface.rs xet_data/tests/test_console_upload.rs
git commit -m "feat(console): xorb and shard upload state tracking"
```

---

### Task 13: Download group and file instrumentation

**Files:**
- Modify: `xet_data/src/processing/file_download_session.rs`
- Modify: `xet_pkg/src/xet_session/file_download_group.rs` (kind tag)
- Modify: `xet_pkg/src/xet_session/download_stream_group.rs` (kind tag)
- Create: `xet_data/tests/test_console_download.rs`

- [ ] **Step 1: Write the failing test**

Create `xet_data/tests/test_console_download.rs` (TestEnvironment imports as in Task 10; upload a file first so there is something to download, mirroring `xet_data/tests/test_full_file_download.rs:27-59`):

```rust
#![cfg(feature = "console")]

mod console_common;
use console_common::{install_scope, upload_random_file};
use serial_test::serial;
// + the same TestEnvironment / FileUploadSession / FileDownloadSession imports
//   as test_console_upload.rs, plus FileDownloadState from xet_runtime::console::model

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn download_group_and_files_visible_in_console() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    // Arrange: upload one file (its commit console is also created; ignore it).
    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
    let xfi = upload_random_file(&upload_session, &env.base_dir, 4 << 20).await;
    upload_session.finalize().await.unwrap();

    let download_session = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    let groups = scope.live_download_groups();
    assert_eq!(groups.len(), 1);

    let out = env.base_dir.join("out.bin");
    let (_id, _n) = download_session.download_file(&xfi, &out).await.unwrap();

    let detail = groups[0].snapshot(true);
    let all: Vec<_> = detail.files.iter().cloned()
        .chain(detail.completed_files.iter().map(|(_, f)| f.clone()))
        .collect();
    assert_eq!(all.len(), 1);
    let f = &all[0];
    assert_eq!(f.state, FileDownloadState::Complete);
    assert_eq!(f.file_hash.as_deref(), Some(xfi.hash.as_str()));
    assert!(f.bytes_completed > 0, "items provider must surface bytes");

    drop(download_session);
    let ended = scope.ended_download_groups();
    assert_eq!(ended.len(), 1);
}
```

(`download_file` vs `download_file_background`: use whichever the existing test at `test_full_file_download.rs:27-59` uses — it calls `download_session.download_file(xfi, &out_path)`.) Note the group ends as `Aborted` via Drop unless finalized; if `FileDownloadSession` has an explicit finalize/finish path, hook it in step 3 and assert `Finished`, otherwise assert the ended entry exists without pinning the state and leave prompt-finish to the xet_pkg hooks below.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_download`
Expected: FAIL — no groups tracked.

- [ ] **Step 3: Instrument FileDownloadSession**

Struct field + creation in `FileDownloadSession::new` (`file_download_session.rs:28-61`), same pattern as Task 10 — progress provider from `progress.clone()`, plus the items provider:

```rust
        #[cfg(feature = "console")]
        let console = ctx.common.console_scope().map(|scope| {
            let c = xet_runtime::console::state::DownloadGroupConsole::new(
                Some(scope),
                xet_runtime::console::model::DownloadGroupKind::Files,
                Some(config.session.endpoint.clone()),
            );
            let p = progress.clone();
            c.set_progress_provider(Box::new(move || { /* map p.report() exactly as Task 10 */ }));
            let p2 = progress.clone();
            c.set_items_provider(Box::new(move || {
                p2.item_reports().into_iter().map(|(id, r)| (id.0, (r.total_bytes, r.bytes_completed))).collect()
            }));
            c
        });
```

Store as `pub(crate) console: Option<Arc<DownloadGroupConsole>>` plus a pub accessor for xet_pkg:

```rust
    #[cfg(feature = "console")]
    pub fn console(&self) -> Option<&std::sync::Arc<xet_runtime::console::state::DownloadGroupConsole>> {
        self.console.as_ref()
    }
```

Per-file hooks in `download_file_background` (`:115-130`) — file console at spawn, `Reconstructing` after the permit:

```rust
        #[cfg(feature = "console")]
        let file_console = self.console.as_ref().map(|c| {
            c.new_file(id.0, &write_path.to_string_lossy(), Some(file_info.hash.clone()), None)
        });
        let handle = runtime.spawn(async move {
            let _permit = semaphore.acquire().await?;
            #[cfg(feature = "console")]
            if let Some(fc) = &file_console {
                fc.set_state(xet_runtime::console::model::FileDownloadState::Reconstructing);
            }
            let result = session.download_file_with_id(&file_info, &write_path, id).await;
            #[cfg(feature = "console")]
            if let Some(c) = &session.console {
                use xet_runtime::console::model::FileDownloadState as S;
                if let Some(fc) = &file_console {
                    fc.set_state(if result.is_ok() { S::Complete } else { S::Failed });
                }
                c.retire_file(id.0);
            }
            result
        });
```

Apply the same wrap to the synchronous `download_file` path if it doesn't route through `download_file_background` (check; the existing test calls `download_file` directly). For streams (`download_stream` entry points in this file), create the file console with the requested `FileRange` as `requested_range`.

In xet_pkg: `XetFileDownloadGroup::new` (`file_download_group.rs:146`) and `XetDownloadStreamGroup::new` (`download_stream_group.rs:122`) call `download_session.console()` and set the kind (`Files` is the default; streams set `Stream`):

```rust
        #[cfg(feature = "console")]
        if let Some(c) = download_session.console() {
            c.set_kind(xet_runtime::console::model::DownloadGroupKind::Stream);
        }
```

`XetFileDownloadGroup::finish` (`file_download_group.rs:253`): after `handle_finish` succeeds, call a new `FileDownloadSession` method `console_finish()` that runs `c.finalize(DownloadGroupState::Finished)`. `abort` (`:178`) similarly calls `console_abort()` → `finalize(Aborted)`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_download`
Expected: PASS. Regression: `cargo test -p xet-data --features simulation --test test_full_file_download` — pass.

- [ ] **Step 5: Commit**

```bash
git add xet_data/src/processing/file_download_session.rs xet_pkg/src/xet_session/file_download_group.rs xet_pkg/src/xet_session/download_stream_group.rs xet_data/tests/test_console_download.rs
git commit -m "feat(console): download group and per-file download state tracking"
```

---

### Task 14: Term fetch-block instrumentation

**Files:**
- Modify: `xet_data/src/file_reconstruction/reconstruction_terms/manager.rs`
- Modify: the reconstructor construction path that already threads `progress_updater` (start at `file_download_session.rs::setup_reconstructor`, follow `progress_updater: Option<Arc<ItemProgressUpdater>>` down to `ReconstructionTermManager`'s constructor — typically 2 hops; add a parallel cfg-gated `console: Option<Arc<DownloadFileConsole>>` parameter/field at each)
- Modify: `xet_data/tests/test_console_download.rs`

- [ ] **Step 1: Write the failing test**

Add to `test_console_download.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn term_blocks_and_prefetch_state_recorded() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
    let xfi = upload_random_file(&upload_session, &env.base_dir, 16 << 20).await;
    upload_session.finalize().await.unwrap();

    let download_session = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    let group = scope.live_download_groups().pop().unwrap();
    let out = env.base_dir.join("out.bin");
    download_session.download_file(&xfi, &out).await.unwrap();

    let detail = group.snapshot(true);
    let (_, f) = detail.completed_files.last().expect("file completed");
    assert!(f.consumed_blocks >= 1, "at least one fetch block must have been consumed");
    let pf = f.prefetch.as_ref().expect("prefetch state recorded");
    assert!(pf.prefetched_byte_position > 0);
    assert!(pf.active_byte_position > 0);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_download term_blocks`
Expected: FAIL — `consumed_blocks == 0` / no prefetch.

- [ ] **Step 3: Thread the file console into the manager and hook it**

Give `ReconstructionTermManager` (struct at `manager.rs:26`) the field and a block-id counter:

```rust
    #[cfg(feature = "console")]
    console: Option<std::sync::Arc<xet_runtime::console::state::DownloadFileConsole>>,
    #[cfg(feature = "console")]
    next_block_id: u64,
```

Plumbing: `download_file_with_id` (`file_download_session.rs:141`) already creates the per-file console in Task 13's spawn wrapper — move file-console creation INTO `download_file_with_id` instead if that's where `setup_reconstructor` is called (it is), so the same `Arc` can be handed to the reconstructor; the spawn wrapper then fetches it from the group console by id (`DownloadGroupConsole` needs `file(id) -> Option<Arc<DownloadFileConsole>>` — add it). Pass `console.clone()` through `setup_reconstructor` alongside `progress_updater`.

Hooks in `manager.rs`:

- In `prefetch_block` (~line 234), right after `self.prefetched_byte_position = prefetch_block_range.end;` (the block is now committed to the queue):

```rust
        #[cfg(feature = "console")]
        let block_console = self.console.as_ref().map(|fc| {
            self.next_block_id += 1;
            fc.new_term_block(self.next_block_id, (prefetch_block_range.start, prefetch_block_range.end))
        });
```

- Inside the spawned task (`let jh = tokio::task::spawn(async move { … })`), first line `block_console`-gated `set_state(TermState::Fetching)`; after the `retrieve_file_term_block` result, on `Ok(Some(...))` map the returned terms:

```rust
            #[cfg(feature = "console")]
            if let Some(bc) = &block_console {
                bc.set_state(xet_runtime::console::model::TermState::Fetching); // moved to top of task
            }
            let result = retrieve_file_term_block(&runtime, client, file_hash, prefetch_block_range).await;
            #[cfg(feature = "console")]
            if let Some(bc) = &block_console {
                if let Ok(Some((_, _, ref file_terms))) = result {
                    bc.resolved(
                        file_terms
                            .iter()
                            .map(|t| xet_runtime::console::model::TermInfo {
                                xorb_hash: t.xorb_block.hash().hex(),
                                chunk_range: (t.xorb_chunk_range.start, t.xorb_chunk_range.end),
                                byte_range: (t.byte_range.start, t.byte_range.end),
                            })
                            .collect(),
                    );
                }
            }
```

Adjust the destructuring order to the actual `RawFetchedFileTerms` tuple (`type RawFetchedFileTerms = Result<Option<(Vec<FileTerm>, u64, u64)>>` at `manager.rs:21` — the in-task code at ~line 285 destructures `(ref returned_range, transfer_bytes, ref file_terms)`; trust the type alias and fix the existing-quote discrepancy by reading the real code). For the xorb hash accessor: `grep -n "impl XorbBlock" -A 20 xet_data/src/file_reconstruction` (or wherever `XorbBlock` lives) and use its hash field/method; `ChunkRange`'s `start`/`end` likewise (cas-types). `block_console` must move into the closure — capture it before `tokio::task::spawn`.

- Block-id ↔ JoinHandle pairing: change `prefetch_queue` to `VecDeque<(u64, JoinHandle<RawFetchedFileTerms>)>` under cfg? NO — keep the queue type stable; instead push the block id alongside via a parallel cfg-gated `VecDeque<u64>` (`block_id_queue`) pushed/popped in lockstep in `prefetch_block`/`next_file_terms`. Cleaner alternative if you prefer one structure: a small cfg-gated wrapper struct. Pick the parallel queue; it touches the least code.

- In `next_file_terms` (~line 93): where the completion-rate estimator updates, push prefetch state:

```rust
        #[cfg(feature = "console")]
        if let Some(fc) = &self.console {
            fc.set_prefetch(
                self.prefetch_queue.len(),
                self.prefetched_byte_position,
                self.current_active_byte_position,
                self.completion_rate_estimator.value(), // check accessor: grep "impl ExpWeightedMovingAvg" for the getter name
            );
        }
```

and where a block's JoinHandle is popped and awaited successfully, pop the parallel id and `fc.consume_term_block(block_id)`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-data --features "console simulation" --test test_console_download`
Expected: both download tests pass. Regression: `cargo test -p xet-data --features simulation` — pass.

- [ ] **Step 5: Commit**

```bash
git add xet_data/src/file_reconstruction/reconstruction_terms/manager.rs xet_data/src/processing/file_download_session.rs xet_data/tests/test_console_download.rs
git commit -m "feat(console): term fetch-block states and prefetch telemetry"
```

---

### Task 15: End-to-end xet_pkg test over HTTP

The full spec loop: real `XetSession`, real commit + download group, every endpoint polled over HTTP, retention asserted.

**Files:**
- Modify: `xet_pkg/tests/test_console.rs`

- [ ] **Step 1: Write the test** (this is the acceptance test — write it, run it, fix whatever it exposes)

```rust
async fn get(url: String) -> serde_json::Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn full_transfer_visible_over_http() {
    // Env must be set BEFORE the first session builds (that's what starts the server).
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let temp = tempfile::tempdir().unwrap();
    let session = XetSessionBuilder::new().build().unwrap();
    let endpoint = format!("local://{}", temp.path().join("cas").display());
    let addr = xet_runtime::console::server::bound_addr().unwrap();
    let base = format!("http://{addr}/api/v1");

    // Upload two files (call sequence from xet_pkg/tests/test_xet_session.rs:336).
    let metas = {
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let mut metas = Vec::new();
        for (name, data) in [("big.bin", vec![0xABu8; 4 << 20]), ("small.bin", b"small content".to_vec())] {
            let h = commit.upload_bytes(data, Sha256Policy::Compute, Some(name.into())).await.unwrap();
            metas.push(h.finalize_ingestion().await.unwrap());
        }
        commit.commit().await.unwrap();
        metas
    };

    let sid = {
        let sessions = get(format!("{base}/sessions")).await;
        sessions["sessions"][0]["id"].as_str().unwrap().to_string()
    };

    let uploads = get(format!("{base}/sessions/{sid}/uploads")).await;
    assert!(uploads["as_of"].as_u64().unwrap() > 0);

    // Commit finished -> it lives in the session detail's ended_upload_commits.
    let detail = get(format!("{base}/sessions/{sid}")).await;
    let ended = detail["ended_upload_commits"].as_array().unwrap();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0]["state"], "completed");
    assert!(ended[0]["dedup"]["total_bytes"].as_u64().unwrap() > 0);
    let files = ended[0]["completed_files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    for entry in files {
        let f = &entry[1]; // completed_files entries are (epoch_ms, snapshot) pairs
        assert_eq!(f["state"], "complete");
        assert!(f["file_hash"].is_string());
        assert_eq!(f["shard_uploaded"], true);
    }

    // Download one file; then check downloads + concurrency + snapshot.
    let dest = temp.path().join("dest.bin");
    let group = session
        .new_file_download_group()
        .unwrap()
        .with_endpoint(&endpoint)
        .build()
        .await
        .unwrap();
    group
        .download_file_to_path(metas[0].xet_info.clone(), dest.clone())
        .await
        .unwrap();
    group.finish().await.unwrap();

    let detail = get(format!("{base}/sessions/{sid}")).await;
    assert_eq!(detail["ended_download_groups"].as_array().unwrap().len(), 1);
    assert_eq!(detail["ended_download_groups"][0]["state"], "finished");

    let conc = get(format!("{base}/sessions/{sid}/concurrency")).await;
    let monitors = conc["monitors"].as_array().unwrap();
    assert!(!monitors.is_empty(), "local client registers at least the upload monitor");
    assert!(monitors.iter().any(|m| m["total_permits"].as_u64().unwrap() > 0));

    let snapshot = get(format!("{base}/snapshot")).await;
    assert_eq!(snapshot["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["process"]["pid"], std::process::id());
}
```

Imports mirror `xet_pkg/tests/test_xet_session.rs` (`XetSessionBuilder`, `Sha256Policy`, `tempfile::tempdir`); `metas[i]` exposes `xet_info` the same way that file's tests do (`file_meta.xet_info.clone()`). If the existing console test in this file (Task 8) already started the server on a different ephemeral port within this process, `bound_addr()` returns that same address — the env var and `#[serial]` keep this coherent.

- [ ] **Step 2: Run until green**

Run: `cargo test -p xet-pkg --features "console simulation" --test test_console`
Expected: PASS (both tests in the file). This is the task where cross-task integration bugs surface — fix them here.

- [ ] **Step 3: Full-matrix regression**

Run: `cargo test -p xet-pkg && cargo test -p xet-data --features simulation && cargo build -p xet-pkg --features console && cargo clippy -p xet-runtime -p xet-client -p xet-data -p xet-pkg --features console -- -D warnings`
Expected: everything green.

- [ ] **Step 4: Commit**

```bash
git add xet_pkg/tests/test_console.rs
git commit -m "test(console): end-to-end HTTP acceptance test over a real XetSession"
```

---

### Task 16: CI job for the console feature

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add console steps to the existing jobs**

After the existing "Build and Test" step (~line 92, which runs `cargo test --verbose --no-fail-fast --features "strict simulation git-xet-for-integration-test"`), add:

```yaml
      - name: Build and Test (console feature)
        run: |
          cargo test --verbose --no-fail-fast -p xet-runtime -p xet-client -p xet-data -p xet-pkg --features "console simulation"
```

After the existing Lint step (~line 83), add:

```yaml
      - name: Lint (console feature)
        run: |
          cargo clippy -r --verbose -p xet-runtime -p xet-client -p xet-data -p xet-pkg --features console -- -D warnings
```

Notes: `strict` is intentionally absent (xet_pkg has no `strict` feature; plain feature names passed with multiple `-p` must exist in every selected package — `console` and `simulation` do). Match the YAML indentation of the surrounding steps exactly.

- [ ] **Step 2: Validate locally**

Run both commands from the YAML locally.
Expected: green. (This is the same matrix Task 15 step 3 ran — this step catches YAML/flag typos.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: build, lint, and test the console feature"
```

---

### Task 17: Agent skill + README pointer

**Files:**
- Create: `docs/skills/xet-console/SKILL.md`
- Create: symlink `.claude/skills/xet-console` → `../../docs/skills/xet-console`
- Modify: `README.md` (development/debugging section)

- [ ] **Step 1: Write the skill**

Create `docs/skills/xet-console/SKILL.md`:

````markdown
---
name: xet-console
description: Inspect the live state of a running hf-xet/xet-core process (uploads, downloads, dedup, concurrency) via the xet-console HTTP API on localhost:6660. Use when debugging an in-progress or just-finished transfer in a console-enabled build.
---

# xet-console: live session inspection

Requires the process under inspection to be built with the `console` feature
(`maturin develop --features console`, or `--features console` on any crate
test/binary). The process serves read-only JSON on `http://127.0.0.1:6660`
(override: `XET_CONSOLE_PORT`; `0` = ephemeral, port then appears in the
process logs as `xet-console listening on ...`).

## Connect

```bash
curl -s localhost:6660/ | jq '{service, version, pid}'
```

If `pid` is not the process you expect (or the connection is refused), ask the
user which port their process used (`XET_CONSOLE_PORT`), then substitute it.

## First call: the full snapshot

```bash
curl -s localhost:6660/api/v1/snapshot | jq .
```

Everything in one document: process info and, per session, monitors, upload
commits, and download groups (each with in-flight items, recent completions,
and cumulative counters). Prefer scoped endpoints below when the snapshot is
large.

## Endpoints

```
/api/v1/process                          pid, argv, version, n_active_sessions
/api/v1/sessions                         active + recently ended sessions
/api/v1/sessions/{sid}                   session detail incl. ended commits/groups
/api/v1/sessions/{sid}/uploads           commit summaries
/api/v1/sessions/{sid}/uploads/{cid}     files, xorbs, shards, dedup, progress
/api/v1/sessions/{sid}/downloads         group summaries
/api/v1/sessions/{sid}/downloads/{gid}   files, term blocks, prefetch, progress
/api/v1/sessions/{sid}/concurrency       adaptive concurrency monitors + history
/api/v1/snapshot                         everything (?session={sid} to filter)
```

All timestamps are epoch milliseconds; every response carries `as_of`.
Detail endpoints return in-flight items + a bounded recent-completions ring +
cumulative counters (`?files=all` accepted for the full retained file list).

## Recipes

What's uploading right now:
```bash
curl -s localhost:6660/api/v1/snapshot | jq '.sessions[].upload_commit_details[] | {id, state, files: [.files[] | {name, state, bytes_chunked}], xorbs_in_flight: .xorbs.in_flight | length}'
```

Is dedup pulling its weight (ratio of deduped to total):
```bash
curl -s "localhost:6660/api/v1/sessions/$SID/uploads/$CID" | jq '.dedup | {ratio: (if .total_bytes > 0 then .deduped_bytes / .total_bytes else null end), via_global: .deduped_bytes_by_global_dedup, new: .new_bytes}'
```

Which files are stuck and where:
```bash
curl -s "localhost:6660/api/v1/sessions/$SID/uploads/$CID" | jq '[.files[] | {name, state, shard_uploaded}] | group_by(.state) | map({state: .[0].state, n: length, names: [.[].name][:5]})'
```

What are the concurrency monitors doing:
```bash
curl -s "localhost:6660/api/v1/sessions/$SID/concurrency" | jq '.monitors[] | {tag, permits: "\(.active_permits)/\(.total_permits)", success: .success.success_ratio, rtt_ms: .latency.predicted_max_rtt_ms, recent_limits: [.limit_history[-5:][] | .[1]]}'
```

## Symptom → diagnosis

- **Monitor `success.success_ratio` < 0.5, limit history trending down** — the
  server is pushing back or the network is struggling; expect reduced
  concurrency. Look at `latency.predicted_max_rtt_ms` for confirmation.
- **Download term blocks piling up `enqueued` + `available_permits == 0`** on
  the download monitor — the transfer is concurrency-bound; the adaptive
  controller is the bottleneck, not the disk.
- **Upload files parked in `awaiting_shard`** — xorbs are uploaded but the
  commit's finalize (shard upload) hasn't run; the caller hasn't called
  `commit()` yet or finalize is stuck.
- **Low `deduped_bytes`, high `new_bytes`** — content is genuinely novel (or
  chunking is mismatched against what the server has); not a transfer problem.
- **Files stuck in `queued`** — the per-process file semaphores
  (`max_concurrent_file_ingestion` / `max_concurrent_file_downloads`, shown in
  the session's `config`) are saturated.
- **Session absent from `/sessions`** — the process isn't console-enabled, a
  different process owns 6660 (check `/` for pid/argv), or the session already
  ended (check `ended_sessions`).
````

- [ ] **Step 2: Symlink for Claude Code + README pointer**

```bash
mkdir -p .claude/skills
ln -s ../../docs/skills/xet-console .claude/skills/xet-console
```

In `README.md`, add to the development section (find the appropriate existing section header):

```markdown
### Debugging live transfers: xet-console

Build with `--features console` and every `XetSession` in the process serves a
read-only inspection API on `http://127.0.0.1:6660` (uploads, downloads, dedup,
adaptive concurrency). See `docs/design/2026-06-11-xet-console-design.md` and
the agent/CLI guide at `docs/skills/xet-console/SKILL.md`.
```

- [ ] **Step 3: Verify the symlink resolves**

Run: `ls -lH .claude/skills/xet-console/SKILL.md && head -5 .claude/skills/xet-console/SKILL.md`
Expected: frontmatter prints. Confirm git tracks the symlink: `git status --short` shows `.claude/skills/xet-console` as a new path (if `.claude/` is globally ignored in this repo, note that the canonical file in `docs/skills/` is the tracked one and the symlink is best-effort local).

- [ ] **Step 4: Commit**

```bash
git add docs/skills/xet-console/SKILL.md .claude/skills/xet-console README.md
git commit -m "docs(console): agent skill for the xet-console API + README pointer"
```

---

### Task 18: Final verification sweep

**Files:** none new.

- [ ] **Step 1: Format and lint everything**

```bash
cargo +nightly fmt
cargo clippy -p xet-runtime -p xet-client -p xet-data -p xet-pkg --features console -- -D warnings
cargo clippy -- -D warnings
```

Expected: fmt produces no diff beyond your own files (commit any formatting it applies); both clippy runs clean.

- [ ] **Step 2: Full test matrix**

```bash
cargo test -p xet-runtime --features console
cargo test -p xet-client --features console
cargo test -p xet-data --features "console simulation"
cargo test -p xet-pkg --features "console simulation"
cargo test --features "strict simulation git-xet-for-integration-test"   # feature-off world untouched
```

Expected: all green.

- [ ] **Step 3: Manual smoke (optional but recommended)**

```bash
XET_CONSOLE_PORT=6660 cargo test -p xet-pkg --features "console simulation" --test test_console -- --nocapture &
sleep 2 && curl -s localhost:6660/api/v1/snapshot | jq '.sessions | length'
```

Or the real thing: `maturin develop --features console` in the hf_xet wheel setup, run an `hf upload`, and `curl localhost:6660/api/v1/snapshot | jq .` mid-transfer.

- [ ] **Step 4: Commit any stragglers**

```bash
git add -u
git commit -m "chore(console): formatting and final verification fixes"
```

---

## Deferred to the TUI plan (do not build here)

The ratatui `xet_console` workspace bin crate (layout A pages, navigation, discovery picker) is a separate plan consuming this API. Nothing in this plan may depend on it.

## Known judgment calls an executor may hit

- **Exact field/method names** at hook sites were verified against the tree at plan time (cited `file:line` throughout) but small drifts happen — when a name differs, follow the cited anchor (the surrounding function), not the name.
- **Lock discipline**: console cells use `std::sync::Mutex` with tiny critical sections and poison-tolerant reads. Never hold a console lock across an `.await`.
- **Feature-off builds must stay byte-identical in behavior**: every console touch is `#[cfg(feature = "console")]`. If a change can't be cleanly gated, restructure the hook, not the host code.







