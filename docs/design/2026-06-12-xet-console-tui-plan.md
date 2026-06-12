# xet-console TUI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A ratatui terminal client (`xet-console` binary) that polls the xet-console HTTP API and renders live session state — overview, upload-commit detail (layout A), download-group detail, and concurrency monitors — with tokio-console-style navigation.

**Architecture:** New unpublished workspace member `xet_console/` (package `xet-console`). Two threads, no tokio in this crate: a poller thread fetches `/api/v1/snapshot` with `reqwest::blocking` into an `Arc<Shared>` state; the main thread runs a crossterm event loop and draws with ratatui. Wire types are imported from `xet_runtime::console::model` (the contract — never redefined). A pure, unit-testable `App` state machine owns pages/selection/focus; render modules are tested headlessly with ratatui's `TestBackend` against a shared fixture snapshot; the HTTP client is tested against a real in-process console server.

**Tech Stack:** Rust, ratatui + crossterm (latest via `cargo add`), reqwest blocking, clap (workspace), humantime (workspace), xet-runtime `console` feature.

**Spec:** `docs/design/2026-06-11-xet-console-design.md` — section "TUI — `xet-console`" + "Implementation notes (v1 deltas)". The core API is fully implemented on this branch.

---

## Facts you need (verified against the tree at plan time)

- **Wire types** live in `xet_runtime/src/console/model.rs` and are the single source of truth. The TUI imports them via `xet-runtime = { path = "../xet_runtime", features = ["console"] }` (pulling axum into this dev tool's build is an accepted design decision). Key shapes used everywhere below:
  - `SnapshotResponse { as_of: u64, process: ProcessInfo, sessions: Vec<SessionFull> }`
  - `SessionFull { detail: SessionDetail, upload_commit_details: Vec<UploadCommitDetail>, download_group_details: Vec<DownloadGroupDetail> }`
  - `SessionDetail { as_of, id: String, state: SessionState, created_at, config: Vec<(String,String)>, monitors: Vec<MonitorSnapshot>, upload_commits: Vec<UploadCommitSummary>, ended_upload_commits: Vec<UploadCommitDetail>, download_groups: Vec<DownloadGroupSummary>, ended_download_groups: Vec<DownloadGroupDetail> }`
  - `UploadCommitDetail { as_of, id: u64, state: UploadCommitState, created_at, endpoint, progress: Option<ProgressSnapshot>, dedup: DedupSnapshot, files: Vec<FileUploadSnapshot>, completed_files: Vec<(u64, FileUploadSnapshot)>, file_counts: FileCounts, xorbs: XorbsSnapshot, shards: Vec<ShardSnapshot> }`
  - `FileUploadSnapshot { id, name, size: Option<u64>, state: FileUploadState, bytes_chunked, n_chunks, file_hash: Option<String>, sha256: Option<String>, dedup: Option<DedupSnapshot>, xorb_deps: Vec<XorbDepSnapshot>, shard_uploaded: bool, created_at, finished_at: Option<u64> }`
  - `XorbsSnapshot { in_flight: Vec<XorbSnapshot>, counts: XorbCounts { formed, uploaded, failed }, recent: Vec<(u64, XorbSnapshot)> }`; `XorbSnapshot { hash, state: XorbState, raw_bytes, serialized_bytes, bytes_transferred, n_files, created_at, finished_at }`
  - `ShardSnapshot { hash: Option<String>, state: ShardState, n_xorbs, size }`
  - `DownloadGroupDetail { …, kind: DownloadGroupKind, state: DownloadGroupState, progress, files: Vec<FileDownloadSnapshot>, completed_files: Vec<(u64, FileDownloadSnapshot)>, file_counts }`
  - `FileDownloadSnapshot { id, name, file_hash: Option<String>, requested_range: Option<(u64,u64)>, total_bytes, bytes_completed, state: FileDownloadState, prefetch: Option<PrefetchSnapshot { queue_depth, prefetched_byte_position, active_byte_position, completion_rate_bps: Option<f64> }>, term_blocks: Vec<TermBlockSnapshot { block_id, byte_range, state: TermState, terms: Vec<TermInfo>, created_at, fetched_at }>, consumed_blocks, created_at, finished_at }`
  - `MonitorSnapshot { tag: String, total_permits, active_permits, available_permits, bounds: PermitBounds{min,max}, adjustment_enabled, bytes_sent, success: Option<SuccessModelSnapshot { success_ratio, thresholds: Thresholds{increase,decrease}, recommended_adjustment: AdjustmentRecommendation }>, latency: Option<LatencyModelSnapshot { predicted_max_rtt_ms: Option<f64>, rtt_standard_error_ms: Option<f64>, predicted_bandwidth_bps: Option<f64> }>, limit_history: Vec<(u64, usize)> }`
  - `ProgressSnapshot { total_bytes, bytes_completed, rate_bps: Option<f64>, transfer_bytes, transfer_bytes_completed, transfer_rate_bps: Option<f64> }`; `DedupSnapshot { total_bytes, deduped_bytes, new_bytes, deduped_bytes_by_global_dedup, defrag_prevented_dedup_bytes, xorb_bytes_uploaded, shard_bytes_uploaded }`
  - `IndexResponse { service, version, pid, argv, start_time_ms, endpoints }`
  - Enums serialize snake_case; all are plain Rust enums here (`SessionState::{Active,Ended}`, `UploadCommitState::{Active,Committing,Completed,Aborted}`, `FileUploadState::{Queued,Chunking,Processed,AwaitingXorbs,AwaitingShard,Complete,Failed,Aborted}`, `XorbState`, `ShardState`, `DownloadGroupKind::{Files,Stream}`, `DownloadGroupState::{Active,Finished,Aborted}`, `FileDownloadState`, `TermState::{Enqueued,Fetching,Fetched,Consumed}`, `AdjustmentRecommendation::{Increase,Hold,Decrease}`).
- **The server for tests**: `xet_runtime::console::server::{ensure_started, bound_addr}` (set `XET_CONSOLE_PORT=0` BEFORE first call; `#[serial]` any test that touches it — the server + registry are process-global). Fabricate live state via `xet_runtime::console::registry::registry().register_session(id, config) -> SessionHandle` (`handle.scope: Arc<SessionConsole>`) and the cells: `scope.new_monitor(tag, semaphore, bounds, enabled)`, `UploadCommitConsole::new(Some(&scope), endpoint)`, `commit.new_file(id, name, size)`, `DownloadGroupConsole::new(Some(&scope), kind, endpoint)` etc. — see `xet_runtime/src/console/state.rs` tests for exact construction patterns and `xet_runtime/tests/console_server.rs` for the server-test pattern.
- **Connection policy** (spec): default `http://127.0.0.1:6660`; positional URL-or-port argument or `--port` overrides; `--interval` poll rate, default 500ms. No discovery scan (strict-port design).
- **Workspace**: root `Cargo.toml` members list has a "Top-level crates (not published as packages)" group (`git_xet`, `simulation`) — `xet_console` joins it. Package names are hyphenated (`xet-console`); `clap`, `humantime`, `anyhow`, `serde_json`, `serial_test`, `reqwest` are workspace deps. The workspace reqwest is `default-features = false` — add the `blocking` feature in this crate (loopback http only, no TLS features needed).
- **ratatui/crossterm versions**: resolve with `cargo add ratatui crossterm` (network registry). The code in this plan targets the ratatui 0.29-era API (`Terminal`, `Frame`, `Layout/Constraint/Direction`, widgets `Block/Borders/Paragraph/Table/Row/Cell/Gauge/Sparkline/Clear`, `TestBackend`); if the resolved major differs, adapt mechanically at the compiler's direction — keep semantics.
- Run all commands from `/Users/assafvayner/hf/xet-core`. Branch: `assaf/xet-console`, commit directly. NEVER `git add -A` (`.superpowers/` and `.claude/skills/xet-console` must stay untracked).

## File structure (locked in)

```
xet_console/
  Cargo.toml
  src/
    main.rs        — args, terminal lifecycle (raw mode/alt screen/panic hook), event loop
    client.rs      — ConsoleClient (blocking reqwest): index(), snapshot()
    poll.rs        — Shared/PollState + spawn_poller thread
    app.rs         — App state machine: Page/Pane/selection/keys (pure, no I/O)
    fixtures.rs    — #[cfg(test)] sample SnapshotResponse used by all render tests
    ui/
      mod.rs       — draw() dispatch, header/footer bars, help overlay, banners
      widgets.rs   — humanize_bytes/rate, short_hash, state styling, buffer_to_string test helper
      overview.rs  — page 1
      upload.rs    — page 2 (layout A)
      download.rs  — page 3 (layout A mirror)
      concurrency.rs — page 4
```

All tests are unit tests inside these modules (`xet-console` is a bin crate; `cargo test -p xet-console` runs them).

---

### Task 1: Crate scaffold, args, terminal lifecycle

**Files:**
- Modify: `Cargo.toml` (workspace members)
- Create: `xet_console/Cargo.toml`
- Create: `xet_console/src/main.rs`
- Create: `xet_console/src/client.rs` (stub for compile)
- Create: `xet_console/src/poll.rs` (stub)
- Create: `xet_console/src/app.rs` (stub)
- Create: `xet_console/src/ui/mod.rs` (stub)
- Create: `xet_console/src/ui/widgets.rs` (stub)

- [ ] **Step 1: Workspace member + crate manifest**

Root `Cargo.toml`: add `"xet_console",` to the members list in the `# Top-level crates (not published as packages)` group (after `"git_xet",`).

Create `xet_console/Cargo.toml`:

```toml
[package]
name = "xet-console"
version.workspace = true
edition.workspace = true
license.workspace = true
publish = false
description = "Terminal UI for the xet-console live observability API"

[dependencies]
xet-runtime = { version = "1.5.2", path = "../xet_runtime", features = ["console"] }
anyhow = { workspace = true }
clap = { workspace = true }
humantime = { workspace = true }
reqwest = { workspace = true, features = ["blocking"] }
serde_json = { workspace = true }

[dev-dependencies]
serial_test = { workspace = true }
```

Then run: `cargo add -p xet-console ratatui crossterm`
Expected: latest versions added to `xet_console/Cargo.toml`. Note which versions resolved (you'll target their API).

- [ ] **Step 2: Write the failing args test**

`xet_console/src/main.rs` skeleton with the test first:

```rust
mod app;
mod client;
mod poll;
mod ui;
#[cfg(test)]
mod fixtures;

use std::time::Duration;

use clap::Parser;

const DEFAULT_BASE: &str = "http://127.0.0.1:6660";

/// Terminal UI for the xet-console live observability API.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Base URL (http://host:port), bare host:port, or bare port of the console server.
    target: Option<String>,
    /// Port on 127.0.0.1 (overridden by `target` if given).
    #[arg(long)]
    port: Option<u16>,
    /// Poll interval (e.g. 500ms, 2s).
    #[arg(long, default_value = "500ms", value_parser = humantime::parse_duration)]
    interval: Duration,
}

fn resolve_base(target: Option<&str>, port: Option<u16>) -> String {
    if let Some(t) = target {
        let t = t.trim().trim_end_matches('/');
        if t.starts_with("http://") || t.starts_with("https://") {
            return t.to_string();
        }
        if let Ok(p) = t.parse::<u16>() {
            return format!("http://127.0.0.1:{p}");
        }
        return format!("http://{t}");
    }
    if let Some(p) = port {
        return format!("http://127.0.0.1:{p}");
    }
    DEFAULT_BASE.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_resolution_rules() {
        assert_eq!(resolve_base(None, None), "http://127.0.0.1:6660");
        assert_eq!(resolve_base(None, Some(7000)), "http://127.0.0.1:7000");
        assert_eq!(resolve_base(Some("8123"), None), "http://127.0.0.1:8123");
        assert_eq!(resolve_base(Some("http://10.0.0.5:6660/"), Some(1)), "http://10.0.0.5:6660");
        assert_eq!(resolve_base(Some("myhost:6660"), None), "http://myhost:6660");
    }
}
```

Create one-line stubs so it compiles: `client.rs`, `poll.rs`, `app.rs`, `ui/mod.rs`, `ui/widgets.rs` each containing only `// filled in by later tasks` (and `ui/mod.rs` additionally `pub mod widgets;`). `fixtures.rs` is `#[cfg(test)]`-declared but the file must exist: create it with `// test fixtures arrive in Task 5`.

Run: `cargo test -p xet-console base_resolution` — expect FAIL to compile first if you wrote the test before `resolve_base`; then PASS once complete (the strict red step here is the compile error).

- [ ] **Step 3: Terminal lifecycle + minimal loop**

Add to `main.rs`:

```rust
use std::io;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode};
use crossterm::execute;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let base = resolve_base(args.target.as_deref(), args.port);

    // Restore the terminal even if we panic mid-draw.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_hook(info);
    }));

    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;

    let result = run(&mut terminal, &base, args.interval);

    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen)?;
    result
}

fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    base: &str,
    _interval: Duration,
) -> anyhow::Result<()> {
    loop {
        terminal.draw(|f| {
            use ratatui::widgets::Paragraph;
            f.render_widget(Paragraph::new(format!("xet-console — connecting to {base}  (q to quit)")), f.area());
        })?;
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                    return Ok(());
                }
            }
        }
    }
}
```

(`f.area()` is ratatui ≥0.27; older is `f.size()` — follow the compiler.)

- [ ] **Step 4: Verify**

Run: `cargo test -p xet-console` (1 passed), `cargo clippy -p xet-console -- -D warnings` (clean), `cargo build -p xet-console` (clean). Optionally sanity-run `cargo run -p xet-console -- --help`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock xet_console/
git commit -m "feat(xet-console): TUI crate scaffold, args, terminal lifecycle"
```

---

### Task 2: Typed client against a real server

**Files:**
- Modify: `xet_console/src/client.rs`

- [ ] **Step 1: Write the failing live-server test**

Replace `client.rs` with tests first (implementation in step 3 — write the file in one go if you prefer; the red step is the compile error):

```rust
#[cfg(test)]
mod tests {
    use serial_test::serial;
    use xet_runtime::console::registry::registry;
    use xet_runtime::console::server;

    use super::*;

    #[test]
    #[serial]
    fn client_fetches_index_and_snapshot_from_live_server() {
        unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
        server::ensure_started();
        let addr = server::bound_addr().expect("server bound");
        let _session = registry().register_session("tui-client-test".into(), vec![]);

        let client = ConsoleClient::new(format!("http://{addr}"));
        let index = client.index().expect("index");
        assert_eq!(index.service, "xet-console");
        assert_eq!(index.pid, std::process::id());

        let snap = client.snapshot().expect("snapshot");
        assert!(snap.as_of > 0);
        assert!(snap.sessions.iter().any(|s| s.detail.id == "tui-client-test"));

        // /sessions carries the ended ring, which /snapshot doesn't.
        let sessions = client.sessions().expect("sessions");
        assert!(sessions.as_of > 0);
        assert!(sessions.sessions.iter().any(|s| s.id == "tui-client-test"));
    }

    #[test]
    fn connection_refused_is_an_error_not_a_panic() {
        // Port 1 on loopback is essentially never listening.
        let client = ConsoleClient::new("http://127.0.0.1:1".into());
        assert!(client.snapshot().is_err());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-console client` — compile error (`ConsoleClient` not defined).

- [ ] **Step 3: Implement**

Above the tests in `client.rs`:

```rust
use std::time::Duration;

use anyhow::{Context, Result};
use xet_runtime::console::model::{IndexResponse, SessionsResponse, SnapshotResponse};

/// Blocking HTTP client for the xet-console API. The TUI polls /api/v1/snapshot
/// (everything the four pages render about LIVE sessions) plus /api/v1/sessions
/// (the only endpoint carrying the ended-sessions ring).
pub struct ConsoleClient {
    base: String,
    http: reqwest::blocking::Client,
}

impl ConsoleClient {
    pub fn new(base: String) -> Self {
        let http = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .expect("static client config");
        Self { base, http }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn index(&self) -> Result<IndexResponse> {
        self.get_json(&format!("{}/", self.base))
    }

    pub fn snapshot(&self) -> Result<SnapshotResponse> {
        self.get_json(&format!("{}/api/v1/snapshot", self.base))
    }

    pub fn sessions(&self) -> Result<SessionsResponse> {
        self.get_json(&format!("{}/api/v1/sessions", self.base))
    }

    fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let resp = self.http.get(url).send().with_context(|| format!("GET {url}"))?;
        let resp = resp.error_for_status().with_context(|| format!("GET {url}"))?;
        resp.json::<T>().with_context(|| format!("decoding {url}"))
    }
}
```

(`serde` comes in transitively; if the `serde::de::DeserializeOwned` path needs the crate explicitly, add `serde = { workspace = true }` to Cargo.toml — report if so. The `expect` in `new` is on a statically-valid builder config, acceptable in this client-side tool.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-console client` — 2 passed (the live test starts a real axum server in-process and round-trips the real wire format through the real serde derives: this is the cross-crate contract test).

- [ ] **Step 5: Commit**

```bash
git add xet_console/src/client.rs xet_console/Cargo.toml Cargo.lock
git commit -m "feat(xet-console): typed blocking client with live-server contract test"
```

---

### Task 3: Poller thread + shared state

**Files:**
- Modify: `xet_console/src/poll.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use serial_test::serial;
    use xet_runtime::console::registry::registry;
    use xet_runtime::console::server;

    use super::*;
    use crate::client::ConsoleClient;

    #[test]
    #[serial]
    fn poller_populates_and_refreshes_shared_state() {
        unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
        server::ensure_started();
        let addr = server::bound_addr().expect("server bound");
        let _session = registry().register_session("tui-poll-test".into(), vec![]);

        let shared = Arc::new(Shared::default());
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = spawn_poller(
            ConsoleClient::new(format!("http://{addr}")),
            shared.clone(),
            Duration::from_millis(50),
            shutdown.clone(),
        );

        let mut first_as_of = 0;
        for _ in 0..40 {
            if let Some(snap) = &shared.lock().snapshot {
                first_as_of = snap.as_of;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(first_as_of > 0, "poller never populated a snapshot");

        // A later poll refreshes as_of.
        let mut refreshed = false;
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(50));
            if shared.lock().snapshot.as_ref().is_some_and(|s| s.as_of > first_as_of) {
                refreshed = true;
                break;
            }
        }
        assert!(refreshed, "as_of never advanced");
        assert!(shared.lock().last_error.is_none());

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn poller_records_errors_and_keeps_running() {
        let shared = Arc::new(Shared::default());
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = spawn_poller(
            ConsoleClient::new("http://127.0.0.1:1".into()),
            shared.clone(),
            Duration::from_millis(20),
            shutdown.clone(),
        );
        let mut got_error = false;
        for _ in 0..100 {
            std::thread::sleep(Duration::from_millis(20));
            if shared.lock().last_error.is_some() {
                got_error = true;
                break;
            }
        }
        assert!(got_error, "connection failure must surface as last_error");
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-console poll` — compile error.

- [ ] **Step 3: Implement**

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use xet_runtime::console::model::{SessionSummary, SnapshotResponse};

use crate::client::ConsoleClient;

#[derive(Default)]
pub struct PollState {
    pub snapshot: Option<SnapshotResponse>,
    /// Ended sessions (from /sessions — /snapshot carries live sessions only).
    pub ended_sessions: Vec<SessionSummary>,
    /// Epoch ms of the last successful poll (drives the staleness indicator).
    pub last_success_ms: Option<u64>,
    /// Last fetch error; cleared on the next success.
    pub last_error: Option<String>,
    /// Set by the UI ('p'); the poller skips fetches while true.
    pub paused: bool,
}

#[derive(Default)]
pub struct Shared(Mutex<PollState>);

impl Shared {
    /// Poison-tolerant lock: a panicked holder must not take the UI down.
    pub fn lock(&self) -> MutexGuard<'_, PollState> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub fn spawn_poller(
    client: ConsoleClient,
    shared: Arc<Shared>,
    interval: Duration,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("xet-console-poller".into())
        .spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let paused = shared.lock().paused;
                if !paused {
                    match client.snapshot() {
                        Ok(snap) => {
                            // Best-effort companion fetch; the ended ring is cosmetic.
                            let ended = client.sessions().map(|s| s.ended_sessions).unwrap_or_default();
                            let mut st = shared.lock();
                            st.snapshot = Some(snap);
                            st.ended_sessions = ended;
                            st.last_success_ms = Some(now_ms());
                            st.last_error = None;
                        },
                        Err(e) => {
                            shared.lock().last_error = Some(format!("{e:#}"));
                        },
                    }
                }
                // Sleep in slices so shutdown stays responsive.
                let mut remaining = interval;
                while !shutdown.load(Ordering::Relaxed) && remaining > Duration::ZERO {
                    let slice = remaining.min(Duration::from_millis(50));
                    std::thread::sleep(slice);
                    remaining = remaining.saturating_sub(slice);
                }
            }
        })
        .expect("spawn poller thread")
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-console poll` — 2 passed. `cargo clippy -p xet-console -- -D warnings` — clean.

- [ ] **Step 5: Commit**

```bash
git add xet_console/src/poll.rs
git commit -m "feat(xet-console): poller thread with shared state, staleness, and error capture"
```

---

### Task 4: App state machine

**Files:**
- Modify: `xet_console/src/app.rs`

The App is pure (no I/O, no ratatui): pages, per-pane selection, key handling. The overview drills via a flattened entry list built from the snapshot.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<OverviewEntry> {
        vec![
            OverviewEntry::Session { session_idx: 0 },
            OverviewEntry::Commit { session_idx: 0, commit_idx: 0 },
            OverviewEntry::Group { session_idx: 0, group_idx: 0 },
        ]
    }

    #[test]
    fn quit_help_pause_toggle() {
        let mut app = App::default();
        app.handle_key(KeyCode::Char('?'), &entries());
        assert!(app.show_help);
        app.handle_key(KeyCode::Char('?'), &entries());
        assert!(!app.show_help);
        app.handle_key(KeyCode::Char('p'), &entries());
        assert!(app.paused);
        app.handle_key(KeyCode::Char('q'), &entries());
        assert!(app.should_quit);
    }

    #[test]
    fn overview_drill_into_commit_and_back() {
        let mut app = App::default();
        assert_eq!(app.page, Page::Overview);
        app.handle_key(KeyCode::Char('j'), &entries()); // row 1 = the commit
        app.handle_key(KeyCode::Enter, &entries());
        assert_eq!(app.page, Page::Upload);
        assert_eq!(app.session_idx, 0);
        assert_eq!(app.commit_idx, 0);
        app.handle_key(KeyCode::Esc, &entries());
        assert_eq!(app.page, Page::Overview);
    }

    #[test]
    fn overview_drill_into_group() {
        let mut app = App::default();
        app.overview_row = 2;
        app.handle_key(KeyCode::Enter, &entries());
        assert_eq!(app.page, Page::Download);
        assert_eq!(app.group_idx, 0);
    }

    #[test]
    fn number_keys_jump_pages_and_tab_cycles_panes() {
        let mut app = App::default();
        app.handle_key(KeyCode::Char('4'), &entries());
        assert_eq!(app.page, Page::Concurrency);
        app.handle_key(KeyCode::Char('2'), &entries());
        assert_eq!(app.page, Page::Upload);
        assert_eq!(app.pane, Pane::Main);
        app.handle_key(KeyCode::Tab, &entries());
        assert_eq!(app.pane, Pane::SideTop);
        app.handle_key(KeyCode::Tab, &entries());
        assert_eq!(app.pane, Pane::SideBottom);
        app.handle_key(KeyCode::Tab, &entries());
        assert_eq!(app.pane, Pane::Main);
        app.handle_key(KeyCode::Char('1'), &entries());
        assert_eq!(app.page, Page::Overview);
    }

    #[test]
    fn selection_moves_saturate_at_zero() {
        let mut app = App::default();
        app.handle_key(KeyCode::Char('k'), &entries());
        assert_eq!(app.overview_row, 0);
        app.handle_key(KeyCode::Down, &entries());
        app.handle_key(KeyCode::Down, &entries());
        app.handle_key(KeyCode::Down, &entries()); // clamped to last entry (2)
        assert_eq!(app.overview_row, 2);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-console app` — compile error.

- [ ] **Step 3: Implement**

```rust
use crossterm::event::KeyCode;
use xet_runtime::console::model::SnapshotResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Page {
    #[default]
    Overview,
    Upload,
    Download,
    Concurrency,
}

/// Pane focus within a detail page (layout A): Main = files table,
/// SideTop = xorbs / term blocks, SideBottom = shards / prefetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Pane {
    #[default]
    Main,
    SideTop,
    SideBottom,
}

/// One selectable row on the overview page, in render order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverviewEntry {
    Session { session_idx: usize },
    Commit { session_idx: usize, commit_idx: usize },
    Group { session_idx: usize, group_idx: usize },
}

/// Flatten the snapshot into overview rows: each session header, then its
/// commits (live details first, then ended), then its groups (live then ended).
/// Render and key handling share this so selection always matches the screen.
pub fn overview_entries(snapshot: &SnapshotResponse) -> Vec<OverviewEntry> {
    let mut out = Vec::new();
    for (si, s) in snapshot.sessions.iter().enumerate() {
        out.push(OverviewEntry::Session { session_idx: si });
        let n_commits = s.upload_commit_details.len() + s.detail.ended_upload_commits.len();
        for ci in 0..n_commits {
            out.push(OverviewEntry::Commit { session_idx: si, commit_idx: ci });
        }
        let n_groups = s.download_group_details.len() + s.detail.ended_download_groups.len();
        for gi in 0..n_groups {
            out.push(OverviewEntry::Group { session_idx: si, group_idx: gi });
        }
    }
    out
}

#[derive(Debug, Default)]
pub struct App {
    pub page: Page,
    pub pane: Pane,
    pub show_help: bool,
    pub paused: bool,
    pub should_quit: bool,
    pub session_idx: usize,
    pub commit_idx: usize,
    pub group_idx: usize,
    pub overview_row: usize,
    pub main_row: usize, // files table row on detail pages
    pub monitor_row: usize,
}

impl App {
    pub fn handle_key(&mut self, code: KeyCode, entries: &[OverviewEntry]) {
        if self.show_help {
            // Any key dismisses help except q which still quits.
            if code == KeyCode::Char('q') {
                self.should_quit = true;
            }
            self.show_help = false;
            return;
        }
        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('p') => self.paused = !self.paused,
            KeyCode::Char('1') => self.page = Page::Overview,
            KeyCode::Char('2') => self.enter_detail(Page::Upload),
            KeyCode::Char('3') => self.enter_detail(Page::Download),
            KeyCode::Char('4') => self.page = Page::Concurrency,
            KeyCode::Esc => {
                self.page = Page::Overview;
                self.pane = Pane::Main;
            },
            KeyCode::Tab => {
                if matches!(self.page, Page::Upload | Page::Download) {
                    self.pane = match self.pane {
                        Pane::Main => Pane::SideTop,
                        Pane::SideTop => Pane::SideBottom,
                        Pane::SideBottom => Pane::Main,
                    };
                }
            },
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(1, entries.len()),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(-1, entries.len()),
            KeyCode::Enter => {
                if self.page == Page::Overview
                    && let Some(entry) = entries.get(self.overview_row)
                {
                    match *entry {
                        OverviewEntry::Session { session_idx } => self.session_idx = session_idx,
                        OverviewEntry::Commit { session_idx, commit_idx } => {
                            self.session_idx = session_idx;
                            self.commit_idx = commit_idx;
                            self.enter_detail(Page::Upload);
                        },
                        OverviewEntry::Group { session_idx, group_idx } => {
                            self.session_idx = session_idx;
                            self.group_idx = group_idx;
                            self.enter_detail(Page::Download);
                        },
                    }
                }
            },
            _ => {},
        }
    }

    fn enter_detail(&mut self, page: Page) {
        self.page = page;
        self.pane = Pane::Main;
        self.main_row = 0;
    }

    fn move_selection(&mut self, delta: i64, overview_len: usize) {
        let bump = |v: &mut usize, max: usize| {
            let next = (*v as i64 + delta).max(0) as usize;
            *v = if max == 0 { 0 } else { next.min(max - 1) };
        };
        match self.page {
            // Detail-pane and monitor rows are clamped against live data at
            // render time; here they only need the saturating move.
            Page::Overview => bump(&mut self.overview_row, overview_len),
            Page::Concurrency => bump(&mut self.monitor_row, usize::MAX),
            Page::Upload | Page::Download => match self.pane {
                Pane::Main => bump(&mut self.main_row, usize::MAX),
                // v1: pane focus is visual; side panes render whole and have
                // no independent selection. (Per-pane scrolling is deferred.)
                Pane::SideTop | Pane::SideBottom => {},
            },
        }
    }
}
```

(`usize::MAX` bound: `next.min(usize::MAX - 1)` — fine. If clippy objects to the arithmetic, restructure with `saturating_add_signed`.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p xet-console app` — 5 passed. Clippy clean.

- [ ] **Step 5: Commit**

```bash
git add xet_console/src/app.rs
git commit -m "feat(xet-console): pure app state machine with page/pane navigation"
```

---

### Task 5: Widget helpers + test fixtures

**Files:**
- Modify: `xet_console/src/ui/widgets.rs`
- Create: `xet_console/src/fixtures.rs` (replace stub)

- [ ] **Step 1: Write the failing helper tests**

`ui/widgets.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humanize_bytes_picks_sane_units() {
        assert_eq!(humanize_bytes(0), "0 B");
        assert_eq!(humanize_bytes(1023), "1023 B");
        assert_eq!(humanize_bytes(1024), "1.0 KiB");
        assert_eq!(humanize_bytes(4 * 1024 * 1024), "4.0 MiB");
        assert_eq!(humanize_bytes(5_368_709_120), "5.0 GiB");
    }

    #[test]
    fn humanize_rate_handles_none() {
        assert_eq!(humanize_rate(None), "–");
        assert_eq!(humanize_rate(Some(412_000_000.0)), "392.9 MiB/s");
    }

    #[test]
    fn short_hash_truncates_with_ellipsis() {
        assert_eq!(short_hash("f3ab129944aa"), "f3ab12…");
        assert_eq!(short_hash("abc"), "abc");
    }

    #[test]
    fn percent_is_safe_on_zero_total() {
        assert_eq!(percent(0, 0), 0);
        assert_eq!(percent(50, 200), 25);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-console widgets` — compile error.

- [ ] **Step 3: Implement the helpers**

```rust
use ratatui::style::{Color, Style};
use xet_runtime::console::model::{
    DownloadGroupState, FileDownloadState, FileUploadState, SessionState, ShardState, TermState,
    UploadCommitState, XorbState,
};

pub fn humanize_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    format!("{v:.1} {}", UNITS[unit])
}

pub fn humanize_rate(bps: Option<f64>) -> String {
    match bps {
        Some(v) if v.is_finite() && v > 0.0 => format!("{}/s", humanize_bytes(v as u64)),
        _ => "–".to_string(),
    }
}

pub fn short_hash(h: &str) -> String {
    if h.len() > 6 { format!("{}…", &h[..6]) } else { h.to_string() }
}

pub fn percent(done: u64, total: u64) -> u16 {
    if total == 0 { 0 } else { ((done.saturating_mul(100)) / total).min(100) as u16 }
}

/// One style language for all lifecycle states across pages.
pub fn state_style(label: &str) -> Style {
    match label {
        "complete" | "completed" | "uploaded" | "finished" | "consumed" => Style::default().fg(Color::Green),
        "failed" | "aborted" => Style::default().fg(Color::Red),
        "queued" | "enqueued" | "staging" | "formed" => Style::default().fg(Color::DarkGray),
        _ => Style::default().fg(Color::Cyan), // any in-flight state
    }
}

// snake_case labels matching the wire encoding, so the UI vocabulary matches
// what agents see over HTTP.
pub fn upload_state_label(s: FileUploadState) -> &'static str {
    match s {
        FileUploadState::Queued => "queued",
        FileUploadState::Chunking => "chunking",
        FileUploadState::Processed => "processed",
        FileUploadState::AwaitingXorbs => "awaiting_xorbs",
        FileUploadState::AwaitingShard => "awaiting_shard",
        FileUploadState::Complete => "complete",
        FileUploadState::Failed => "failed",
        FileUploadState::Aborted => "aborted",
    }
}

pub fn commit_state_label(s: UploadCommitState) -> &'static str {
    match s {
        UploadCommitState::Active => "active",
        UploadCommitState::Committing => "committing",
        UploadCommitState::Completed => "completed",
        UploadCommitState::Aborted => "aborted",
    }
}

pub fn xorb_state_label(s: XorbState) -> &'static str {
    match s {
        XorbState::Formed => "formed",
        XorbState::Queued => "queued",
        XorbState::Uploading => "uploading",
        XorbState::Uploaded => "uploaded",
        XorbState::Failed => "failed",
    }
}

pub fn shard_state_label(s: ShardState) -> &'static str {
    match s {
        ShardState::Staging => "staging",
        ShardState::Uploading => "uploading",
        ShardState::Uploaded => "uploaded",
    }
}

pub fn download_state_label(s: FileDownloadState) -> &'static str {
    match s {
        FileDownloadState::Queued => "queued",
        FileDownloadState::Reconstructing => "reconstructing",
        FileDownloadState::Complete => "complete",
        FileDownloadState::Failed => "failed",
        FileDownloadState::Aborted => "aborted",
    }
}

pub fn group_state_label(s: DownloadGroupState) -> &'static str {
    match s {
        DownloadGroupState::Active => "active",
        DownloadGroupState::Finished => "finished",
        DownloadGroupState::Aborted => "aborted",
    }
}

pub fn term_state_label(s: TermState) -> &'static str {
    match s {
        TermState::Enqueued => "enqueued",
        TermState::Fetching => "fetching",
        TermState::Fetched => "fetched",
        TermState::Consumed => "consumed",
    }
}

pub fn session_state_label(s: SessionState) -> &'static str {
    match s {
        SessionState::Active => "active",
        SessionState::Ended => "ended",
    }
}

/// Flattens a TestBackend buffer to a newline-joined string for content asserts.
#[cfg(test)]
pub fn buffer_text(backend: &ratatui::backend::TestBackend) -> String {
    let buffer = backend.buffer();
    let area = buffer.area;
    let mut out = String::new();
    for y in 0..area.height {
        for x in 0..area.width {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    out
}
```

(Buffer cell indexing: ratatui ≥0.26 supports `buffer[(x, y)]`; older needs `buffer.get(x, y)` — follow the compiler. `humanize_rate(Some(412e6))`: 412_000_000/1024² = 392.9 — the test value is precomputed to match.)

- [ ] **Step 4: Write the fixture**

Replace `fixtures.rs` (compiled only for tests via the `#[cfg(test)] mod fixtures;` declaration in main.rs):

```rust
//! One canonical fixture snapshot shared by every render test. Covers: an
//! active session with one active commit (mixed file states, in-flight +
//! recent xorbs, staging + uploaded shards), one ended commit, one active
//! download group (term blocks + prefetch), one monitor with history, and an
//! ended session in the registry sense (not present here: /snapshot only
//! carries live sessions).

use xet_runtime::console::model::*;

pub fn sample_snapshot() -> SnapshotResponse {
    let progress = ProgressSnapshot {
        total_bytes: 8 << 30,
        bytes_completed: 3 << 30,
        rate_bps: Some(412_000_000.0),
        transfer_bytes: 1 << 30,
        transfer_bytes_completed: 900 << 20,
        transfer_rate_bps: Some(380_000_000.0),
    };
    let dedup = DedupSnapshot {
        total_bytes: 3 << 30,
        deduped_bytes: 2 << 30,
        new_bytes: 1 << 30,
        deduped_bytes_by_global_dedup: 500 << 20,
        defrag_prevented_dedup_bytes: 1 << 20,
        xorb_bytes_uploaded: 800 << 20,
        shard_bytes_uploaded: 256 << 10,
    };
    let files = vec![
        FileUploadSnapshot {
            id: 12,
            name: "model-00001.safetensors".into(),
            size: Some(4 << 30),
            state: FileUploadState::Chunking,
            bytes_chunked: 1 << 30,
            n_chunks: 16384,
            file_hash: None,
            sha256: None,
            dedup: None,
            xorb_deps: vec![],
            shard_uploaded: false,
            created_at: 1,
            finished_at: None,
        },
        FileUploadSnapshot {
            id: 13,
            name: "tokenizer.json".into(),
            size: Some(9 << 20),
            state: FileUploadState::AwaitingShard,
            bytes_chunked: 9 << 20,
            n_chunks: 80,
            file_hash: Some("9c01dd00aa00bb00".into()),
            sha256: Some("ff".repeat(32)),
            dedup: Some(dedup.clone()),
            xorb_deps: vec![XorbDepSnapshot { xorb_hash: "f3ab12cd".into(), n_bytes: 9 << 20, uploaded: true }],
            shard_uploaded: false,
            created_at: 1,
            finished_at: None,
        },
    ];
    let commit = UploadCommitDetail {
        as_of: 1000,
        id: 7,
        state: UploadCommitState::Active,
        created_at: 1,
        endpoint: Some("local://cas".into()),
        progress: Some(progress.clone()),
        dedup: dedup.clone(),
        files,
        completed_files: vec![(
            900,
            FileUploadSnapshot {
                id: 11,
                name: "config.json".into(),
                size: Some(4096),
                state: FileUploadState::Complete,
                bytes_chunked: 4096,
                n_chunks: 1,
                file_hash: Some("4be211aa00".into()),
                sha256: None,
                dedup: None,
                xorb_deps: vec![],
                shard_uploaded: true,
                created_at: 1,
                finished_at: Some(900),
            },
        )],
        file_counts: FileCounts { in_flight: 2, completed: 1, failed: 0, aborted: 0 },
        xorbs: XorbsSnapshot {
            in_flight: vec![XorbSnapshot {
                hash: "f3ab12cd5566".into(),
                state: XorbState::Uploading,
                raw_bytes: 64 << 20,
                serialized_bytes: 60 << 20,
                bytes_transferred: 30 << 20,
                n_files: 1,
                created_at: 2,
                finished_at: None,
            }],
            counts: XorbCounts { formed: 48, uploaded: 46, failed: 1 },
            recent: vec![(
                950,
                XorbSnapshot {
                    hash: "9c01ee".into(),
                    state: XorbState::Uploaded,
                    raw_bytes: 64 << 20,
                    serialized_bytes: 61 << 20,
                    bytes_transferred: 61 << 20,
                    n_files: 2,
                    created_at: 2,
                    finished_at: Some(950),
                },
            )],
        },
        shards: vec![
            ShardSnapshot { hash: None, state: ShardState::Staging, n_xorbs: 48, size: 0 },
            ShardSnapshot { hash: Some("aa55cc".into()), state: ShardState::Uploaded, n_xorbs: 12, size: 262_144 },
        ],
    };
    let ended_commit = UploadCommitDetail {
        as_of: 500,
        id: 5,
        state: UploadCommitState::Completed,
        created_at: 0,
        endpoint: Some("local://cas".into()),
        progress: Some(progress.clone()),
        dedup: dedup.clone(),
        files: vec![],
        completed_files: vec![],
        file_counts: FileCounts { in_flight: 0, completed: 8, failed: 0, aborted: 0 },
        xorbs: XorbsSnapshot::default(),
        shards: vec![],
    };
    let group = DownloadGroupDetail {
        as_of: 1000,
        id: 3,
        kind: DownloadGroupKind::Files,
        state: DownloadGroupState::Active,
        created_at: 5,
        endpoint: Some("local://cas".into()),
        progress: Some(progress.clone()),
        files: vec![FileDownloadSnapshot {
            id: 21,
            name: "out.bin".into(),
            file_hash: Some("ffee9900".into()),
            requested_range: None,
            total_bytes: 1 << 30,
            bytes_completed: 600 << 20,
            state: FileDownloadState::Reconstructing,
            prefetch: Some(PrefetchSnapshot {
                queue_depth: 2,
                prefetched_byte_position: 800 << 20,
                active_byte_position: 600 << 20,
                completion_rate_bps: Some(900_000_000.0),
            }),
            term_blocks: vec![TermBlockSnapshot {
                block_id: 4,
                byte_range: (600 << 20, 700 << 20),
                state: TermState::Fetching,
                terms: vec![],
                created_at: 6,
                fetched_at: None,
            }],
            consumed_blocks: 5,
            created_at: 5,
            finished_at: None,
        }],
        completed_files: vec![],
        file_counts: FileCounts { in_flight: 1, completed: 0, failed: 0, aborted: 0 },
    };
    let monitor = MonitorSnapshot {
        tag: "upload".into(),
        total_permits: 16,
        active_permits: 14,
        available_permits: 2,
        bounds: PermitBounds { min: 1, max: 64 },
        adjustment_enabled: true,
        bytes_sent: 3 << 30,
        success: Some(SuccessModelSnapshot {
            success_ratio: 0.94,
            thresholds: Thresholds { increase: 0.8, decrease: 0.5 },
            recommended_adjustment: AdjustmentRecommendation::Increase,
        }),
        latency: Some(LatencyModelSnapshot {
            predicted_max_rtt_ms: Some(412.5),
            rtt_standard_error_ms: Some(38.0),
            predicted_bandwidth_bps: Some(510_000_000.0),
        }),
        limit_history: vec![(100, 8), (200, 10), (300, 12), (400, 14), (500, 16)],
    };
    let detail = SessionDetail {
        as_of: 1000,
        id: "9f3c0000-1111-2222-3333-444455556666".into(),
        state: SessionState::Active,
        created_at: 0,
        config: vec![("max_concurrent_file_ingestion".into(), "8".into())],
        monitors: vec![monitor],
        upload_commits: vec![],
        ended_upload_commits: vec![ended_commit.clone()],
        download_groups: vec![],
        ended_download_groups: vec![],
    };
    SnapshotResponse {
        as_of: 1000,
        process: ProcessInfo {
            as_of: 1000,
            pid: 4242,
            argv: vec!["pytest".into()],
            start_time_ms: 0,
            version: "1.5.2".into(),
            n_active_sessions: 1,
        },
        sessions: vec![SessionFull {
            detail,
            upload_commit_details: vec![commit],
            download_group_details: vec![group],
        }],
    }
}
```

- [ ] **Step 5: Run tests + commit**

Run: `cargo test -p xet-console` — widgets tests pass, fixture compiles (no test uses it yet; if dead-code warnings fire under `-D warnings` for fixture fields, they won't — it's a `pub fn` in a cfg(test) module consumed next task; add `#[allow(dead_code)]` on `sample_snapshot` only if clippy complains this task).

```bash
git add xet_console/src/ui/widgets.rs xet_console/src/fixtures.rs
git commit -m "feat(xet-console): widget helpers and canonical render fixture"
```

---

### Task 6: Overview page + draw dispatch

**Files:**
- Modify: `xet_console/src/ui/mod.rs`
- Create: `xet_console/src/ui/overview.rs`
- Modify: `xet_console/src/main.rs` (run loop uses App + Shared + ui::draw)

- [ ] **Step 1: Write the failing render test**

`ui/overview.rs`:

```rust
#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use crate::app::App;
    use crate::fixtures::sample_snapshot;
    use crate::poll::PollState;
    use crate::ui;
    use crate::ui::widgets::buffer_text;

    #[test]
    fn overview_lists_sessions_commits_groups_monitors_and_ended_sessions() {
        use xet_runtime::console::model::{SessionState, SessionSummary};
        let snap = sample_snapshot();
        let state = PollState {
            snapshot: Some(snap),
            ended_sessions: vec![SessionSummary {
                id: "41bcdead-0000-0000-0000-000000000000".into(),
                state: SessionState::Ended,
                created_at: 0,
                n_upload_commits: 2,
                n_download_groups: 0,
                n_monitors: 0,
            }],
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App::default();
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("9f3c0000"), "session id shown:\n{text}");
        assert!(text.contains("commit #7"), "live commit listed:\n{text}");
        assert!(text.contains("commit #5"), "ended commit listed:\n{text}");
        assert!(text.contains("group #3"), "download group listed:\n{text}");
        assert!(text.contains("upload 14/16"), "monitor one-liner shown:\n{text}");
        assert!(text.contains("ended sessions: 41bcdead"), "ended-session footer:\n{text}");
        assert!(text.contains("[1]overview"), "key bar present:\n{text}");
    }

    #[test]
    fn disconnected_banner_when_no_data() {
        let state = PollState::default();
        let app = App::default();
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("no data"), "disconnected hint shown:\n{text}");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-console overview` — compile error (`ui::draw` missing).

- [ ] **Step 3: Implement dispatch + overview**

`ui/mod.rs`:

```rust
pub mod widgets;

pub mod concurrency;
pub mod download;
pub mod overview;
pub mod upload;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use crate::app::{App, Page};
use crate::poll::{PollState, now_ms};

pub fn draw(f: &mut Frame, app: &App, state: &PollState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1), Constraint::Length(1)])
        .split(f.area());

    draw_header(f, chunks[0], app, state);

    match &state.snapshot {
        None => {
            let msg = match &state.last_error {
                Some(e) => format!("no data — {e}"),
                None => "no data — waiting for first poll…".to_string(),
            };
            f.render_widget(Paragraph::new(msg).style(Style::default().fg(Color::Red)), chunks[1]);
        },
        Some(snap) => match app.page {
            // Overview also renders the ended-sessions footer, so it takes the
            // whole PollState; detail pages need only the snapshot.
            Page::Overview => overview::draw(f, chunks[1], app, snap, &state.ended_sessions),
            Page::Upload => upload::draw(f, chunks[1], app, snap),
            Page::Download => download::draw(f, chunks[1], app, snap),
            Page::Concurrency => concurrency::draw(f, chunks[1], app, snap),
        },
    }

    draw_key_bar(f, chunks[2]);

    if app.show_help {
        draw_help(f);
    }
}

fn draw_header(f: &mut Frame, area: Rect, app: &App, state: &PollState) {
    let age = state.last_success_ms.map(|t| now_ms().saturating_sub(t));
    let status = if app.paused {
        "PAUSED".to_string()
    } else if let Some(e) = &state.last_error {
        format!("DISCONNECTED: {e}")
    } else {
        match age {
            Some(ms) if ms > 3000 => format!("stale {}s", ms / 1000),
            Some(_) => "live".to_string(),
            None => "connecting".to_string(),
        }
    };
    let pid = state.snapshot.as_ref().map(|s| s.process.pid).unwrap_or(0);
    let line = format!(" xet-console — pid {pid} — {status}");
    let style = if app.paused || state.last_error.is_some() {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().add_modifier(Modifier::BOLD)
    };
    f.render_widget(Paragraph::new(line).style(style), area);
}

fn draw_key_bar(f: &mut Frame, area: Rect) {
    f.render_widget(
        Paragraph::new(" [1]overview [2]upload [3]download [4]concurrency  ↑↓/jk ⏎ drill  tab pane  esc back  p pause  ? help  q quit")
            .style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn draw_help(f: &mut Frame) {
    let area = centered_rect(60, 14, f.area());
    f.render_widget(Clear, area);
    let text = "\
  1/2/3/4   jump to page
  ↑↓ or jk  move selection
  ⏎         drill into selected item
  tab       cycle pane focus (detail pages)
  esc       back to overview
  p         pause/resume polling
  q         quit

  any key closes this help";
    f.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL).title(" help ")),
        area,
    );
}

fn centered_rect(width: u16, height: u16, parent: Rect) -> Rect {
    let w = width.min(parent.width);
    let h = height.min(parent.height);
    Rect {
        x: parent.x + (parent.width - w) / 2,
        y: parent.y + (parent.height - h) / 2,
        width: w,
        height: h,
    }
}
```

`ui/overview.rs` (above its tests):

```rust
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use xet_runtime::console::model::{SessionSummary, SnapshotResponse};

use crate::app::{App, OverviewEntry, overview_entries};
use crate::ui::widgets::*;

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse, ended_sessions: &[SessionSummary]) {
    let (list_area, footer_area) = if ended_sessions.is_empty() {
        (area, None)
    } else {
        let split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(1)])
            .split(area);
        (split[0], Some(split[1]))
    };

    let entries = overview_entries(snap);
    let items: Vec<ListItem> = entries.iter().map(|e| ListItem::new(entry_line(e, snap))).collect();

    let mut state = ListState::default();
    if !entries.is_empty() {
        state.select(Some(app.overview_row.min(entries.len() - 1)));
    }
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(format!(
            " sessions ({}) — as_of {} ",
            snap.sessions.len(),
            snap.as_of
        )))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("▸ ");
    f.render_stateful_widget(list, list_area, &mut state);

    if let Some(footer) = footer_area {
        let ids = ended_sessions
            .iter()
            .map(|s| s.id[..8.min(s.id.len())].to_string())
            .collect::<Vec<_>>()
            .join(", ");
        f.render_widget(
            Paragraph::new(format!(" ended sessions: {ids}"))
                .style(Style::default().fg(ratatui::style::Color::DarkGray)),
            footer,
        );
    }
}

fn entry_line(e: &OverviewEntry, snap: &SnapshotResponse) -> String {
    match *e {
        OverviewEntry::Session { session_idx } => {
            let s = &snap.sessions[session_idx];
            let monitors = s
                .detail
                .monitors
                .iter()
                .map(|m| format!("{} {}/{}", m.tag, m.active_permits, m.total_permits))
                .collect::<Vec<_>>()
                .join(" · ");
            format!(
                "session {} [{}]  monitors: {}",
                &s.detail.id[..8.min(s.detail.id.len())],
                session_state_label(s.detail.state),
                if monitors.is_empty() { "none".to_string() } else { monitors }
            )
        },
        OverviewEntry::Commit { session_idx, commit_idx } => {
            let s = &snap.sessions[session_idx];
            let live = &s.upload_commit_details;
            let (c, ended) = if commit_idx < live.len() {
                (&live[commit_idx], false)
            } else {
                (&s.detail.ended_upload_commits[commit_idx - live.len()], true)
            };
            let pct = c.progress.as_ref().map(|p| percent(p.bytes_completed, p.total_bytes)).unwrap_or(0);
            let rate = humanize_rate(c.progress.as_ref().and_then(|p| p.rate_bps));
            format!(
                "  ▲ commit #{} [{}] {}% {} files {}/{}{}",
                c.id,
                commit_state_label(c.state),
                pct,
                rate,
                c.file_counts.completed,
                c.file_counts.completed + c.file_counts.in_flight as u64,
                if ended { "  (ended)" } else { "" }
            )
        },
        OverviewEntry::Group { session_idx, group_idx } => {
            let s = &snap.sessions[session_idx];
            let live = &s.download_group_details;
            let (g, ended) = if group_idx < live.len() {
                (&live[group_idx], false)
            } else {
                (&s.detail.ended_download_groups[group_idx - live.len()], true)
            };
            let pct = g.progress.as_ref().map(|p| percent(p.bytes_completed, p.total_bytes)).unwrap_or(0);
            format!(
                "  ▼ group #{} [{}] {}% {} files in flight{}",
                g.id,
                group_state_label(g.state),
                pct,
                g.file_counts.in_flight,
                if ended { "  (ended)" } else { "" }
            )
        },
    }
}
```

Create empty-for-now `ui/upload.rs`, `ui/download.rs`, `ui/concurrency.rs` so the dispatch compiles, each with a placeholder that is REAL code (not a todo) and gets replaced next tasks:

```rust
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::Paragraph;
use xet_runtime::console::model::SnapshotResponse;

use crate::app::App;

pub fn draw(f: &mut Frame, area: Rect, _app: &App, _snap: &SnapshotResponse) {
    f.render_widget(Paragraph::new("(page arrives in a later task)"), area);
}
```

- [ ] **Step 4: Wire the real run loop in main.rs**

Replace `run` and the call site in `main`:

```rust
fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let base = resolve_base(args.target.as_deref(), args.port);

    let shared = std::sync::Arc::new(poll::Shared::default());
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let poller = poll::spawn_poller(
        client::ConsoleClient::new(base.clone()),
        shared.clone(),
        args.interval,
        shutdown.clone(),
    );

    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_hook(info);
    }));

    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;

    let result = run(&mut terminal, &shared);

    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen)?;
    shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = poller.join();
    result
}

fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    shared: &poll::Shared,
) -> anyhow::Result<()> {
    let mut app = app::App::default();
    loop {
        {
            let state = shared.lock();
            terminal.draw(|f| ui::draw(f, &app, &state))?;
        }
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    let entries = {
                        let state = shared.lock();
                        state.snapshot.as_ref().map(|s| app::overview_entries(s)).unwrap_or_default()
                    };
                    app.handle_key(key.code, &entries);
                    shared.lock().paused = app.paused;
                }
            }
        }
        if app.should_quit {
            return Ok(());
        }
    }
}
```

(Lock discipline: the `MutexGuard` is dropped before `event::poll` — never hold it across the 100ms wait.)

- [ ] **Step 5: Run tests + commit**

Run: `cargo test -p xet-console` (overview tests + all prior pass), `cargo clippy -p xet-console -- -D warnings` clean. Optional manual smoke: in one shell `cargo test -p hf-xet --features "console simulation" --test test_console -- --nocapture` won't stay up long; better: `XET_CONSOLE_PORT=6660 cargo run -p hf-xet --example …` — skip manual here, Task 11 covers it.

```bash
git add xet_console/src/ui/ xet_console/src/main.rs
git commit -m "feat(xet-console): overview page, draw dispatch, header/help/key bar, run loop"
```

---

### Task 7: Upload commit detail page (layout A)

**Files:**
- Modify: `xet_console/src/ui/upload.rs` (replace placeholder)

Layout A per the spec: header strip (progress + dedup), files table left ~60%, xorbs pane right-top, shards pane right-bottom, recent line at the bottom.

- [ ] **Step 1: Write the failing render test**

```rust
#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use crate::app::{App, Page};
    use crate::fixtures::sample_snapshot;
    use crate::poll::PollState;
    use crate::ui;
    use crate::ui::widgets::buffer_text;

    fn render(app: &App) -> String {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, app, &state)).unwrap();
        buffer_text(terminal.backend())
    }

    #[test]
    fn upload_page_shows_layout_a_panes() {
        let app = App { page: Page::Upload, ..Default::default() };
        let text = render(&app);
        // header strip
        assert!(text.contains("commit #7"), "{text}");
        assert!(text.contains("dedup"), "{text}");
        // files table (left)
        assert!(text.contains("model-00001.safetensors"), "{text}");
        assert!(text.contains("chunking"), "{text}");
        assert!(text.contains("tokenizer.json"), "{text}");
        assert!(text.contains("awaiting_shard"), "{text}");
        assert!(text.contains("9c01dd…"), "short hash shown: {text}");
        // completed file folded into the table after in-flight rows
        assert!(text.contains("config.json"), "{text}");
        // xorbs pane (right top)
        assert!(text.contains("xorbs"), "{text}");
        assert!(text.contains("f3ab12…"), "{text}");
        assert!(text.contains("done 46"), "{text}");
        assert!(text.contains("failed 1"), "{text}");
        // shards pane (right bottom)
        assert!(text.contains("shards"), "{text}");
        assert!(text.contains("staging"), "{text}");
        assert!(text.contains("uploaded"), "{text}");
    }

    #[test]
    fn upload_page_handles_missing_commit_gracefully() {
        let app = App { page: Page::Upload, commit_idx: 99, ..Default::default() };
        let text = render(&app);
        assert!(text.contains("no such commit"), "{text}");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p xet-console upload` — assertions fail against the placeholder.

- [ ] **Step 3: Implement**

Replace `ui/upload.rs`'s draw with:

```rust
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};
use xet_runtime::console::model::{SnapshotResponse, UploadCommitDetail};

use crate::app::{App, Pane};
use crate::ui::widgets::*;

/// Selected commit for the page: live details first, then ended (same order
/// as overview_entries builds rows).
fn selected_commit<'a>(app: &App, snap: &'a SnapshotResponse) -> Option<&'a UploadCommitDetail> {
    let s = snap.sessions.get(app.session_idx)?;
    let live = &s.upload_commit_details;
    if app.commit_idx < live.len() {
        live.get(app.commit_idx)
    } else {
        s.detail.ended_upload_commits.get(app.commit_idx - live.len())
    }
}

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse) {
    let Some(c) = selected_commit(app, snap) else {
        f.render_widget(Paragraph::new("no such commit — esc to go back"), area);
        return;
    };

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(3), Constraint::Length(1)])
        .split(area);

    draw_header(f, rows[0], c);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(rows[1]);
    draw_files(f, cols[0], app, c);

    let side = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(cols[1]);
    draw_xorbs(f, side[0], app, c);
    draw_shards(f, side[1], app, c);

    draw_recent(f, rows[2], c);
}

fn draw_header(f: &mut Frame, area: Rect, c: &UploadCommitDetail) {
    let (pct, done, total, rate) = match &c.progress {
        Some(p) => (
            percent(p.bytes_completed, p.total_bytes),
            humanize_bytes(p.bytes_completed),
            humanize_bytes(p.total_bytes),
            humanize_rate(p.rate_bps),
        ),
        None => (0, "?".into(), "?".into(), "–".into()),
    };
    let d = &c.dedup;
    let dedup_pct = percent(d.deduped_bytes, d.total_bytes);
    let lines = vec![
        Line::from(format!(
            " ▲ commit #{} [{}]  {done} / {total} ({pct}%)  {rate}",
            c.id,
            commit_state_label(c.state),
        )),
        Line::from(format!(
            " dedup {dedup_pct}%: {} deduped ({} global) · {} new · xorb↑ {} · shard↑ {}",
            humanize_bytes(d.deduped_bytes),
            humanize_bytes(d.deduped_bytes_by_global_dedup),
            humanize_bytes(d.new_bytes),
            humanize_bytes(d.xorb_bytes_uploaded),
            humanize_bytes(d.shard_bytes_uploaded),
        )),
    ];
    f.render_widget(Paragraph::new(lines).style(Style::default().add_modifier(Modifier::BOLD)), area);
}

fn draw_files(f: &mut Frame, area: Rect, app: &App, c: &UploadCommitDetail) {
    // In-flight rows first, then the recent-completions ring (oldest first).
    let mut rows: Vec<Row> = Vec::new();
    for file in &c.files {
        let pct = file.size.map(|s| percent(file.bytes_chunked, s)).unwrap_or(0);
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(upload_state_label(file.state)).style(state_style(upload_state_label(file.state))),
            Cell::from(format!("{pct:>3}%")),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
            Cell::from(if file.shard_uploaded { "✓" } else { "–" }),
        ]));
    }
    for (_, file) in &c.completed_files {
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(upload_state_label(file.state)).style(state_style(upload_state_label(file.state))),
            Cell::from("100%"),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
            Cell::from(if file.shard_uploaded { "✓" } else { "–" }),
        ]));
    }
    let n = rows.len();
    let mut tstate = TableState::default();
    if n > 0 {
        tstate.select(Some(app.main_row.min(n - 1)));
    }
    let focused = app.pane == Pane::Main;
    let table = Table::new(
        rows,
        [
            Constraint::Min(24),
            Constraint::Length(15),
            Constraint::Length(5),
            Constraint::Length(9),
            Constraint::Length(5),
        ],
    )
    .header(Row::new(vec!["name", "state", "prog", "hash", "shard"]).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(pane_block(format!(" files ({}/{} active) ", c.file_counts.in_flight, n), focused))
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    f.render_stateful_widget(table, area, &mut tstate);
}

fn draw_xorbs(f: &mut Frame, area: Rect, app: &App, c: &UploadCommitDetail) {
    let mut lines: Vec<Line> = Vec::new();
    for x in &c.xorbs.in_flight {
        let pct = percent(x.bytes_transferred, x.serialized_bytes.max(1));
        lines.push(Line::styled(
            format!("{} {} {pct}%", short_hash(&x.hash), xorb_state_label(x.state)),
            state_style(xorb_state_label(x.state)),
        ));
    }
    lines.push(Line::from(format!(
        "done {} · failed {} · formed {}",
        c.xorbs.counts.uploaded, c.xorbs.counts.failed, c.xorbs.counts.formed
    )));
    for (_, x) in c.xorbs.recent.iter().rev().take(3) {
        lines.push(Line::from(format!("recent {} {}", short_hash(&x.hash), xorb_state_label(x.state))));
    }
    let focused = app.pane == Pane::SideTop;
    f.render_widget(
        Paragraph::new(lines).block(pane_block(format!(" xorbs ({} in-flight) ", c.xorbs.in_flight.len()), focused)),
        area,
    );
}

fn draw_shards(f: &mut Frame, area: Rect, app: &App, c: &UploadCommitDetail) {
    let lines: Vec<Line> = c
        .shards
        .iter()
        .map(|s| {
            Line::styled(
                format!(
                    "{} {} — {} xorbs, {}",
                    s.hash.as_deref().map(short_hash).unwrap_or_else(|| "#live".into()),
                    shard_state_label(s.state),
                    s.n_xorbs,
                    humanize_bytes(s.size),
                ),
                state_style(shard_state_label(s.state)),
            )
        })
        .collect();
    let focused = app.pane == Pane::SideBottom;
    f.render_widget(
        Paragraph::new(lines).block(pane_block(format!(" shards ({}) ", c.shards.len()), focused)),
        area,
    );
}

fn draw_recent(f: &mut Frame, area: Rect, c: &UploadCommitDetail) {
    let last_file = c.completed_files.last().map(|(_, fl)| fl.name.clone());
    let last_xorb = c.xorbs.recent.last().map(|(_, x)| short_hash(&x.hash));
    let line = match (last_file, last_xorb) {
        (Some(fl), Some(x)) => format!(" recent: {fl} done · xorb {x} ↑"),
        (Some(fl), None) => format!(" recent: {fl} done"),
        (None, Some(x)) => format!(" recent: xorb {x} ↑"),
        (None, None) => " recent: —".to_string(),
    };
    f.render_widget(Paragraph::new(line).style(Style::default().fg(ratatui::style::Color::DarkGray)), area);
}
```

Add `pane_block` to `ui/widgets.rs` (shared with download page):

```rust
use ratatui::widgets::{Block, Borders};

/// Bordered pane; the focused pane gets a highlighted title so tab-cycling is visible.
pub fn pane_block(title: String, focused: bool) -> Block<'static> {
    let block = Block::default().borders(Borders::ALL).title(title);
    if focused {
        block.border_style(Style::default().fg(Color::Cyan))
    } else {
        block
    }
}
```

- [ ] **Step 4: Run tests + commit**

Run: `cargo test -p xet-console upload` (2 passed) and the full crate suite; clippy clean.

```bash
git add xet_console/src/ui/upload.rs xet_console/src/ui/widgets.rs
git commit -m "feat(xet-console): upload commit detail page (layout A)"
```

---

### Task 8: Download group detail page

**Files:**
- Modify: `xet_console/src/ui/download.rs` (replace placeholder)

Layout A mirror: files table left; term-blocks pane right-top; prefetch pane right-bottom.

- [ ] **Step 1: Write the failing render test**

```rust
#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use crate::app::{App, Page};
    use crate::fixtures::sample_snapshot;
    use crate::poll::PollState;
    use crate::ui;
    use crate::ui::widgets::buffer_text;

    #[test]
    fn download_page_shows_files_terms_and_prefetch() {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App { page: Page::Download, ..Default::default() };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("group #3"), "{text}");
        assert!(text.contains("out.bin"), "{text}");
        assert!(text.contains("reconstructing"), "{text}");
        assert!(text.contains("term blocks"), "{text}");
        assert!(text.contains("fetching"), "{text}");
        assert!(text.contains("consumed 5"), "{text}");
        assert!(text.contains("queue 2"), "{text}");
        assert!(text.contains("prefetch"), "{text}");
    }
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p xet-console download` fails on placeholder.

- [ ] **Step 3: Implement**

```rust
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};
use xet_runtime::console::model::{DownloadGroupDetail, DownloadGroupKind, SnapshotResponse};

use crate::app::{App, Pane};
use crate::ui::widgets::*;

fn selected_group<'a>(app: &App, snap: &'a SnapshotResponse) -> Option<&'a DownloadGroupDetail> {
    let s = snap.sessions.get(app.session_idx)?;
    let live = &s.download_group_details;
    if app.group_idx < live.len() {
        live.get(app.group_idx)
    } else {
        s.detail.ended_download_groups.get(app.group_idx - live.len())
    }
}

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse) {
    let Some(g) = selected_group(app, snap) else {
        f.render_widget(Paragraph::new("no such download group — esc to go back"), area);
        return;
    };

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(3)])
        .split(area);

    let (pct, rate) = match &g.progress {
        Some(p) => (percent(p.bytes_completed, p.total_bytes), humanize_rate(p.rate_bps)),
        None => (0, "–".to_string()),
    };
    let kind = match g.kind {
        DownloadGroupKind::Files => "files",
        DownloadGroupKind::Stream => "stream",
    };
    f.render_widget(
        Paragraph::new(format!(
            " ▼ group #{} [{}] kind {kind}  {pct}%  {rate}",
            g.id,
            group_state_label(g.state)
        ))
        .style(Style::default().add_modifier(Modifier::BOLD)),
        rows[0],
    );

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(rows[1]);
    draw_files(f, cols[0], app, g);

    let side = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(cols[1]);
    draw_terms(f, side[0], app, g);
    draw_prefetch(f, side[1], app, g);
}

fn draw_files(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let mut rows: Vec<Row> = Vec::new();
    for file in &g.files {
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(download_state_label(file.state)).style(state_style(download_state_label(file.state))),
            Cell::from(format!("{:>3}%", percent(file.bytes_completed, file.total_bytes.max(1)))),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
        ]));
    }
    for (_, file) in &g.completed_files {
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(download_state_label(file.state)).style(state_style(download_state_label(file.state))),
            Cell::from("100%"),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
        ]));
    }
    let n = rows.len();
    let mut tstate = TableState::default();
    if n > 0 {
        tstate.select(Some(app.main_row.min(n - 1)));
    }
    let table = Table::new(
        rows,
        [Constraint::Min(24), Constraint::Length(16), Constraint::Length(5), Constraint::Length(9)],
    )
    .header(Row::new(vec!["name", "state", "prog", "hash"]).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(pane_block(format!(" files ({}/{} active) ", g.file_counts.in_flight, n), app.pane == Pane::Main))
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    f.render_stateful_widget(table, area, &mut tstate);
}

/// Term blocks of the file selected in the files pane (in-flight files only;
/// completed files have no live blocks).
fn draw_terms(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let file = g.files.get(app.main_row.min(g.files.len().saturating_sub(1)));
    let mut lines: Vec<Line> = Vec::new();
    if let Some(file) = file {
        for b in &file.term_blocks {
            lines.push(Line::styled(
                format!(
                    "block {} [{}] {}–{} ({} terms)",
                    b.block_id,
                    term_state_label(b.state),
                    humanize_bytes(b.byte_range.0),
                    humanize_bytes(b.byte_range.1),
                    b.terms.len(),
                ),
                state_style(term_state_label(b.state)),
            ));
        }
        lines.push(Line::from(format!("consumed {}", file.consumed_blocks)));
    } else {
        lines.push(Line::from("no in-flight file selected"));
    }
    f.render_widget(
        Paragraph::new(lines).block(pane_block(" term blocks ".into(), app.pane == Pane::SideTop)),
        area,
    );
}

fn draw_prefetch(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let file = g.files.get(app.main_row.min(g.files.len().saturating_sub(1)));
    let lines: Vec<Line> = match file.and_then(|fl| fl.prefetch.as_ref()) {
        Some(p) => vec![
            Line::from(format!("queue {}", p.queue_depth)),
            Line::from(format!("prefetched @ {}", humanize_bytes(p.prefetched_byte_position))),
            Line::from(format!("active     @ {}", humanize_bytes(p.active_byte_position))),
            Line::from(format!("rate {}", humanize_rate(p.completion_rate_bps))),
        ],
        None => vec![Line::from("no prefetch state")],
    };
    f.render_widget(
        Paragraph::new(lines).block(pane_block(" prefetch ".into(), app.pane == Pane::SideBottom)),
        area,
    );
}
```

- [ ] **Step 4: Run tests + commit**

Run: `cargo test -p xet-console download` (1 passed) + full crate; clippy clean.

```bash
git add xet_console/src/ui/download.rs
git commit -m "feat(xet-console): download group detail page"
```

---

### Task 9: Concurrency monitors page

**Files:**
- Modify: `xet_console/src/ui/concurrency.rs` (replace placeholder)

- [ ] **Step 1: Write the failing render test**

```rust
#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use crate::app::{App, Page};
    use crate::fixtures::sample_snapshot;
    use crate::poll::PollState;
    use crate::ui;
    use crate::ui::widgets::buffer_text;

    #[test]
    fn concurrency_page_shows_monitor_models_and_history() {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App { page: Page::Concurrency, ..Default::default() };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("upload"), "{text}");
        assert!(text.contains("permits 14/16"), "{text}");
        assert!(text.contains("bounds 1..64"), "{text}");
        assert!(text.contains("success 0.94"), "{text}");
        assert!(text.contains("increase"), "recommendation shown: {text}");
        assert!(text.contains("rtt 412.5ms"), "{text}");
        assert!(text.contains("limit history"), "{text}");
    }
}
```

- [ ] **Step 2: Run to verify failure** — fails on the placeholder.

- [ ] **Step 3: Implement**

```rust
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Gauge, Paragraph, Sparkline};
use xet_runtime::console::model::{AdjustmentRecommendation, MonitorSnapshot, SnapshotResponse};

use crate::app::App;
use crate::ui::widgets::*;

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse) {
    let Some(session) = snap.sessions.get(app.session_idx) else {
        f.render_widget(Paragraph::new("no session selected — esc to go back"), area);
        return;
    };
    let monitors = &session.detail.monitors;
    if monitors.is_empty() {
        f.render_widget(
            Paragraph::new("no live concurrency monitors (they exist only while a commit/group's client is alive)"),
            area,
        );
        return;
    }
    // One fixed-height card per monitor, scrolled by monitor_row.
    const CARD: u16 = 7;
    let visible = (area.height / CARD).max(1) as usize;
    let first = app.monitor_row.min(monitors.len().saturating_sub(1)).saturating_sub(visible.saturating_sub(1));
    let mut y = area.y;
    for m in monitors.iter().skip(first).take(visible) {
        let card = Rect { x: area.x, y, width: area.width, height: CARD.min(area.y + area.height - y) };
        draw_monitor(f, card, m);
        y += CARD;
        if y >= area.y + area.height {
            break;
        }
    }
}

fn draw_monitor(f: &mut Frame, area: Rect, m: &MonitorSnapshot) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // title
            Constraint::Length(1), // permits gauge
            Constraint::Length(1), // success model
            Constraint::Length(1), // latency model
            Constraint::Length(1), // history label
            Constraint::Length(2), // sparkline
        ])
        .split(area);

    f.render_widget(
        Paragraph::new(format!(
            " {} — permits {}/{} ({} free) — bounds {}..{} — adj {} — sent {}",
            m.tag,
            m.active_permits,
            m.total_permits,
            m.available_permits,
            m.bounds.min,
            m.bounds.max,
            if m.adjustment_enabled { "auto" } else { "fixed" },
            humanize_bytes(m.bytes_sent),
        ))
        .style(Style::default().add_modifier(Modifier::BOLD)),
        rows[0],
    );

    let ratio = if m.total_permits == 0 { 0.0 } else { m.active_permits as f64 / m.total_permits as f64 };
    f.render_widget(
        Gauge::default().ratio(ratio.clamp(0.0, 1.0)).gauge_style(Style::default().fg(Color::Cyan)),
        rows[1],
    );

    let success = match &m.success {
        Some(s) => format!(
            " success {:.2} (↑>{:.2} ↓<{:.2}) rec {}",
            s.success_ratio,
            s.thresholds.increase,
            s.thresholds.decrease,
            match s.recommended_adjustment {
                AdjustmentRecommendation::Increase => "increase",
                AdjustmentRecommendation::Hold => "hold",
                AdjustmentRecommendation::Decrease => "decrease",
            }
        ),
        None => " success: no samples yet".to_string(),
    };
    f.render_widget(Paragraph::new(success), rows[2]);

    let latency = match &m.latency {
        Some(l) => format!(
            " rtt {}ms ±{} · bw {}",
            l.predicted_max_rtt_ms.map(|v| format!("{v:.1}")).unwrap_or_else(|| "–".into()),
            l.rtt_standard_error_ms.map(|v| format!("{v:.0}")).unwrap_or_else(|| "–".into()),
            humanize_rate(l.predicted_bandwidth_bps),
        ),
        None => " latency: no samples yet".to_string(),
    };
    f.render_widget(Paragraph::new(latency), rows[3]);

    f.render_widget(
        Paragraph::new(Line::styled(
            format!(" limit history ({} adjustments)", m.limit_history.len()),
            Style::default().fg(Color::DarkGray),
        )),
        rows[4],
    );
    let data: Vec<u64> = m.limit_history.iter().map(|(_, v)| *v as u64).collect();
    f.render_widget(Sparkline::default().data(&data).style(Style::default().fg(Color::Green)), rows[5]);
}
```

- [ ] **Step 4: Run tests + commit**

Run: `cargo test -p xet-console concurrency` (1 passed) + full crate; clippy clean.

```bash
git add xet_console/src/ui/concurrency.rs
git commit -m "feat(xet-console): concurrency monitors page with permit gauges and limit sparkline"
```

---

### Task 10: Live end-to-end smoke test

A headless test that drives the REAL pipeline: in-process console server → fabricated transfer state → blocking client → poller → App → ratatui render. This is the TUI's acceptance test.

**Files:**
- Create: `xet_console/src/smoke.rs` (`#[cfg(test)]` module)
- Modify: `xet_console/src/main.rs` (add `#[cfg(test)] mod smoke;`)

- [ ] **Step 1: Write the test** (acceptance test — write it, run it, fix what it exposes)

```rust
#![cfg(test)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossterm::event::KeyCode;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use serial_test::serial;
use xet_runtime::console::model::FileUploadState;
use xet_runtime::console::registry::registry;
use xet_runtime::console::server;
use xet_runtime::console::state::UploadCommitConsole;

use crate::app::{App, overview_entries};
use crate::client::ConsoleClient;
use crate::poll::{Shared, spawn_poller};
use crate::ui;
use crate::ui::widgets::buffer_text;

#[test]
#[serial]
fn end_to_end_render_from_live_server() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    server::ensure_started();
    let addr = server::bound_addr().expect("server bound");

    // Fabricate a session with one commit holding one chunking file.
    let handle = registry().register_session("tui-smoke".into(), vec![]);
    let commit = UploadCommitConsole::new(Some(&handle.scope), Some("local://smoke".into()));
    let file = commit.new_file(1, "smoke.bin", Some(1024));
    file.set_state(FileUploadState::Chunking);
    file.add_chunked_bytes(512, 4);

    // Real client + real poller.
    let shared = Arc::new(Shared::default());
    let shutdown = Arc::new(AtomicBool::new(false));
    let poller = spawn_poller(
        ConsoleClient::new(format!("http://{addr}")),
        shared.clone(),
        Duration::from_millis(50),
        shutdown.clone(),
    );
    for _ in 0..40 {
        if shared.lock().snapshot.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Drive the App like the run loop does: drill into the commit and render.
    let mut app = App::default();
    {
        let state = shared.lock();
        let snap = state.snapshot.as_ref().expect("poller populated");
        let entries = overview_entries(snap);
        // Row 0 is our session; row 1 is the commit (this process may carry
        // sessions from sibling #[serial] tests — find OUR commit row).
        let row = entries
            .iter()
            .position(|e| {
                matches!(e, crate::app::OverviewEntry::Commit { session_idx, .. }
                    if snap.sessions[*session_idx].detail.id == "tui-smoke")
            })
            .expect("our commit row exists");
        app.overview_row = row;
        app.handle_key(KeyCode::Enter, &entries);
    }

    let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
    {
        let state = shared.lock();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
    }
    let text = buffer_text(terminal.backend());
    assert!(text.contains("smoke.bin"), "{text}");
    assert!(text.contains("chunking"), "{text}");
    assert!(text.contains("50%"), "512/1024 chunked: {text}");

    shutdown.store(true, Ordering::Relaxed);
    poller.join().unwrap();
    drop(file);
    drop(commit);
}
```

Add `#[cfg(test)] mod smoke;` to `main.rs`'s module list.

- [ ] **Step 2: Run until green**

Run: `cargo test -p xet-console smoke` — PASS (fix whatever integration gaps it exposes — this is the task where seams show up; report any fixes).

- [ ] **Step 3: Full crate verification**

Run: `cargo test -p xet-console` (all tests), `cargo clippy -p xet-console -- -D warnings`, `cargo build -p xet-console`.

- [ ] **Step 4: Commit**

```bash
git add xet_console/src/smoke.rs xet_console/src/main.rs
git commit -m "test(xet-console): live end-to-end smoke through server, poller, app, and render"
```

---

### Task 11: CI, docs, manual smoke, final sweep

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/skills/xet-console/SKILL.md`
- Modify: `README.md`

- [ ] **Step 1: CI**

In `.github/workflows/ci.yml`, extend the two console steps added previously:

- In the `Lint (console feature)` step, add a second line: `cargo clippy -r --verbose -p xet-console -- -D warnings`
- In the `Build and Test (console feature)` step, add a second line: `cargo test --verbose --no-fail-fast -p xet-console`

(Separate lines, not merged into the existing `-p` lists — `xet-console` has no `console`/`simulation` features of its own.) Validate locally by running both new commands.

- [ ] **Step 2: Docs**

`docs/skills/xet-console/SKILL.md` — add a short section after the Connect section:

```markdown
## Human TUI

A terminal client renders the same API: `cargo run -p xet-console` (defaults to
127.0.0.1:6660; pass a port, host:port, or URL — `cargo run -p xet-console -- 7001`;
`--interval 250ms` adjusts the poll rate). Pages: [1] overview, [2] upload commit,
[3] download group, [4] concurrency; `?` shows keys.
```

`README.md` — in the "Debugging live transfers: xet-console" section, append the sentence: `A terminal UI ships in-repo: `cargo run -p xet-console` (see docs/skills/xet-console/SKILL.md).`

- [ ] **Step 3: Manual smoke (recommended; report what you see)**

Shell 1: `XET_CONSOLE_PORT=6660 cargo test -p hf-xet --features "console simulation" --test test_console full_transfer -- --nocapture --test-threads 1` runs too fast to watch; better: write a 20-line throwaway script under /tmp (NOT committed) that builds a session via `xet_pkg` and sleeps mid-upload, or simply run the TUI against the test from shell 1 and confirm it connects and renders the ended state before the process exits. Minimum bar: `cargo run -p xet-console -- 6660` against a console-enabled process shows the overview, and against nothing shows the DISCONNECTED banner without crashing or corrupting the terminal (q restores the shell cleanly).

- [ ] **Step 4: Final sweep**

```bash
cargo +nightly fmt
cargo clippy -p xet-console -- -D warnings
cargo test -p xet-console
cargo test -p xet-runtime -p xet-client --features console
cargo build   # workspace default, feature-off world untouched
```

All green. If fmt touched files, include them.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml docs/skills/xet-console/SKILL.md README.md
git commit -m "ci+docs(xet-console): CI coverage, skill and README pointers for the TUI"
```

---

## Deferred (do not build here)

- Per-item stream-group rendering (the API doesn't carry per-item stream state in v1).
- Scrolling/virtualization beyond what's specified; mouse support; configurable themes; `--scan` (strict-port design has no discovery).
- SSE; the TUI polls.

## Judgment calls an executor may hit

- **ratatui API drift**: this plan targets the 0.29-era API. If `cargo add` resolves a newer major, follow the compiler (`f.area()` vs `f.size()`, `Table::new(rows, widths)` argument shapes, `row_highlight_style` vs `highlight_style`, buffer indexing in the test helper). Keep semantics identical.
- **Unicode in assertions**: if a glyph (▲/▼/⏎/…) renders as multiple cells and breaks a `contains` assertion, assert on the adjacent ASCII text instead — don't fight the terminal model.
- **Test parallelism**: every test that touches the global server/registry is `#[serial]`; pure render tests (fixture-based) are not and must not be.




