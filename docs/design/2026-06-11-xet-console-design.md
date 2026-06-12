# xet-console — design

**Date:** 2026-06-11
**Status:** approved design, pre-implementation
**Branch:** `assaf/xet-console`

## Problem

Debugging xet-core internals during a live transfer means logs and print statements today. There is no way to look *inside* a running `XetSession` — which files are chunking, which terms are enqueued versus in flight, how dedup is performing mid-commit, what the adaptive concurrency controllers believe about the network — while it is happening.

**Goal:** a compile-time-gated observability portal. When `hf-xet` (`xet_pkg`) is built with the `console` feature, each process exposes a read-only HTTP+JSON view of every `XetSession` in it: upload commits (files, chunking progress, dedup, xorbs, shards), download groups (files, reconstruction terms, prefetch state), and adaptive concurrency monitors — consumed by a human through a terminal UI and by AI agents through the same API plus a skill document. Maximal information first; noise reduction can come later.

**Non-goals (v1):** production/end-user builds (this is a dev tool behind a feature flag), write/control operations (read-only), remote access (loopback only), persistence of history across process exit, SSE/streaming, a web UI, an MCP server.

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Human client | Terminal UI (ratatui), new workspace bin crate `xet_console` | Debugging happens in terminals, often over SSH; no port-forwarding friction. Tauri/webapp rejected for v1; the API leaves a web UI possible later. |
| Agent interface | Curl-able HTTP+JSON API + a skill document; no MCP server | Agents are fluent in curl+jq; an MCP binary would duplicate that with more maintenance. |
| Transport | HTTP+JSON via axum (already a workspace dep) | gRPC (tonic/prost) adds heavy deps and agents/browsers can't consume it easily. |
| Data acquisition | **Live-state registry** (approach A): handlers snapshot real component state on demand; small inline console-state structs where state doesn't exist today | No parallel model to drift from reality; ~zero cost with no client attached; about half the model already exists as readable state. Event-stream aggregator (tokio-console style) rejected: double bookkeeping, ~2× instrumentation code. tracing-Layer scraping rejected: needed data isn't in spans; brittle extraction. |
| History | Current state + bounded in-memory history rings | A client attaching mid-run sees how it got here (concurrency limit changes, throughput, recent completions) without an event-log subsystem. |
| Topology | One server per process; sessions namespaced under `/api/v1/sessions/{sid}` | A process can hold many `XetSession`s (pytest constantly does). One port, sessions come and go underneath. |
| Port | **Fixed port 6660, strict.** `XET_CONSOLE_PORT` overrides. If the bind fails, warn and run without a console — no fallback scan. | Predictable for humans and agents ("it's on 6660"). Nothing is written to disk for discovery. |
| Disk | The console writes nothing to disk. If it ever needs a directory, it uses `~/.cache/huggingface/xet/console/`. | — |
| Skill location | Canonical at `docs/skills/xet-console/SKILL.md`; symlink `.claude/skills/xet-console` → it; README pointer | Non-Claude agents and humans find it in docs/; Claude Code auto-loads it via the symlink. |
| TUI detail layout | Layout A: master files table left, xorbs/shards panels stacked right | Densest single-screen view; chosen over full-width table + bottom strip. |

## Architecture

### Crate and feature layout

Everything console-related roots in a new module **`xet_runtime::console`**:

- **`model`** — the wire types: plain serde structs that are simultaneously the registry's snapshot output, the server's response bodies, and the TUI's deserialization target. The API contract lives in one place.
- **`registry`** — the process-global `ConsoleRegistry` (see below).
- **`server`** — the axum app. `axum` becomes an *optional* dependency of `xet_runtime` only.

Rationale: every crate already depends on `xet_runtime` and threads `XetContext`, so instrumentation in `xet_client`/`xet_data`/`xet_pkg` reaches the registry without new dependency edges. Hosting the server here (rather than `xet_pkg`) also lets `git_xet` enable the console later.

Feature chain, mirroring the existing `python` and `tokio-console` chains:

```
xet_pkg/console → xet-data/console → xet-client/console → xet-runtime/console (pulls axum)
```

With the feature **off**, instrumented components hold a zero-sized `ConsoleHandle<T>` whose methods are no-ops — call sites stay clean (`self.console.update(...)`) and compile to nothing. With the feature **on**, the handle wraps an `Arc` of the component's console-state struct, registered with the registry.

Dev usage: `maturin develop --features console` (wheel builds pass the feature through to `xet_pkg`).

### ConsoleRegistry

A process-global registry (`OnceLock` in `xet_runtime::console`):

- **Live entities** are held as **weak references** — the registry never extends lifetimes and never leaks. Hierarchy: sessions → upload commits / download groups → per-entity console state (files, xorbs, shards, terms) and concurrency monitors.
- **Completed entities**: when a commit/group/file finishes or aborts, the owning component finalizes an owned **summary** (final state + key metrics) into the registry. Retention is bounded: last **64** completed commits/groups per session, last **16** ended sessions. A transfer that finished a moment ago is still inspectable while the process lives.
- **Aborts** are captured via cancel/drop hooks (`TaskRuntime` cancellation, `Drop` impls) writing a final `aborted` summary — an interrupted pytest run remains inspectable.

### Server lifecycle

- The first console-enabled `XetSession` lazily starts the process-global server.
- The server runs on a **dedicated thread with its own current-thread tokio runtime** — deliberately not on any session's runtime, because owned runtimes die with their session while the server must outlive every individual session. It lives until process exit.
- Binds `127.0.0.1:6660` (or `XET_CONSOLE_PORT`; the value `0` binds an ephemeral port — for tests). "Strict" means no automatic fallback scan, not that the port isn't configurable. Bind failure (port taken, etc.): `tracing::warn!` once; the process runs without a console. **v1 refuses non-loopback binds.**
- On success: `tracing::info!` with the URL.

### The one failure rule

**The console must never break the transfer.** Every console-side failure — bind failure, poisoned lock, weak reference that dangles mid-snapshot — degrades to a `tracing::warn` and a partial or absent console view. No console error ever surfaces on the host data path. Overhead budget: feature off → compiled out; feature on with no client attached → atomic counter updates and short leaf locks only.

## Data model

### Entity tree

```
Process ─ pid, argv, start_time, hf-xet version, runtime kind (owned/external), worker threads
└─ Session ×N ─ uuid, created_at, active|ended, config snapshot (endpoint, concurrency settings)
   ├─ ConcurrencyMonitor ×N (tag: xorb-upload / term-download / shard …)
   │     permits {total, active, available}, bounds, adjustment enabled, bytes_sent
   │     success model {ratio, thresholds, recommendation −1/0/+1}
   │     latency model {predicted max RTT ± stderr, predicted bandwidth}
   │     history ring: (t, limit) per adjustment + periodic in-flight samples
   ├─ UploadCommit ×N ─ id, state, created_at, group progress + rates, live dedup totals
   │  ├─ File ×N ──── name, size, state, bytes_chunked, chunks, file_hash?, sha256?,
   │  │               dedup {total, deduped, new, via_global_dedup, defrag_prevented},
   │  │               xorb_deps [{xorb, bytes, uploaded}], shard_uploaded
   │  ├─ Xorb ×N ──── hash, size, n_chunks, state, bytes_transferred, files_contained, timings
   │  └─ Shard ×N ─── state, n_xorbs, n_files, size, hash?, timings
   └─ DownloadGroup ×N (file group | stream group) ─ id, kind, state, group progress + rates
      └─ File ×N ──── name, file_hash, range?, size, bytes_done, transfer_bytes, state, rate
         └─ Term ×N ─ xorb hash, chunk range, byte range, size, state, timings
                      + per-file prefetch: queue depth, prefetched_pos, active_pos, rate EWMA
```

### Lifecycle state machines

**Upload file:** `queued` (registered, waiting on `file_ingestion_semaphore`) → `chunking` (bytes/size, chunk count, live dedup counters) → `processed` (**file hash now known**) → `awaiting_xorbs` (chunks live in xorbs not yet uploaded) → `awaiting_shard` (xorbs done; metadata shard pending) → `complete` | `failed` | `aborted`.

**Xorb:** `formed` (DataAggregator cut) → `queued` (serialized, waiting upload permit) → `uploading` (bytes_transferred via the existing `ProgressCallback`, teed) → `uploaded` | `failed`.

**Shard:** `staging` (in-memory, accumulating xorb metadata) → `flushed` (staged to disk) → `uploading` → `uploaded`. A file's `shard_uploaded` flag flips when the shard holding its metadata reaches `uploaded` — typically at commit finalize, earlier if a staged shard was flushed and uploaded mid-session.

**Download file:** `queued` (waiting `file_download_semaphore`) → `reconstructing` → `complete` | `failed` | `aborted`.

**Term:** `enqueued` (in `ReconstructionTermManager`'s prefetch queue) → `fetching` (CAS range request in flight, permit held) → `fetched` (data ready, not yet consumed) → `consumed`.

### What exists vs. what's new

Already readable, snapshot directly:

- `GroupProgress` / `ItemProgress` atomics — bytes, transfer bytes, rates (`xet_data/src/progress_tracking/progress_types.rs`)
- `CompletionTracker` — file↔xorb dependency graph (`xet_data/src/progress_tracking/`)
- `DeduplicationMetrics` — per-file and aggregate
- `AdaptiveConcurrencyController` — permit counts (`total/active/available_permits()`), `CCSuccessModelState`, `CCLatencyModelState` (`xet_client/src/cas_client/adaptive_concurrency/controller.rs`)
- `ReconstructionTermManager` — byte positions, completion-rate EWMA (`xet_data/src/file_reconstruction/reconstruction_terms/manager.rs`)

New instrumentation, concentrated in four places:

1. **Per-term states** — hooked in `ReconstructionTermManager` (enqueue/spawn/fetch-complete/consume).
2. **Per-xorb states** — hooked at xorb task spawn, permit acquisition, the existing `ProgressCallback`, and completion (`FileUploadSession`).
3. **Per-shard states** — hooked in `SessionShardInterface` (staging/flush/upload).
4. **Per-file upload pipeline stage** — hooked in `SingleFileCleaner` (chunking progress, dedup counters, hash at finalize) and `CompletionTracker` (xorb/shard gating).

Plus: session/commit/group registration in `xet_pkg` (`XetSession`, `XetUploadCommit`, `FileDownloadGroup`, `DownloadStreamGroup`) and monitor registration in `AdaptiveConcurrencyController::new*`.

### Memory bounding rule

Live maps hold **in-flight items only** — naturally bounded by concurrency limits. Completions fold into cumulative counters plus a bounded **recent-completions ring (256 entries** with timestamps**)** per commit/group, which feeds "recently finished" panes and throughput history. Console state never grows with file count or file size.

### Conventions

- Entity IDs reuse the existing `UniqueId` u64s; session IDs are the existing UUIDs.
- Hashes travel as full hex strings; clients truncate for display.
- Timestamps are epoch milliseconds, stamped server-side.
- Enums serialize as snake_case strings (`"awaiting_xorbs"`, `"fetching"`).

## HTTP API

All endpoints are `GET`, all responses JSON, versioned under `/api/v1`, additive evolution only.

```
/                                        self-describing index: service, version, endpoint list
/api/v1/process                          pid, argv, start_time, runtime kind, worker threads
/api/v1/sessions                         summaries: active + retained ended sessions
/api/v1/sessions/{sid}                   config snapshot, monitor/commit/group summaries
/api/v1/sessions/{sid}/uploads           commit summaries
/api/v1/sessions/{sid}/uploads/{cid}     full commit: progress, dedup, files, xorbs, shards
/api/v1/sessions/{sid}/downloads         group summaries
/api/v1/sessions/{sid}/downloads/{gid}   full group: progress, files, per-file term state
/api/v1/sessions/{sid}/concurrency       monitors with full history rings
/api/v1/snapshot                         everything in one call (agent one-shot; ?session= filter)
```

- **Payload bounding mirrors the memory rule:** detail endpoints return in-flight items + the recent ring + cumulative counters by default; `?files=all` returns the complete file list.
- Every response carries top-level `as_of` (epoch ms). Errors: `{"error": "..."}` with appropriate status codes.
- Clients poll (TUI default 500 ms). No SSE in v1; an `/api/v1/events` SSE stream and CORS headers (for a future web UI) are noted as compatible later additions.
- `GET /` includes pid/argv/start_time so a client can confirm it reached the process it expected.

Example (abridged `uploads/{cid}`):

```json
{
  "as_of": 1781221200123,
  "id": 7, "state": "active", "created_at": 1781221190000,
  "progress": {"total_bytes": 8589934592, "bytes_completed": 3221225472,
               "rate_bps": 412000000, "transfer_bytes_completed": 901775360},
  "dedup":    {"total": 3221225472, "deduped": 2319450112, "new": 901775360,
               "via_global_dedup": 524288000, "defrag_prevented": 1048576},
  "files": [
    {"id": 12, "name": "model-00001.safetensors", "state": "chunking",
     "size": 4831838208, "bytes_chunked": 1073741824, "chunks": 16384,
     "file_hash": null, "xorb_deps": [], "shard_uploaded": false}
  ],
  "xorbs": {
    "in_flight": [{"hash": "f3ab…", "state": "uploading", "size": 67108864,
                   "bytes_transferred": 33554432, "files": [12]}],
    "counts": {"formed": 48, "uploaded": 46, "failed": 0},
    "recent": [{"hash": "9c01…", "completed_at": 1781221199847}]
  },
  "shards": [{"state": "staging", "n_xorbs": 48, "n_files": 3, "size": 262144}]
}
```

## TUI — `xet-console`

New workspace bin crate **`xet_console`** (binary `xet-console`): ratatui + crossterm, reqwest for polling, deserializes `xet_runtime::console::model` types directly.

- **Connection:** defaults to `http://127.0.0.1:6660`; `--port`/URL argument overrides; `--interval` sets the poll rate (default 500 ms).
- **Pages:** `[1]` session overview (sessions, commits, groups, monitor one-liners; landing page) · `[2]` upload commit detail · `[3]` download group detail · `[4]` concurrency monitors (gauges, model states, limit-history sparklines).
- **Navigation** (tokio-console-style): `↑↓`/`jk` select · `⏎` drill into selection · `esc` back · `tab` cycle pane focus · `p` pause refresh · `?` help · `q` quit.
- **Detail layout (chosen: A):** progress + dedup header strip; files table on the left (~60%, with state and progress bar per file); xorbs and shards panels stacked on the right; a recent-events line above the key bar. The download page mirrors it: terms panel (enqueued/fetching/fetched + queue depth and prefetch positions) where the xorbs panel sits.
- Finished/aborted commits, groups, and ended sessions render from retained summaries, visually distinguished.

## Agent skill

Canonical: **`docs/skills/xet-console/SKILL.md`** (markdown + YAML frontmatter, readable by any agent). Symlink **`.claude/skills/xet-console` → `../../docs/skills/xet-console`** for Claude Code auto-loading. A pointer goes in the README's development section.

Content:

1. **Connect:** `curl -s localhost:6660/` — if that's not the expected process (check pid/argv), ask the user for the port (`XET_CONSOLE_PORT`).
2. **First call:** `/api/v1/snapshot` for the whole world, then scoped endpoints.
3. **Recipes:** curl+jq one-liners per question — what's uploading right now, is dedup pulling its weight, which files are stuck and in what state, what are the monitors doing.
4. **Symptom → diagnosis guidance:** success ratio < 0.5 → server pushing back / network trouble; terms piling in `enqueued` with zero free permits → download is concurrency-bound; files parked in `awaiting_shard` → finalize hasn't run; low dedup with high `new` bytes → content genuinely novel (or chunking mismatch).

## Testing

- **Unit:** registry lifecycle (register/unregister, weak-ref cleanup, bounded retention), ring buffers, model serde round-trips, state-transition helpers.
- **Integration** (behind `console`): scripted upload/download against the existing local/mock CAS test infrastructure while polling the real axum server; assert state progressions appear (file `queued→chunking→…→complete`, xorb/term states, monitors present, finished commit retained). Tests bind **port 0** (ephemeral, via `XET_CONSOLE_PORT=0`) to avoid 6660 collisions in CI.
- **TUI:** ratatui `TestBackend` snapshot tests rendered from fixture JSON; no live server.
- **CI:** a `--features console` build + clippy + test job so the feature can't rot.
- **Manual smoke:** `maturin develop --features console`, run a real transfer, attach `xet-console`.

## Future work (explicitly out of v1)

- SSE `/api/v1/events` stream; CORS for a browser client; a web UI on the same API.
- MCP wrapper if agents prove to struggle with raw HTTP (API is already shaped for it).
- OpenAPI document under `openapi/` (the self-describing `/` + skill suffice for v1).
- Per-request CAS HTTP tracing (request log ring per monitor).
- Noise reduction / curated views once the firehose proves itself.
