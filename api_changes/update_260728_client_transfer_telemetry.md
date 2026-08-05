# Client transfer telemetry

**Date**: 2026-07-28
**Crates**: `xet-runtime` (new `telemetry` config group), `xet-client`
(`cas_client::telemetry`, `Client` trait), `xet-data` (`telemetry` module, session hooks)

## What changed

The client now reports one performance summary per transfer to `POST /v1/telemetry` on the CAS
server, where a companion change adds the endpoint. Reporting is best-effort: it is never retried,
never surfaces an error, and never blocks data movement.

### Wire format: the envelope is entirely snake_case

The body carries exactly five keys — `time`, `event`, `session_id`, `user_agent`, `metrics` — and
**every one is snake_case**, matching the naming convention across the whole document (envelope,
`metrics`, and the fields the server stamps itself).

`user_agent` was previously emitted as camelCase `userAgent`. The server accepts that spelling as a
serde alias for older clients, so both work, but a body carrying **both** is rejected as a
duplicate-field 400 — so exactly one must be sent, and it is now the snake_case one. A test asserts
no envelope key contains a capital letter.

### New config group: `telemetry`

| Field | Env var | Default |
|---|---|---|
| `enabled` | `HF_XET_TELEMETRY_ENABLED` | `true` |
| `heartbeat_after` | `HF_XET_TELEMETRY_HEARTBEAT_AFTER` | `300s` |
| `heartbeat_interval` | `HF_XET_TELEMETRY_HEARTBEAT_INTERVAL` | `300s` |
| `request_timeout` | `HF_XET_TELEMETRY_REQUEST_TIMEOUT` | `5s` |
| `final_flush_timeout` | `HF_XET_TELEMETRY_FINAL_FLUSH_TIMEOUT` | `2s` |
| `max_in_flight` | `HF_XET_TELEMETRY_MAX_IN_FLIGHT` | `32` |

`max_in_flight` is a **process-wide** ceiling: one counter is shared by every sink, so the total
number of in-flight telemetry POSTs is bounded no matter how many transfers run at once. A per-sink
counter would have bounded each transfer separately and multiplied by the transfer count, which puts
the backpressure in the wrong place — a snapshot download fans out into many concurrent per-file
transfers, so the heaviest telemetry moment would have been the one with no aggregate limit. The
default is sized as a process-wide number accordingly: a transfer emits one terminal document and
heartbeats only begin after `heartbeat_after`, so the realistic burst is a set of concurrent
transfers finalizing together.

`HF_HUB_DISABLE_TELEMETRY` and `HF_HUB_OFFLINE` also force it off, and win over
`HF_XET_TELEMETRY_ENABLED=1`. These are applied at the end of `XetConfig::with_env_overrides`
rather than through `ENVIRONMENT_NAME_ALIASES`, because that table maps names with identical
polarity and these are inverted.

### `Client` trait gained a method — **with a default body**

```rust
#[cfg(not(target_family = "wasm"))]
fn transfer_telemetry(&self) -> Option<Arc<TransferTelemetry>> { None }
```

**No existing implementor needs to change.** Only `RemoteClient` overrides it; the local,
in-memory, and simulation clients inherit `None` and report nothing.

Note the failure mode this creates: an override with a mistyped signature compiles cleanly and is
silently never called. `xet_data/tests/test_transfer_telemetry.rs` exists to catch that and should
not be deleted.

There is a second, subtler failure mode: the emit machinery can work perfectly while no production
caller ever reaches it. Tests that drive `FileDownloadSession` directly cannot see that, because
they call `finalize()` themselves. `xet_pkg/tests/test_download_telemetry.rs` exists to close the
gap — it goes through `XetFileDownloadGroup::finish_blocking()` and asserts on what the server
received. Keep new coverage at that altitude; a test that calls `finalize()` itself re-opens it.

### Session behavior

- `FileUploadSession::finalize_impl` now delegates to a new private `finalize_inner` and reports on
  both the success and error paths. Public signatures are unchanged.
- `FileDownloadSession::finalize` likewise, and reports a successful transfer. It keeps the debug
  assertion that every item completed, so it must only be used on a clean-completion path.
- **New:** `FileDownloadSession::finalize_with(outcome, error_class)` finalizes while reporting an
  explicit outcome, and makes no completeness claim. Use it for a session that ended badly, and for
  one whose notion of "complete" belongs to the caller. `finalize` alone can only ever report `ok`,
  so a download that failed has to go through this or the failure-rate signal is always zero.
- **Download groups now finalize their session.** `XetFileDownloadGroup::finish`/`finish_blocking`
  and the legacy `data_client::download_async` finalize on both the success and error paths, the
  latter classified via the new `XetError::telemetry_class()`. Previously nothing called
  `FileDownloadSession::finalize`, so downloads through the Python bindings reported nothing at all.
- **`XetError` gained two variants: `RateLimited(String)` and `ServerError(String)`**, for HTTP 429
  and 5xx respectively. Additive, and the enum is already `#[non_exhaustive]`.
  - **Python-visible behavior is unchanged**: both map to `PyConnectionError`, exactly as
    `XetError::Network` did before. Only the message prefix differs (`Rate limited:` /
    `Server error:` instead of `Network error:`).
  - They exist because HTTP status did not survive the flattening into `XetError`. Every HTTP failure
    became `Network`, so `telemetry_class()` could never return `rate_limited` or `server_error`,
    while the upload path — which classifies from `DataError` and inspects `reqwest::Error::status()`
    — reported both. A 429 therefore meant two different things depending on direction, defeating the
    point of a shared `error_class` vocabulary.
  - One gap is deliberately left: a 404 arriving as a `reqwest` status still classifies as `network`,
    not `not_found`. Routing it correctly would change the Python exception type callers catch, which
    is a user-visible change rather than a telemetry fix. Pinned by a test so it stays a decision.
- **New:** `XetDownloadStreamGroup::finish`/`finish_blocking` (and `finish()` plus context-manager
  support on the Python class). Streams are consumed independently, so the group cannot detect
  completion itself, and `finish` is how a caller states it explicitly. **Purely additive** - a
  group that is never finished behaves exactly as before and still reports, so no existing caller
  has to change. Note that `finish` closes the group: streams already handed out stay usable, but
  opening a new one afterwards is an error.
- **Both sessions gained a `Drop` impl**, emitting a terminal summary when the session was never
  finalized — the safety net for callers that abandon a session. It is deliberately *not* gated on
  an ambient tokio runtime: the send is spawned on the `XetRuntime`'s own stored handle, so
  requiring `Handle::try_current()` only served to disable the path for embedders that release the
  last `Arc` from a foreign thread, which is exactly what the Python bindings do.
  - An abandoned **upload** reports `aborted`: the commit never happened, so nothing was durably
    transferred.
  - An abandoned **download** reports `ok` or `dropped` depending on what actually transferred, via
    the new `GroupProgress::all_items_complete()`. Reaching `Drop` only means nobody called
    `finalize()`, which for downloads is the common case rather than a failure — `finish` is new and
    existing embedders have not adopted it. Reporting all of those as `dropped` would make the
    outcome field mean "probably fine" and leave a failure-rate dashboard nothing to measure, so
    `dropped` is reserved for a genuinely incomplete transfer.
  - `all_items_complete()` requires every item to have a **finalized size** as well as all of its
    bytes delivered, and requires at least one item. The size check is not redundant: for an
    open-ended stream range the size is discovered incrementally, so `bytes_completed` can equal
    `total_bytes` mid-transfer whenever the consumer catches up to the prefetch frontier. A byte
    comparison alone would report an abandoned stream as a success.
- New public accessors: `FileDownloadSession::client()`, and
  `TestEnvironment::telemetry_docs()` under the `simulation` feature.
- New public helpers: `xet_data::telemetry::{classify_error, outcome_for_class}`, for callers that
  need to produce an `(outcome, error_class)` pair themselves.

### Simulation server

`POST /v1/telemetry` is now routed by the local test server, and received documents are readable
via `LocalServer::telemetry_docs()` / `LocalTestServer::telemetry_docs()`.

### New dependencies

`chrono` and `uuid` were added to `xet-client`, gated to non-wasm targets.

## Why

We had no client-side view of upload/download performance, so a throughput regression shipped in
an `hf-xet` release was undetectable. Server-side metrics cover CAS request latency but not
end-to-end client throughput, dedup effectiveness, or where a transfer's wall time goes.

## Notes for downstream agents

- **The metric key set is a contract.** `xet_data/src/telemetry/payload.rs` is the source of truth.
  Adding a key is safe; changing an existing key's JSON type is not — consumers assign a field
  type on first sight and cannot change it in place, so a type change breaks ingestion for every
  document carrying it. `test_upload_key_set_is_exact` and `test_numeric_types_stable` enforce this.
- **All `f64` values must go through the finite guard** in that module. `serde_json` renders NaN
  and infinity as `null`, and a single such document poisons the field's type for a consumer.
- **No PII.** The payload carries no file names, paths, hashes, repository ids, or user ids; the
  server derives identity from the request's JWT.
- A follow-up PR adds the generated schema (`telemetry/metrics.schema.json`) and a CI
  compatibility gate. Until then the const key lists in the tests are the only thing pinning the
  contract.
