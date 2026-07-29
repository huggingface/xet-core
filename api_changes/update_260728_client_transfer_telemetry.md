# Client transfer telemetry

**Date**: 2026-07-28
**Crates**: `xet-runtime` (new `telemetry` config group), `xet-client`
(`cas_client::telemetry`, `Client` trait), `xet-data` (`telemetry` module, session hooks)

## What changed

The client now reports one performance summary per transfer to `POST /v1/telemetry` on the CAS
server (added server-side in `huggingface-internal/xetcas#1207`). Reporting is best-effort: it is
never retried, never surfaces an error, and never blocks data movement.

### New config group: `telemetry`

| Field | Env var | Default |
|---|---|---|
| `enabled` | `HF_XET_TELEMETRY_ENABLED` | `true` |
| `heartbeat_after` | `HF_XET_TELEMETRY_HEARTBEAT_AFTER` | `300s` |
| `heartbeat_interval` | `HF_XET_TELEMETRY_HEARTBEAT_INTERVAL` | `300s` |
| `request_timeout` | `HF_XET_TELEMETRY_REQUEST_TIMEOUT` | `5s` |
| `final_flush_timeout` | `HF_XET_TELEMETRY_FINAL_FLUSH_TIMEOUT` | `2s` |
| `max_in_flight` | `HF_XET_TELEMETRY_MAX_IN_FLIGHT` | `4` |

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

### Session behavior

- `FileUploadSession::finalize_impl` now delegates to a new private `finalize_inner` and reports on
  both the success and error paths. Public signatures are unchanged.
- `FileDownloadSession::finalize` likewise.
- **Both sessions gained a `Drop` impl**, emitting an `aborted`/`dropped` summary when the session
  was never finalized. This is the only reporting path for `XetDownloadStreamGroup`, which holds a
  download session and has no explicit `finish()`. Anything constructing these sessions in a
  non-tokio context is unaffected: `Drop` returns early when there is no runtime handle.
- New public accessors: `FileDownloadSession::client()`, and
  `TestEnvironment::telemetry_docs()` under the `simulation` feature.

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
  Adding a key is safe; changing an existing key's JSON type is not — Elasticsearch field mappings
  are immutable once established, so a type change produces per-document indexing failures and
  needs a reindex. `test_upload_key_set_is_exact` and `test_numeric_types_stable` enforce this.
- **All `f64` values must go through the finite guard** in that module. `serde_json` renders NaN
  and infinity as `null`, and a single such document poisons the field's mapping.
- **No PII.** The payload carries no file names, paths, hashes, repository ids, or user ids; the
  server derives identity from the request's JWT.
- A follow-up PR will add the generated schema artifacts (`telemetry/metrics.schema.json` and
  `telemetry/es-index-template.json`) and a CI compatibility gate. Until then the const key lists
  in the tests are the only thing pinning the contract.
