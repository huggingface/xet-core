# Plan: `upload` feature flag for `hf-xet`

Goal: add an `upload` cargo feature to the published `hf-xet` crate (xet_pkg), **enabled by
default**, that — when disabled — compiles out all upload functionality and the dependencies
only uploads need, leaving a download-only (smaller) build.

## 0. Current state (from repo audit)

Published crate: `xet_pkg` (package name `hf-xet`). Dependency chain:

```
hf-xet (xet_pkg)
 └─ xet-data, xet-client
     └─ xet-core-structures
         └─ xet-runtime
```

Where upload code lives today:

| Crate | Upload-only code | Shared / download code |
|---|---|---|
| `xet_pkg` | `xet_session::{upload_commit, upload_file_handle, upload_stream_handle}`, `XetSession::new_upload_commit`, `legacy::data_client` upload paths | download groups/handles, session runtime, error types |
| `xet_data` | `deduplication` (chunking + dedup), `processing::{file_upload_session, range_upload, deduplication_interface, shard_interface, migration_tool, file_cleaner}` | `file_reconstruction`, `processing::file_download_session`, `xet_file` (XetFileInfo), progress_tracking |
| `xet_client` | `Client::{upload_shard, upload_xorb, acquire_upload_permit, get_file_chunk_hashes}`, `cas_client::{shard_upload_v2, multipart, chunk_window_builder}`, `cas_types::ShardUploadEvent`, `progress_tracked_streams::UploadProgressStream` | reconstruction queries, `chunk_cache` (redb), `hub_client`, `adaptive_concurrency`, telemetry, download streams |
| `xet_core_structures` | `xorb_object` serialization (compression, byte_grouping — xorbs are *uploaded* artifacts), `metadata_shard` shard-*writing* side | `merklehash`, `metadata_shard::{file_structs (MDBFileInfo), chunk_verification, shard_file_reconstructor}` — downloads need these |
| `xet_runtime` | nothing | everything |

Dependencies that appear upload-only (verify in Phase 0):

- `xet_data`: `gearhash` (only used in `deduplication/chunking.rs`)
- `xet_core_structures`: `lz4_flex` (only `xorb_object/compression_scheme.rs`), `heapify`,
  `countio` (xorb/shard writing paths)

## 1. Feature design

### xet_pkg (`hf-xet`) — the public flag

```toml
[features]
default = ["rustls-tls", "upload"]
upload = ["xet-data/upload", "xet-client/upload"]
```

Gate upload-only public API with `#[cfg(feature = "upload")]`:

- `xet_session/mod.rs`: `pub mod upload_commit; upload_file_handle; upload_stream_handle;`
  get `#[cfg(feature = "upload")]`; re-exports (`XetUploadCommit`, `XetCommitReport`,
  `XetFileMetadata`, `XetFileUpload`, `XetStreamUpload`, `DeduplicationMetrics`, …) likewise.
- `session.rs`: `XetSession::new_upload_commit` (+ `_blocking` variants) cfg'd out.
- `legacy/` (used by the `hf_xet` Python binding): upload entry points in
  `legacy/data_client.rs` / `mod.rs` cfg'd; keep download entry points.
- `test_utils.rs`: gate upload helpers; download helpers stay.
- Doc comment / crate-level docs: mention the feature.

### xet_data

```toml
[features]
default = ["rustls-tls", "upload"]     # keep current behavior for direct dependents
upload = [
    "xet-client/upload",
    "xet-core-structures/upload",
    "dep:gearhash",                    # becomes optional
]
```

Gated code: `pub mod deduplication;`, `processing::{file_upload_session, range_upload,
deduplication_interface, shard_interface, file_cleaner}` (and `migration_tool`, already
wasm-gated), plus their re-exports in `processing/mod.rs` (`FileUploadSession`,
`upload_ranges`, `SingleFileCleaner`, `Sha256Policy`, `RawXorbData`, `create_remote_client`
if upload-only). Shared: `file_reconstruction`, `file_download_session`, `xet_file`,
`progress_tracking`, `telemetry`.

### xet_client

```toml
[features]
default = ["rustls-tls", "upload"]
upload = ["xet-core-structures/upload"]
```

- `cas_client/interface.rs`: the `Client` trait mixes upload and download methods. Two options:
  - **(a) Minimal (recommended for first pass):** `#[cfg(feature = "upload")]` on the four
    upload trait methods (`acquire_upload_permit`, `upload_shard`, `upload_xorb`,
    `get_file_chunk_hashes`) and on the matching impl blocks in `remote_client.rs` and the
    simulation clients (`memory_client`, `local_client`, `simulation_client`, …).
  - (b) Cleaner long-term: split into `Client` + `UploadClient` supertrait; larger refactor,
    breaks internal impls — do later if desired.
- Gate `cas_client::{shard_upload_v2 (already wasm-gated), multipart, chunk_window_builder}`,
  `ShardUploadEvent` / `ShardUploadProgress*` exports, `UploadProgressStream`.
- `simulation` feature: the local test server exercises uploads. Either make
  `simulation = [..., "upload"]` or cfg the upload handlers inside `simulation/` — simplest is
  to have `simulation` imply `upload`.
- Keep shared: `adaptive_concurrency` (statrs is used for RTT prediction on both directions),
  `chunk_cache`/redb, `retry_wrapper`, `hub_client`, download progress streams, telemetry.

### xet_core_structures

```toml
[features]
default = ["upload"]
upload = ["dep:lz4_flex", "dep:heapify", "dep:countio"]   # each becomes optional, pending audit
```

- Gate `xorb_object` serialization / `byte_grouping` (xorb assembly is upload-only;
  downloads fetch CAS blocks via reconstruction terms, not xorbs).
- `metadata_shard` is **partially shared**: `file_structs` (MDBFileInfo) and
  `chunk_verification` are used by downloads — keep them ungated; gate the shard-*writing*
  side (`shard_file` writing, `shard_in_memory`, `streaming_shard` writer paths) as the audit
  dictates.
- `merklehash` is shared — ungated.

### xet_runtime

No changes.

## 2. Propagating to in-repo consumers

| Consumer | Action |
|---|---|
| `hf_xet` (Python bindings) | Needs uploads. Depends on `xet-pkg` with default features → gets `upload` automatically; optionally declare it explicitly for clarity: `features = ["python", "upload"]`. |
| `git_xet` | Needs uploads. Default features → fine; declare explicitly (`xet-pkg/upload`, `xet-data/upload`). |
| `wasm/hf_xet_wasm` | Exposes wasm upload API; default features → fine (wasm build already cfgs out `shard_upload_v2` etc. — combine cfgs: `#[cfg(all(feature = "upload", not(target_family = "wasm")))]`). |
| `wasm/hf_xet_thin_wasm` | Uses `xet_data::deduplication::Chunker` directly (upload-side chunking). Depends on `xet-data` with defaults → gets `upload`; declare explicitly. |
| `examples/xet_pkg_napi`, `write_bench`, `simulation` | Audit each; they use defaults today, so behavior is unchanged. |
| `xet_pkg` bins/tests/examples | `xtool` + `test_xtool_cli` + examples use uploads: make `internal-tools = ["dep:clap", "dep:url", "dep:walkdir", "upload"]` (required-features already exist on the targets). |
| `xet_data` dev-deps / benches | `dev-dependencies` pull `xet-client` with `simulation` (implies upload) — fine. Gate upload-specific unit tests with `#![cfg(feature = "upload")]` at file level. |

## 3. Implementation order (bottom-up, keeps the workspace compiling at each step)

1. **Phase 0 — audit (½ day):** finalize the upload-only module/dep map. For each candidate
   dependency (`gearhash`, `lz4_flex`, `heapify`, `countio`, …) confirm no download path uses
   it (`rg`; also `cargo bloat`/`cargo tree` sanity). For each candidate module, check nothing
   shared imports it (e.g. does `file_reconstruction` touch `deduplication`? — current grep
   says no).
2. **Phase 1 — xet_core_structures:** add `upload` feature, make deps optional, gate xorb
   serialization + shard writing. `cargo check -p xet-core-structures
   --no-default-features`.
3. **Phase 2 — xet_client:** add `upload`, gate trait methods + impls + modules;
   `simulation` implies `upload`.
4. **Phase 3 — xet_data:** add `upload`, `dep:gearhash`, gate dedup/processing modules.
5. **Phase 4 — xet_pkg:** add the public `upload` feature (default on), gate `xet_session`
   upload API + legacy upload paths, `internal-tools` requires `upload`.
6. **Phase 5 — consumers + CI + docs:** update all in-repo consumers, add CI jobs, update
   README/docs.

## 4. Testing / verification

- Existing CI keeps covering the default (upload on) build — no change needed there.
- New CI job(s) in `.github/workflows/ci.yml` for the download-only configuration:
  ```yaml
  cargo check -p hf-xet --no-default-features --features rustls-tls
  cargo test  -p hf-xet --no-default-features --features rustls-tls
  # and per-crate:
  cargo test -p xet-data -p xet-client -p xet-core-structures --no-default-features
  ```
- **Feature-unification caveat:** within this workspace, other members (git_xet, hf_xet)
  enable `upload`, so a plain workspace `cargo test` always has it on. To validate the true
  external-consumer experience, build a scratch project outside the workspace that depends
  on hf-xet by path with `default-features = false, features = ["rustls-tls"]` and run
  `cargo tree` there — confirm `gearhash`, `lz4_flex` (etc.) are gone from the resolved graph,
  and compare binary sizes.
- `cargo hack --feature-powerset` (or at least `--no-default-features` + each feature) on the
  consolidated crates to catch cfg mistakes.
- `cargo machete` (repo already uses it) — ensure newly-optional deps don't get flagged;
  `cargo check --features strict` with upload off to catch unused imports under `deny(warnings)`.

## 5. Semver / compatibility notes

- Additive, default-on feature → no breaking change for existing consumers; `default-features
  = false` users who relied on uploads must now add `features = ["upload"]` — call this out
  in the release notes and the README feature table.
- docs.rs builds with all features → docs stay complete; document the download-only recipe:
  `hf-xet = { version = "...", default-features = false, features = ["rustls-tls"] }`.
- Note for future external consumers: cargo feature unification means `upload` cannot be
  disabled for *one* dependent if another crate in the same graph enables it — expected cargo
  behavior, worth documenting.

## 6. Risks

- **Trait entanglement in `xet_client`** (`Client` mixes upload/download) — the cfg-gating of
  trait methods must touch every impl (remote + 4 simulation clients); a split trait is
  cleaner but a bigger refactor. Start with (a).
- **Shared types referenced from both sides** (`XetFileInfo`, `Sha256Policy`, progress types,
  `MerkleHash`): gate the *functions*, keep the *types* ungated where both sides or the public
  API need them; only move a type behind `upload` if nothing download-side references it.
- `strict` (`deny(warnings)`) builds: gating code can orphan imports — run strict checks in
  both feature configurations.
- Wasm: existing `not(target_family = "wasm")` cfgs must be combined with the feature cfg,
  not replace it.
