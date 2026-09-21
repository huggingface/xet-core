# `upload` feature flag on `hf-xet` (and sub-crates)

**Date:** 2025-09-21
**Crates affected:** `hf-xet` (xet_pkg), `xet-data`, `xet-client`

## Summary

The published `hf-xet` crate (and the internal `xet-data` / `xet-client` crates) now have
an `upload` cargo feature, **enabled by default**. It gates all upload functionality.
Disabling it produces a download-only build that compiles out the upload pipeline and the
dependencies only uploads need (`gearhash`, `sha2` in `xet-data`; the simulation-client
dependency `tempfile` in `xet-client`).

`xet-core-structures` is intentionally **not** feature-gated: xorb-object
(de)serialization is shared — downloads deserialize xorb-format term data (including
BG4 regrouping and lz4 decompression), so `lz4_flex`/`countio` cannot be optional there.

## How to consume

- No change for existing consumers: `upload` is in `default`.
- Download-only consumers:

  ```toml
  hf-xet = { version = "1", default-features = false, features = ["rustls-tls"] }
  ```

  (or `native-tls` / `native-tls-vendored` instead of `rustls-tls`).

- Cargo feature-unification caveat: if another crate in the same build enables `upload`
  (or depends on `hf-xet` with defaults), uploads stay compiled in for the whole graph.

## API removed when `upload` is disabled

In `hf-xet` (`xet_session` module):

- `XetSession::new_upload_commit`
- `XetUploadCommit`, `XetUploadCommitBuilder`, `XetFileUpload`, `XetStreamUpload`,
  `XetCommitReport`, `XetFileMetadata`
- `xet_session::{Sha256Policy, DeduplicationMetrics}`
- `legacy::{upload_bytes_async, upload_async, hash_files_async, clean_bytes, clean_file,
  FileUploadSession, Sha256Policy}`

In `xet-data`:

- `deduplication` module (chunking/dedup, `DeduplicationMetrics`, `RawXorbData`)
- `processing::{FileUploadSession, SingleFileCleaner, Sha256Policy, upload_ranges, migration_tool, test_utils}`
- `progress_tracking::{upload_tracking, UploadGroupProgress, ShardUploadProgress}`

In `xet-client`:

- `Client` trait methods `acquire_upload_permit`, `upload_shard`, `upload_xorb`,
  `get_file_chunk_hashes` (implementations included)
- `cas_client::{simulation, chunk_window_builder, shard_upload_v2}` and the
  `LocalClient`/`MemoryClient`/simulation re-exports; `UploadProgressStream`;
  `ShardUploadProgressCallback`/`ShardUploadProgressType`

## Behavioral notes for downstream code

- The `local://` and `memory://` CAS endpoints (simulation clients) require the `upload`
  feature; `create_remote_client` returns a `CASConfigError` in download-only builds for
  those endpoints.
- Feature interactions: `simulation` implies `upload` (the local/simulation clients
  implement the upload trait methods); `internal-tools` (the `xtool` binary) implies
  `upload` because it exercises uploads.
- In-repo consumers (`hf_xet` Python bindings, `git_xet`, both wasm crates) enable
  `upload` explicitly and are unaffected.
- Tests that exercise upload (including download tests that seed fixtures through the
  upload pipeline) have `required-features = ["upload"]` / are `#[cfg(feature = "upload")]`;
  they still run in the default configuration.
