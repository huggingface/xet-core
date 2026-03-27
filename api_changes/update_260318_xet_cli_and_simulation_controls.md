# xet CLI utility and simulation control updates

**Date**: 2026-03-18

## Summary

This branch introduces a new `xet` command-line utility in `xet_pkg`, and adds
simulation-control updates used by GC/integrity test flows.

The progress-tracking redesign is documented separately in
`update_260313_progress_tracking_redesign.md`; this file covers the additional
CLI and simulation-control deltas.

## New `xet` CLI binary (`xet_pkg`)

- New binary target:
  - `[[bin]] name = "xet"` in `xet_pkg/Cargo.toml`
  - entrypoint: `xet_pkg/src/bin/xet/main.rs`
- New subcommands:
  - `upload` - upload one or more files and print/write resulting file metadata
  - `download` - download by Xet hash + size to a destination path
  - `query` - fetch reconstruction metadata (optional byte range)
  - `stats` - dry-run dedup/compression analysis without upload

### CLI endpoint/token/config behavior

- Endpoint resolution:
  - absolute filesystem paths are normalized to `local://...`
  - `HF_ENDPOINT` is used when `--endpoint` is not provided
  - default endpoint remains `https://huggingface.co`
- Token resolution:
  - `--token` overrides `HF_TOKEN`
- Config overrides:
  - repeated `-c/--config KEY=VALUE` are applied before runtime/session creation

## New CLI support modules

The binary is implemented in:

- `xet_pkg/src/bin/xet/upload.rs`
- `xet_pkg/src/bin/xet/download.rs`
- `xet_pkg/src/bin/xet/query.rs`
- `xet_pkg/src/bin/xet/stats.rs`
- `xet_pkg/src/bin/xet/session.rs`

## Simulation / deletion control updates (`xet_client`)

- `DeletionControlableClient` adds:
  - `remove_shard_dedup_entries(&self, shard_hash: &MerkleHash) -> Result<()>`
- Local-server simulation routes add:
  - `DELETE /simulation/shards/{hash}/dedup_entries`
- `SimulationControlClient` implements the new route-backed method.

### LocalClient deletion semantics

- `delete_file_entry` is now a soft-delete operation (records file hash in an
  internal deleted set without rewriting shard files).
- Soft-deleted files are filtered from:
  - file-entry listing
  - reconstruction lookup
  - file-size / file-data retrieval
- Integrity checking is cross-shard aware for dedup references and validates
  referenced XORB presence on disk.

## Downstream impact notes

- Consumers that implement `DeletionControlableClient` must implement
  `remove_shard_dedup_entries`.
- Packaging that exposes binaries from `xet_pkg` can now surface the `xet` tool.
- Automation/tests that assume hard-delete behavior for `delete_file_entry`
  should be updated to the soft-delete model above.
