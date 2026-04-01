# xet CLI utility updates

**Date**: 2026-03-18

## Summary

This branch introduces a new `xet` command-line utility in `xet_pkg`.

The progress-tracking redesign is documented separately in
`update_260313_progress_tracking_redesign.md`; this file covers additional CLI
deltas only.

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

## Downstream impact notes

- Packaging that exposes binaries from `xet_pkg` can now surface the `xet` tool.
