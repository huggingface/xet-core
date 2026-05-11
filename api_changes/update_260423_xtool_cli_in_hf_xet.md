# API Update: `xtool` moved to `hf-xet` and expanded command surface (2026-04-23)

## Overview

`xtool` is now provided by the `hf-xet` (`xet_pkg`) crate instead of the legacy
`xet_data` processing-bin location.  The CLI now supports both direct CAS mode
and Hub-resolved mode, and adds file-oriented subcommands for upload/download/scan
and reconstruction inspection.

---

## What changed

- Added `xtool` binary target under `xet_pkg`.
- Removed legacy `xet_data/src/processing/bin/xtool.rs`.
- Added command groups:
  - `xtool file upload`
  - `xtool file download`
  - `xtool file scan`
  - `xtool file dump-reconstruction`
  - `xtool dedup`
  - `xtool query`
- Added direct endpoint mode:
  - `--endpoint local://...` (or absolute path auto-normalized to `local://...`)
  - optional `--token`
- Added Hub mode with endpoint/JWT resolution:
  - `--repo-type` + `--repo-id` required together
  - Hub endpoint precedence: `--endpoint` > `HF_ENDPOINT` > default
  - token precedence: `--token` > `HF_TOKEN`
- Added global `--config KEY=VALUE` overrides for `XetConfig`.

---

## Behavioral notes

- In direct mode, `xtool` uses the provided endpoint as CAS directly.
- In Hub mode, `xtool` resolves CAS URL + access token via `get_cas_jwt`.
- `file download` supports source and output write ranges.
- `file upload` supports stdin (`-`) and optional JSON output.
- `file scan` reports dedup/compression metrics and can emit JSON.

---

## Downstream impact

- Tooling and docs that referenced the legacy `xet_data` `xtool` location should
  switch to invoking the `xet_pkg`/`hf-xet` `xtool` binary target.
- CI and local scripts can now use one CLI for both local CAS testing and
  Hub-backed auth flows.

