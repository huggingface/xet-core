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

Note: `/` (the index) is a static self-description (service, version, pid, argv, endpoints) — every other endpoint carries `as_of`.

## Human TUI

A terminal client renders the same API: `cargo run -p xet-console` (defaults to
127.0.0.1:6660; pass a port, host:port, or URL — `cargo run -p xet-console -- 7001`;
`--interval 250ms` adjusts the poll rate). Pages: [1] overview, [2] upload commit,
[3] download group, [4] concurrency; `?` shows keys.

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
/api/v1/sessions/{sid}/uploads           {as_of, commits: [...]} summaries
/api/v1/sessions/{sid}/uploads/{cid}     files, xorbs, shards, dedup, progress
/api/v1/sessions/{sid}/downloads         {as_of, groups: [...]} summaries
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
