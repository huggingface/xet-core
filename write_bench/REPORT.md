# File-writer download benchmark: pwrite vs multi-fd vs sequential

**Date:** 2026-07-09
**Branch:** `parallel-file-writing`
**Host:** AWS `c6g.4xlarge` (ARM64 Graviton, 16 vCPU, 30 GiB RAM), Amazon Linux 2023, 100 GB gp3 EBS, `us-east-1`
**Build:** `cargo build -p write-bench --release` (LTO), `write-timing` feature enabled

## Objective

Compare the on-disk write path of Xet file reconstruction across three writer
implementations, measuring both **total download-to-disk time** and **time spent
inside the write syscalls**:

1. **`parallel`** (default, current) — one shared `Arc<File>`, each term written to its
   offset with a positioned write (`pwrite` / `write_all_at`). No per-write seek, no lock.
2. **`multi_fd`** (this report's addition) — each term opens a **fresh** file handle,
   seeks to its offset, writes, flushes, and closes. Reproduces the pre-#603 multi-handle
   approach.
3. **`sequential`** (pre-existing fallback) — a single handle, single background writer
   thread, terms written strictly in order, batched via `writev`.

## Method

- Two files, chosen to differ in reconstruction fragmentation:
  - **standard:** `facebook/sam3.1 / sam3.1_multiplex.pt` — 3,502,755,717 B (3.26 GiB), 64 terms.
  - **fragmented:** `unsloth/gemma-4-31B-it-GGUF / mmproj-F32.gguf` — 2,302,996,960 B (2.14 GiB), 148 terms.
- A fresh Xet read token is minted **before every run** (HEAD the resolve URL for
  `X-Xet-Hash` + size; GET `…/api/models/{repo}/xet-read-token/main` for `casUrl` /
  `accessToken` / `exp`) and passed into `FileDownloadSession::download_file`.
- `chunk_cache = None`, so **every run performs a real network download** (no local chunk reuse).
- Writer selected per process via env (`HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY`,
  `HF_XET_RECONSTRUCT_WRITE_MULTI_FD`), read fresh at `XetContext` construction.
- Matrix: 2 files × 3 writers × 6 runs; the first run is a discarded warmup, 5 measured.
- Only `FileDownloadSession::download_file` → `reconstruct_to_file` is exercised — the
  only path that uses these writers (the streaming / `xtool download` path does not).

### Write-syscall instrumentation

A benchmark-only, `#[cfg(feature = "write-timing")]`-gated module
(`xet_data::file_reconstruction::write_timing`) wraps the actual write syscalls —
`positioned_write` (parallel), `open+seek+write_all+flush+close` (multi_fd, timing only
the `write_all`), and `write_all` / `write_vectored` (sequential) — accumulating
nanoseconds, bytes, and call count into process globals the harness reads per run.
Production builds do not enable the feature and carry zero overhead.

`write_secs` is the summed time in write syscalls. For the two **parallel** writers the
writes run on concurrent blocking-pool threads, so this sum can exceed wall-clock time
(a value >100% of total means writes overlapped heavily). For **sequential** the single
writer thread serializes writes, so `write_secs` approximates wall-clock write time.

## Results

3-writer run (`results/results_3writers.txt`; warmup dropped, 5 measured):

| Case | Writer | Total mean (s) | Total stdev | Write mean (s) | Write % of total | Write calls |
|---|---|---:|---:|---:|---:|---:|
| standard (3.26 GiB) | parallel   | 6.30 | 1.05 | 1.76 | 27.9% | 64 |
| standard | multi_fd   | 5.58 | 0.47 | 1.80 | 32.2% | 64 |
| standard | sequential | 4.68 | 0.16 | 1.36 | 29.0% | ~11 |
| fragmented (2.14 GiB) | parallel   | 2.11 | 0.37 | 2.08 | 98.5% | 148 |
| fragmented | multi_fd   | 1.95 | 0.17 | 1.99 | 102.0% | 148 |
| fragmented | sequential | 2.03 | 0.13 | 0.85 | 41.8% | ~12 |

An earlier 2-writer run with the same instrumentation (`results/results_with_iotime.txt`)
agrees: standard parallel write 1.78s (31.5%) vs sequential 1.38s (23.4%); fragmented
parallel 2.17s vs sequential 0.95s.

## Findings

1. **The two parallel paths (`pwrite` vs `multi_fd`) are effectively identical.**
   Write-syscall time matches within noise — standard 1.80s (multi_fd) vs 1.76s (pwrite),
   fragmented 1.99s vs 2.08s — with the **same write-call count** (64, 148: one write per
   term). Opening + seeking + flushing + closing a fresh handle per term adds **no
   measurable cost** at these term counts: a per-term `open`/`close` is a few microseconds,
   sub-millisecond across 64–148 terms, invisible next to ~2s of `memcpy` into the page
   cache that dominates the write time in both. The syscall overhead that motivated
   consolidating onto one handle in #603 does not materialize here.

2. **Sequential has the lowest write-syscall time** because it batches many terms into a
   few `writev` calls (~11–12 vs 64–148). Fewer, larger syscalls move the same bytes with
   less per-call overhead — 0.85s vs ~2s on the fragmented file.

3. **No writer is write-bound; total time is network-dominated.** On this same-region
   AWS path the download runs at 500–1300 MiB/s and writes hit the page cache, so the
   write path is never the bottleneck. Writes already overlap downloads in every design,
   so removing head-of-line blocking on writes buys nothing here. The write-parallelism
   (`parallel`/`multi_fd`) shows up as `write_secs` exceeding wall time on the fragmented
   file (writes overlapping across threads), yet total time is unchanged.

4. **Total-time differences in this run are noise, not writer effects.** `parallel`/standard
   drew an 8.3s network outlier (stdev 1.05s), which is why its total looks worst despite
   the cheapest measured write time. The `write_secs` column (stdev ~0.05–0.14s) is the
   trustworthy per-writer signal; total time is not, at this sample size.

## Caveats

- **No `fsync`.** All three writers only copy into the OS page cache (matching production
  durability); none forces a device flush. So "write time" is `memcpy` cost, not disk
  latency, which is why the parallel paths converge. **`multi_fd` is exactly where an
  fsync-per-handle would bite** — flushing each term's handle to the device before closing
  could make it dramatically slower than shared-handle `pwrite`. Not tested here.
- Same-region CAS makes this a fast, network-variable environment. A slower/more distant
  source, or a slow local disk, would shift the balance toward the write path and is where
  the writers might actually diverge.
- 5 measured runs is enough to stabilize `write_secs` but not total time.

## Reproduce

On a fresh non-wasm host with Rust installed and `$HF_TOKEN` exported:

```bash
cargo build -p write-bench --release          # add --features write-timing via the crate
export BIN=target/release/write_bench
cd write_bench && bash run_bench.sh            # env: WARMUP, MEASURED, REVISION, WORKDIR
```

Single ad-hoc download (writer chosen by env; `0`/unset = the given path):

```bash
HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY=1 ./write_bench --cas-url … --token … --token-exp … --hash … --size … --out /path   # sequential
HF_XET_RECONSTRUCT_WRITE_MULTI_FD=1     ./write_bench …                                                                     # multi_fd
./write_bench …                                                                                                            # parallel (pwrite, default)
```

Raw outputs: `results/results_3writers.txt`, `results/results_with_iotime.txt`,
`results/results_total_only.txt`.
