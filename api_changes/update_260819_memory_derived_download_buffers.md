# Memory-derived download buffer defaults

Date: 2026-08-19
Issue: #927 (related: huggingface_hub#3300)

## Summary

The three reconstruction download buffer defaults are no longer compile-time
constants. They are derived at session startup from the memory usable by the
process: `min(host physical RAM, effective cgroup memory limit)`.

| knob | derivation | floor | ceiling | old constant |
|---|---|---|---|---|
| `reconstruction.download_buffer_size` | usable/16 | 32MB | 16GB | 2GB |
| `reconstruction.download_buffer_perfile_size` | usable/64 | 8MB | 2GB | 512MB |
| `reconstruction.download_buffer_limit` | usable/4 | 128MB | 64GB | 8GB |

Values are rounded down to a multiple of 8MB (decimal units, matching
`ByteSize`). 32GB of usable memory reproduces the old defaults (perfile lands
at 496MB rather than 512MB); >= 256GB reproduces the old high-performance
constants. The invariant `size + max_concurrent_file_downloads * perfile <=
limit` holds across the whole range, so the three values describe one coherent
allocation.

High-performance mode (`HF_XET_HIGH_PERFORMANCE` / `HF_XET_HP`) uses fractions
twice as aggressive (usable/8, usable/32, usable/2) with the same floors and
ceilings — it no longer sets a 16GB base buffer on an 8GB machine.

## Toolchain and dependency changes

- **Rust toolchain: 1.94.1 -> 1.95.0** (all CI workflow pins). Required by
  sysinfo 0.39.
- **sysinfo: 0.38.4 -> 0.39.6** (workspace pin `0.39`; all four lockfiles).
  0.39 adds `Process::cgroup_limits()`, which resolves the cgroup path from
  `/proc/<pid>/cgroup` and takes the tightest memory limit across ancestor
  cgroups (v1 and v2) — the container-aware probe this change needs, so no
  hand-rolled `/proc` parsing is maintained in this repo.

## New API

- `xet_runtime::utils::system_memory` (new module): `usable_system_memory()`
  (cached probe; host total via sysinfo, cgroup limit via sysinfo 0.39's
  `Process::cgroup_limits` — cgroup v1 + v2, nested paths, namespaced roots),
  `default_download_buffer_sizes()`, `high_performance_download_buffer_sizes()`,
  `STATIC_DEFAULTS`, `STATIC_HP_DEFAULTS`.
- `reconstruction::ConfigValueGroup::normalize()`: raises
  `download_buffer_limit` to `download_buffer_size` if it is below it. Called
  by `XetContext::new` before the config is frozen. Previously an env
  configuration with `SIZE > LIMIT` panicked at context construction.

## Behavior changes downstream consumers should know about

1. **Defaults are machine-dependent.** Tests or tooling asserting the literal
   values 2GB/512MB/8GB (or HP 16GB/2GB/64GB) will see derived values instead.
   Set `HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS=0` to restore the static
   defaults (kill switch; per-knob env vars also still override).
2. **Env overrides now beat the high-performance preset.** `XetConfig::new()`
   applies defaults -> HP preset -> env overrides. Previously HP silently
   overwrote explicitly set `HF_XET_RECONSTRUCTION_*` values.
3. **`apply_env_overrides` / `with_env_overrides` no longer reset fields.**
   Fields without an env override keep their current value (e.g. from a
   preset) instead of being reset to the macro default. Callers relying on the
   old reset behavior must construct a fresh config instead.
4. Wasm and any platform where memory cannot be determined keep the old static
   defaults. Windows Job Object / Windows-container limits are not probed
   (documented limitation; use the env vars). WSL2 and all Linux containers go
   through the cgroup path.
5. Known accounting slack (pre-existing, unchanged): up to one decompressed
   xorb block (<= 64MiB) per active connection can be resident while only
   partially covered by buffer permits. The floors leave room for this even in
   very small containers.

## Observability

The first probe logs one `info!` line with host total, cgroup limit, usable
memory, and the derived triple. Per-field `Config: <name> = <value> (default)`
log lines and the Python `XetConfig` repr show the derived values.
