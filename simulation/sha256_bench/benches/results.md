# SHA-256: `sha2` 0.10.9 (`asm`) vs 0.11.0

This benchmark backs PR #899 (bump `sha2` 0.10 → 0.11). `sha2` 0.11 removed the
`asm` feature that `xet_data` enabled on non-Windows targets, so before dropping
it we want evidence that hashing throughput does not regress.

## How to run

```bash
cd simulation/sha256_bench
cargo run --release
```

The crate links **both** `sha2` versions at once (Cargo package renaming) so one
run compares them on identical hardware/build settings. It hashes an in-memory
buffer per size with `Digest::update` + `finalize` (matching `xet_data`), runs
for a ≥3s budget per case, and reports the best (min-time) throughput.

## Results

### x86_64 (SHA-NI)

Environment:

- CPU: AMD EPYC 9R14 (x86_64, has the SHA-NI extension), 16 vCPU
- OS: Ubuntu 24.04.4 LTS, kernel Linux 6.17.0-1019-aws (AWS EC2)
- Toolchain: rustc 1.95.0
- Build: `--release` (`opt-level=3`, `lto=true`, mirroring the workspace release
  profile), single-threaded

| Size | 0.10.9 (`asm`) best/op | 0.11.0 best/op | 0.10 GiB/s | 0.11 GiB/s |    Δ |
| ---- | ---------------------: | -------------: | ---------: | ---------: | ---: |
| 1KB  |                 630 ns |         630 ns |      1.514 |      1.514 | 0.0% |
| 50KB |               28.04 µs |       28.04 µs |      1.701 |      1.701 | 0.0% |
| 1MB  |              573.00 µs |      573.02 µs |      1.704 |      1.704 | 0.0% |
| 50MB |               29.00 ms |       28.99 ms |      1.684 |      1.684 | 0.0% |
| 1GB  |              594.63 ms |      594.43 ms |      1.682 |      1.682 | 0.0% |

(Δ = 0.11 best throughput relative to 0.10-`asm`; positive means 0.11 faster.)

### aarch64 (Apple Silicon)

Environment:

- CPU: Apple M2 Max (aarch64, has the FEAT_SHA256 crypto extension), 12 cores
- OS: macOS 26.5.2
- Toolchain: rustc 1.95.0
- Build: `--release` (`opt-level=3`, `lto=true`, mirroring the workspace release
  profile), single-threaded

| Size | 0.10.9 (`asm`) best/op | 0.11.0 best/op | 0.10 GiB/s | 0.11 GiB/s |     Δ |
| ---- | ---------------------: | -------------: | ---------: | ---------: | ----: |
| 1KB  |                 333 ns |         333 ns |      2.864 |      2.864 |  0.0% |
| 50KB |               21.12 µs |       21.12 µs |      2.257 |      2.257 |  0.0% |
| 1MB  |              434.83 µs |      434.79 µs |      2.246 |      2.246 |  0.0% |
| 50MB |               22.01 ms |       21.79 ms |      2.219 |      2.241 | +1.0% |
| 1GB  |              471.54 ms |      454.12 ms |      2.121 |      2.202 | +3.8% |

## Takeaway

No regression on either architecture. On x86_64 steady-state throughput is
identical (~1.68 GiB/s) for all sizes ≥50KB; on the M2 Max, 0.11 matches
0.10-`asm` at small sizes and is slightly faster at 50MB (+1.0%) and 1GB
(+3.8%). The sub-µs 1KB case is dominated by fixed per-call overhead, not the
compression loop.

**Why:** on both x86_64 with SHA-NI and aarch64 with the crypto extensions,
`sha2`'s pure-Rust backend already dispatches to the hardware SHA instructions
at runtime (via `cpufeatures`) in both versions. The `asm` feature
(hand-written `sha2-asm`) competes against an already hardware-accelerated
path, so removing it in 0.11 costs nothing here.

**Caveat:** both machines measured have hardware SHA extensions. On CPUs
without them results could differ, but such hardware is rare among xet-core
targets and the software fallback exists in both versions, so the `asm`
feature's remaining value is narrow.
