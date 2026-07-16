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

## Takeaway

No meaningful difference. Steady-state throughput is identical (~1.68 GiB/s) for
all sizes ≥50KB; the sub-µs 1KB case is dominated by fixed per-call overhead, not
the compression loop.

**Why:** on x86_64 with SHA-NI, `sha2`'s pure-Rust backend already dispatches to
the hardware SHA extensions at runtime (via `cpufeatures`) in both versions. The
`asm` feature (hand-written `sha2-asm`) competes against an already
hardware-accelerated path, so removing it in 0.11 costs nothing here.

**Caveat:** this is specific to SHA-NI-capable x86_64. On CPUs without SHA
extensions results could differ, though 0.11's pure-Rust backend also covers the
aarch64 crypto path, so the `asm` feature's remaining value is narrow.
