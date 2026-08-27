# Chunker performance

This document records a native-versus-browser-WebAssembly benchmark of the
content-defined chunker exposed by `hf_xet_thin_wasm`. The measurements were
taken on 2026-08-27. They are intended to show the cost of running the same
chunking implementation in a browser, not to predict end-to-end upload speed.

## Summary

With the scalar Wasm build used as the baseline, full chunking in Chrome reached
61-63% of native throughput on an Apple M2 and 30-35% on an x86 EC2 host. The
browser-facing API, which also copies input into Wasm and serializes chunk
metadata back to JavaScript, reached 61-62% and 29-34%, respectively.

BLAKE3 has a separate `wasm32_simd` Cargo feature. In an experimental build that
enabled both that feature and the Wasm `simd128` target feature, core chunking
reached 94-95% of native on the M2 and 50-57% on x86. The browser API reached
85-87% and 46-55%, respectively. Based on these results,
`xet-core-structures` now enables `wasm32_simd` for `wasm32` targets. This is
automatically picked up by both `hf-xet` consumers and the thin Wasm package;
native targets continue to use BLAKE3's default target-specific implementation.
The resulting `wasm32` modules require a runtime with WebAssembly SIMD support.

## Methodology

The benchmark used the same `xet_data::deduplication::Chunker` implementation
for native and Wasm builds.

- Input corpora: deterministic pseudo-random bytes and zero-filled bytes
- Corpus size: 32 MiB
- Target chunk size: 64 KiB
- Streaming block size: 1 MiB
- Work per sample: four iterations, or 128 MiB
- Timing: three warmups followed by 15 measured samples
- Statistic reported below: median throughput
- Native and core benchmark builds: Rust 1.97.1, optimization level 3, LTO,
  one codegen unit
- Browser API build: the existing `wasm-pack build --release` configuration
- Wasm target and post-processing: `wasm32-unknown-unknown`, wasm-opt `-O3`

Three workloads were measured:

- **Boundaries** calls the gear-hash boundary scanner and excludes chunk
  allocation and content hashing.
- **Full** calls `Chunker::next_block` and `Chunker::finish`, including chunk
  allocation and BLAKE3 hashing. Input remains resident inside each runtime so
  the measurement minimizes JS/Wasm boundary overhead.
- **Browser API** exercises the existing `Chunker.add_data` and
  `Chunker.finish` bindings. It includes JS-to-Wasm input copies and serialized
  result objects. The native baseline performs equivalent hash and dedup-result
  conversion.

Each native/Wasm workload produced an identical validation checksum.

The local host was an Apple M2 running headless Chrome 151. The x86 host was an
EC2 `m6i.xlarge` with an Intel Xeon Platinum 8375C running Ubuntu 22.04 and
headless Chrome 152. The benchmark itself is single-threaded.

## Scalar baseline results

| Host and corpus | Native full | Wasm full | Full ratio | Native API | Browser API | API ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| M2, pseudo-random | 824 MiB/s | 520 MiB/s | 63.0% | 819 MiB/s | 503 MiB/s | 61.4% |
| M2, zeros | 806 MiB/s | 493 MiB/s | 61.2% | 798 MiB/s | 493 MiB/s | 61.8% |
| x86, pseudo-random | 1,417 MiB/s | 430 MiB/s | 30.3% | 1,389 MiB/s | 403 MiB/s | 29.0% |
| x86, zeros | 1,187 MiB/s | 418 MiB/s | 35.3% | 1,183 MiB/s | 407 MiB/s | 34.4% |

Boundary-only Wasm throughput was 94% of native on the M2 and 50-52% on x86.
The larger full-chunking gap in the scalar baseline shows that content hashing
is an additional bottleneck, particularly on x86.

## BLAKE3 Wasm-SIMD results

The benchmark experiment added BLAKE3's `wasm32_simd` feature and built Wasm
with:

```text
RUSTFLAGS='-C target-feature=+simd128'
```

This makes the resulting module require a runtime with WebAssembly SIMD
support. In the M2 control run, merely setting the Rust target feature without
enabling BLAKE3's Cargo feature did not materially improve full-chunking
throughput.

| Host and corpus | Native full | SIMD Wasm full | Full ratio | Native API | Browser API | API ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| M2, pseudo-random | 824 MiB/s | 778 MiB/s | 94.4% | 819 MiB/s | 699 MiB/s | 85.3% |
| M2, zeros | 806 MiB/s | 766 MiB/s | 95.1% | 798 MiB/s | 696 MiB/s | 87.3% |
| x86, pseudo-random | 1,417 MiB/s | 701 MiB/s | 49.5% | 1,389 MiB/s | 635 MiB/s | 45.7% |
| x86, zeros | 1,187 MiB/s | 680 MiB/s | 57.3% | 1,183 MiB/s | 649 MiB/s | 54.8% |

The feature nearly closed the core-compute gap on the M2, but only partially
closed it on x86. Native x86 code can use wider host-specific SIMD paths, while
portable Wasm SIMD is 128-bit. The remaining difference between the SIMD core
and browser-API results is the practical cost of the binding boundary and
result serialization.

## Binary size impact

The final `hf_xet_thin_wasm_bg.wasm` artifact was also measured before and
after enabling `wasm32_simd`. Both variants used the normal
`wasm-pack build --release --target web` pipeline, including `wasm-opt`. The
SIMD variant enabled only BLAKE3's target-scoped Cargo feature; it did not set a
global `RUSTFLAGS` target feature.

| Build | Raw Wasm | gzip -9 | Brotli quality 11 |
| --- | ---: | ---: | ---: |
| Scalar baseline | 155,218 bytes | 65,128 bytes | 54,165 bytes |
| BLAKE3 Wasm SIMD | 169,297 bytes | 69,115 bytes | 57,185 bytes |
| Increase | 14,079 bytes (9.1%) | 3,987 bytes (6.1%) | 3,020 bytes (5.6%) |

The size increase is therefore about 14 KiB in the raw module and 3-4 KiB over
the wire with compression.

## Interpreting the numbers

- Compare native and Wasm results only within the same host. Absolute
  throughput across the two machines is not directly comparable.
- These measurements cover chunk boundary detection, chunk allocation, and
  chunk hashing. They do not include network requests, dedup lookups, or upload
  scheduling.
- Input shape affects the number and size of chunks, so both pseudo-random and
  zero-filled corpora are included.
- Browser, compiler, dependency, and CPU changes can move these numbers. Treat
  them as a dated baseline and rerun the benchmark before making a performance
  regression decision.
