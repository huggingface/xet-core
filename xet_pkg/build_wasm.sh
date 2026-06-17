#!/bin/sh

set -ex

# Compile gate for the hf-xet (xet_pkg) library on wasm32-unknown-unknown.
#
# This is a `cargo check`, which only emits metadata and never links, so the
# `-C link-arg=...` flags used by wasm/hf_xet_wasm/build_wasm.sh (shared-memory,
# import-memory, TLS/heap exports) are intentionally omitted here — they affect
# the final binary link, not a metadata-only check. The flags that do matter for
# a check (target features, the getrandom backend cfg, and rebuilding std with
# atomics via -Z build-std) are kept in sync with that script.

CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals --cfg getrandom_backend=\"wasm_js\"" \
  cargo +nightly check \
    --target wasm32-unknown-unknown \
    -p hf-xet \
    -Z build-std=std,panic_abort
