#!/bin/sh

set -ex

# A couple of steps are necessary to get this build working which makes it slightly
# nonstandard compared to most other builds.
#
# * First, the Rust standard library needs to be recompiled with atomics
#   enabled. to do that we use Cargo's unstable `-Zbuild-std` feature.
#
# * Next we need to compile everything with the `atomics` and `bulk-memory`
#   features enabled, ensuring that LLVM will generate atomic instructions,
#   shared memory, passive segments, etc.

RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals \
  -C link-arg=--shared-memory \
  -C link-arg=--max-memory=1073741824 \
  -C link-arg=--import-memory \
  -C link-arg=--export=__wasm_init_tls \
  -C link-arg=--export=__tls_size \
  -C link-arg=--export=__tls_align \
  -C link-arg=--export=__tls_base \
  --cfg getrandom_backend=\"wasm_js\"" \
cargo +nightly build \
    --example simple \
    --target wasm32-unknown-unknown \
    --release \
    -Z build-std=std,panic_abort

# Generate JS/WASM glue for web targets
RUSTFLAGS='--cfg getrandom_backend="wasm_js"' \
wasm-bindgen \
    target/wasm32-unknown-unknown/release/examples/simple.wasm \
    --out-dir ./examples/target/ \
    --typescript \
    --target web
