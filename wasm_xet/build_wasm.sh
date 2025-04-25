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

RUSTFLAGS='-C target-feature=+atomics,+bulk-memory,+mutable-globals --cfg getrandom_backend="wasm_js"' \
    cargo +nightly build --target wasm32-unknown-unknown --release -Z build-std=std,panic_abort

RUSTFLAGS='--cfg getrandom_backend="wasm_js"' wasm-bindgen \
    target/wasm32-unknown-unknown/release/wasm_xet.wasm \
    --out-dir ./web/ \
    --target web
