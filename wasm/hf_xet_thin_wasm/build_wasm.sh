#!/bin/sh

set -ex

# however, wasm-pack produces by default smaller wasm binaries by using a tool called wasm-opt
# we can tweak the optimizations via Cargo.toml configuration
# See: https://rustwasm.github.io/docs/wasm-pack/cargo-toml-configuration.html

# valid values here: web, nodejs, bundler, no-modules, deno, default: web
JS_TARGET="${JS_TARGET:-web}"

wasm-pack build --release --target $JS_TARGET

# adapted version of hf_xet_wasm for this package (no need for special features)
# This is essentially the same steps that `wasm-pack` runs minus the optimization which we can add explicitly.

#RUSTFLAGS='--cfg getrandom_backend="wasm_js"' \
#    cargo +nightly build --target wasm32-unknown-unknown --release -Z build-std=std,panic_abort

#RUSTFLAGS='--cfg getrandom_backend="wasm_js"' wasm-bindgen \
#    target/wasm32-unknown-unknown/release/hf_xet_thin_wasm.wasm \
#    --out-dir pkg \
#    --typescript \
#    --target web
