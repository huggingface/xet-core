#!/bin/sh

set -ex

# Build script for hf_xet_wasm_download. Mirrors wasm/hf_xet_wasm/build_wasm.sh:
#  - rebuilds std with atomics enabled
#  - enables atomics/bulk-memory/mutable-globals features
#  - runs wasm-bindgen to produce ES modules for web targets

WASM_BINDGEN_VERSION="0.2.100"

if command -v wasm-bindgen >/dev/null 2>&1; then
    INSTALLED_WASM_BINDGEN_VERSION="$(wasm-bindgen --version | awk '{print $2}')"
else
    INSTALLED_WASM_BINDGEN_VERSION=""
fi

if [ "$INSTALLED_WASM_BINDGEN_VERSION" != "$WASM_BINDGEN_VERSION" ]; then
    cargo install -f wasm-bindgen-cli --version "$WASM_BINDGEN_VERSION"
fi

TARGET_RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals \
  -C link-arg=--shared-memory \
  -C link-arg=--max-memory=1073741824 \
  -C link-arg=--import-memory \
  -C link-arg=--export=__wasm_init_tls \
  -C link-arg=--export=__tls_size \
  -C link-arg=--export=__tls_align \
  -C link-arg=--export=__tls_base \
  -C link-arg=--export=__heap_base \
  --cfg getrandom_backend=\"wasm_js\"" \
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS="$TARGET_RUSTFLAGS" \
cargo +nightly build \
    --target wasm32-unknown-unknown \
    --release \
    -Z build-std=std,panic_abort

wasm-bindgen \
    target/wasm32-unknown-unknown/release/hf_xet_wasm_download.wasm \
    --out-dir ./pkg/ \
    --typescript \
    --target web
