# Agent Guide for wasm

WebAssembly builds of xet-core. Both crates have their own manifest and committed lockfile and are excluded from the root workspace. Shared conventions are in the [root guide](../AGENTS.md).

- `hf_xet_thin_wasm/` — published chunking and hashing for `huggingface.js`; built with `wasm-pack`.
- `hf_xet_wasm/` — example upload/download wrapper around `XetSession`; a CI smoke-test target, not a published SDK. Built with a threaded nightly `-Z build-std` and `wasm-bindgen`.
- `ci-smoke/` — Node + Playwright runner for `hf_xet_wasm` browser scenarios.

## Setup

- Nightly toolchain with the `wasm32-unknown-unknown` target and `rust-src` component (`hf_xet_wasm/rust-toolchain.toml` pins this).
- `wasm-bindgen-cli` 0.2.121 and `wasm-pack` 0.14.0, matching the `=0.2.121` `wasm-bindgen` pin in both `Cargo.toml` files. Bump all of them together.
- Node 24 for `ci-smoke`.

## Build and test

| Command | Purpose |
| --- | --- |
| `(cd xet_pkg && ./build_wasm.sh)` | Compile gate: `cargo check` of `hf-xet` for wasm32 |
| `(cd wasm/hf_xet_thin_wasm && ./build_wasm.sh)` | Build the thin package into `pkg/` |
| `(cd wasm/hf_xet_wasm && ./build_wasm.sh)` | Build the example wrapper into `pkg/` |
| `(cd wasm/ci-smoke && npm ci && npx playwright install --with-deps chromium && node run.mjs all)` | Browser smoke scenarios; needs a built `hf_xet_wasm/pkg/` |

Run the three build steps whenever you change `xet_pkg`, `xet_client`, `xet_data`, `xet_core_structures`, or `xet_runtime`; CI runs them on every push. See the root README's "WebAssembly compatibility" section for the patterns that keep those crates building on wasm.

The smoke scenarios talk to the production Hub and CAS. Read scenarios work anonymously; upload scenarios need `HF_SMOKE_TEST_TOKEN` with write access to `xet-team/xet-wasm-test`. Run a single scenario with `node run.mjs <name>` from `scenarios/`. Scenario files are kebab-case `.mjs` with camelCase identifiers.

After a dependency change, confirm `git status --porcelain wasm/*/Cargo.lock` is empty; CI fails on modified lockfiles.
