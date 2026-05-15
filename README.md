<!---
Copyright 2024 The HuggingFace Team. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
<p align="center">
    <a href="https://github.com/huggingface/xet-core/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/huggingface/xet-core.svg?color=blue"></a>
    <a href="https://github.com/huggingface/xet-core/releases"><img alt="GitHub release" src="https://img.shields.io/github/release/huggingface/xet-core.svg"></a>
    <a href="https://github.com/huggingface/xet-core/blob/main/CODE_OF_CONDUCT.md"><img alt="Contributor Covenant" src="https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg"></a>
</p>

<h3 align="center">
  <p>🤗 xet-core - xet client tech, used in <a target="_blank" href="https://github.com/huggingface/huggingface_hub/">huggingface_hub</a></p>
</h3>

## Welcome

xet-core enables huggingface_hub to utilize xet storage for uploading and downloading to HF Hub. Xet storage provides chunk-based deduplication, efficient storage/retrieval with local disk caching, and backwards compatibility with Git LFS. This library is not meant to be used directly, and is instead intended to be used from [huggingface_hub](https://pypi.org/project/huggingface-hub).

## Key features

♻ **chunk-based deduplication implementation**: avoid transferring and storing chunks that are shared across binary files (models, datasets, etc).

🤗 **Python bindings**: bindings for [huggingface_hub](https://github.com/huggingface/huggingface_hub/) package.

↔ **network communications**: concurrent communication to HF Hub Xet backend services (CAS).

🔖 **local disk caching**: chunk-based cache that sits alongside the existing [huggingface_hub disk cache](https://huggingface.co/docs/huggingface_hub/guides/manage-cache).

## Packages

This repository produces the following packages:

### Rust Crates (crates.io)

| Crate | Description |
|-------|-------------|
| [`hf-xet`](https://crates.io/crates/hf-xet) | High-level client library for uploading and downloading files with chunk-based deduplication |
| [`xet-client`](https://crates.io/crates/xet-client) | HTTP client for communicating with Hugging Face Xet storage servers |
| [`xet-data`](https://crates.io/crates/xet-data) | Data processing pipeline for chunking, deduplication, and file reconstruction |
| [`xet-core-structures`](https://crates.io/crates/xet-core-structures) | Core data structures including MerkleHash, metadata shards, and Xorb objects |
| [`xet-runtime`](https://crates.io/crates/xet-runtime) | Async runtime, configuration, logging, and utility infrastructure |

### Python Package (PyPI)

| Package | Description |
|---------|-------------|
| [`hf-xet`](https://pypi.org/project/hf-xet/) | Python bindings for the Xet storage system, used by [huggingface_hub](https://github.com/huggingface/huggingface_hub) |

Built from the [`hf_xet/`](./hf_xet) directory using [maturin](https://github.com/PyO3/maturin).

### CLI Binary

| Binary | Description |
|--------|-------------|
| `git-xet` | Git LFS compatible command-line tool for Xet storage |

Built from the [`git_xet/`](./git_xet) directory. Distributed via [GitHub releases](https://github.com/huggingface/xet-core/releases).

## Contributions (feature requests, bugs, etc.) are encouraged & appreciated 💙💚💛💜🧡❤️

Please join us in making xet-core better. We value everyone's contributions. Code is not the only way to help. Answering questions, helping each other, improving documentation, filing issues all help immensely. If you are interested in contributing (please do!), check out the [contribution guide](https://github.com/huggingface/xet-core/blob/main/CONTRIBUTING.md) for this repository.

## Issues, Diagnostics & Debugging

If you encounter an issue with `hf-xet`, please collect diagnostic information
and attach it when creating a [new Issue](https://github.com/huggingface/xet-core/issues/new/choose).

The [`scripts/diag/`](scripts/diag/) directory contains platform-specific scripts
that download debug symbols, configure logging, and capture periodic stack traces
and core dumps:

| OS | Script |
|----|--------|
| Linux | [`scripts/diag/hf-xet-diag-linux.sh`](scripts/diag/hf-xet-diag-linux.sh) |
| macOS | [`scripts/diag/hf-xet-diag-macos.sh`](scripts/diag/hf-xet-diag-macos.sh) |
| Windows (Git-Bash) | [`scripts/diag/hf-xet-diag-windows.sh`](scripts/diag/hf-xet-diag-windows.sh) |

```bash
# prefix your failing command with the script for your OS, e.g.:
./scripts/diag/hf-xet-diag-macos.sh -- python my-script.py
```

See [**scripts/diag/README.md**](scripts/diag/README.md) for full usage, output layout, dump analysis instructions, and how to install debug symbols manually.

Quick debugging environment variables:

```bash
RUST_BACKTRACE=full          # full Rust backtraces on panic
RUST_LOG=info                # enable hf-xet logging
HF_XET_LOG_FILE=/tmp/xet.log # write logs to a file (defaults to stdout)
```

## Local Development

### Repo Organization

* [`xet_pkg/`](./xet_pkg) (`hf-xet`): High-level session API for uploading and downloading files with deduplication.
* [`xet_client/`](./xet_client) (`xet-client`): HTTP client for CAS and Hub backend services.
* [`xet_data/`](./xet_data) (`xet-data`): Chunking, deduplication, and file reconstruction pipeline.
* [`xet_core_structures/`](./xet_core_structures) (`xet-core-structures`): MerkleHash, metadata shards, Xorb objects, and shared data structures.
* [`xet_runtime/`](./xet_runtime) (`xet-runtime`): Async runtime, configuration, logging, and utilities.
* [`hf_xet/`](./hf_xet): Python bindings (maturin/PyO3), produces the `hf-xet` PyPI package.
* [`git_xet/`](./git_xet): Git LFS compatible CLI tool (`git-xet`).
* [`wasm/`](./wasm): WebAssembly builds (`hf_xet_thin_wasm`, `hf_xet_wasm_download`, `hf_xet_wasm_upload`).
* [`simulation/`](./simulation): Simulation and benchmarking infrastructure.

### Build, Test & Benchmark

To build xet-core, look at requirements in [GitHub Actions CI Workflow](.github/workflows/ci.yml) for the Rust toolchain to install. Follow Rust documentation for installing rustup and that version of the toolchain. Use the following steps for building, testing, benchmarking.

Many of us on the team use [VSCode](https://code.visualstudio.com/), so we have checked in some settings in the .vscode directory. Install the rust-analyzer extension.

Build:

```
cargo build
```

Test:

```
cargo test
```

Benchmark:
```
cargo bench
```

Linting:
```
cargo clippy -r --verbose -- -D warnings
```

Formatting (requires nightly toolchain):
```
cargo +nightly fmt --manifest-path ./Cargo.toml --all
```

### Building Python package and running locally (on *nix systems):

1. Create Python3 virtualenv: `python3 -mvenv ~/venv`
2. Activate virtualenv: `source ~/venv/bin/activate`
3. Install maturin: `pip3 install maturin ipython`
4. Go to hf_xet crate: `cd hf_xet`
5. Build: `maturin develop`
6. Test: 
```
ipython
import hf_xet as hfxet
hfxet.upload_files()
hfxet.download_files()
```

#### Developing with tokio console

> Prerequisite is installing tokio-console (`cargo install tokio-console`). See [https://github.com/tokio-rs/console](https://github.com/tokio-rs/console)

To use tokio-console with hf-xet there are compile hf_xet with the following command:
```sh
RUSTFLAGS="--cfg tokio_unstable" maturin develop -r --features tokio-console
```

Then while hf_xet is running (via a `hf` cli command or `huggingface_hub` python code), `tokio-console` will be able to connect.

### Ex.

```bash
# In one terminal:
pip install huggingface_hub
RUSTFLAGS="--cfg tokio_unstable" maturin develop -r --features tokio-console
hf download openai/gpt-oss-20b

# In another terminal
cargo install tokio-console
tokio-console
```

#### Building universal whl for MacOS:

From hf_xet directory:
```
MACOSX_DEPLOYMENT_TARGET=10.9 maturin build --release --target universal2-apple-darwin --features openssl_vendored
```

Note: You may need to install x86_64: `rustup target add x86_64-apple-darwin`

### Testing

Unit-tests are run with `cargo test`, benchmarks are run with `cargo bench`. Some crates have a main.rs that can be run for manual testing.

### WebAssembly compatibility

`xet_pkg` (`hf-xet`), `xet_client`, `xet_data`, `xet_core_structures`, and `xet_runtime` must compile cleanly for `wasm32-unknown-unknown` so that the `wasm/hf_xet_wasm_download` JS-binding crate (and downstream consumers like `hf-hub` on the web) keep working. CI enforces this via `cargo +nightly check --target wasm32-unknown-unknown -p hf-xet` plus the `wasm-pack`-style builds under `wasm/`.

When adding or modifying code in those crates, please keep the wasm build green:

- Prefer `web_time::Instant` over `std::time::Instant` / `tokio::time::Instant` on code paths reachable from wasm (the std and tokio variants panic on wasm32).
- Use `wasm_bindgen_futures::spawn_local` via the `tokio_with_wasm::alias as wasmtokio` shim instead of bare `tokio::spawn` / `JoinSet::spawn`. The browser's reqwest backend produces `!Send` futures.
- Apply the conditional `?Send` pattern to `#[async_trait]` definitions whose methods touch HTTP / I/O:
  ```rust
  #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
  #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
  ```
- Gate filesystem-only code (`std::fs`, `tokio::fs`, `OpenOptions`, `spawn_blocking`, disk caches, file-path download methods) with `#[cfg(not(target_family = "wasm"))]`.

See `wasm/hf_xet_wasm_download/` for the JS-binding crate and `examples/download.html` for a browser-based test.

## References & History

* [Technical Blog posts](https://xethub.com/)
* [Git is for Data 'CIDR paper](https://xethub.com/blog/git-is-for-data-published-in-cidr-2023)
* History: xet-core is adapted from [xet-core](https://github.com/xetdata/xet-core), which contains deep git integration, along with very different backend services implementation.
