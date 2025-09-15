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

## Contributions (feature requests, bugs, etc.) are encouraged & appreciated 💙💚💛💜🧡❤️

Please join us in making xet-core better. We value everyone's contributions. Code is not the only way to help. Answering questions, helping each other, improving documentation, filing issues all help immensely. If you are interested in contributing (please do!), check out the [contribution guide](https://github.com/huggingface/xet-core/blob/main/CONTRIBUTING.md) for this repository.

## Issues, Diagnostics & Debugging

If you encounter an issue when using `hf-xet` please help us fix the issue by collecting diagnostic information and attaching that when creating a [new Issue](https://github.com/huggingface/xet-core/issues/new/choose). Download the [hf-xet-diag-linux.sh](hf-xet-diag-linux.sh) or [hf-xet-diag-windows.sh](hf-xet-diag-windows.sh) script based on your operating system and then re-run the python command that resulted in the issue. The diagnostic scripts will download and install debug symbols, setup up logging, and take periodic stack traces throughout process execution in a diagnostics directory that is easy to analyze, package, and upload.

### Diagnostics - Linux (`hf-xet-diag-linux.sh`)

* Uses `gdb` + `gcore` to periodically snapshot stacks and produce core dumps.
* Supports optional ptrace preload helper for debugging.
* Downloads and installs the appropriate `hf_xet-*.dbg` symbol file automatically.

**Requirements:**

```bash
sudo apt-get install gdb build-essential
```

**Example usage:**

```bash
./hf-xet-diag-linux.sh -- python hf-download.py "Qwen/Qwen2.5-VL-3B-Instruct"
```

### Windows (Git-Bash) (`hf-xet-diag-windows.sh`)

* Runs in **Git-Bash**, keeping usage consistent with Linux.
* Uses **Sysinternals ProcDump** for periodic mini dumps (`-mp`).
* Auto-downloads `procdump.exe` if not found.
* Downloads and installs the matching `hf_xet.pdb` debug symbol into the package directory.

**Requirements:**

* Git-Bash (from [Git for Windows](https://gitforwindows.org/))
* Python installed
* Internet access (first run downloads ProcDump and debug symbols)

**Example usage:**

```bash
./hf-xet-diag-windows.sh -- python hf-download.py "Qwen/Qwen2.5-VL-3B-Instruct"
```

---

### Output Layout

Both scripts produce a diagnostics directory named:

```
diag_<command>_<timestamp>/
  ├── console.log   # Combined stdout/stderr of the process
  ├── env.log       # System/environment info
  ├── pid           # Child PID file
  ├── stacks/       # Periodic stack traces / dumps
  └── dumps/        # (Linux only) full gcore dumps
```

This unified layout makes it easier to compare diagnostics across platforms.

---

### Analyzing Dumps

### Usage

From your repo root:

```bash
./analyze-latest.sh
```

* Finds the most recent `diag_*` directory.
* Opens the latest dump inside:

  * **Linux:** opens `dumps/core_*` in `gdb`.
  * **Windows (Git-Bash):** opens `stacks/*.dmp` in **WinDbg** (`windbg` must be on PATH).
* You can also pass a base directory if your diagnostics are stored elsewhere:

  ```bash
  ./analyze-latest.sh /path/to/diagnostics
  ```

**Linux**

* Stack traces are saved under `stacks/` as plain text.
* Core dumps (`dumps/core_*`) can be analyzed with gdb:

  ```bash
  gdb python dumps/core_<pid>
  (gdb) bt        # backtrace
  (gdb) thread apply all bt
  ```
* Ensure the matching debug symbols (`hf_xet-*.dbg`) are in the `hf_xet` package directory.

**Windows**

* Dumps are saved under `stacks/` as `.dmp` files.
* Open `.dmp` files in **WinDbg** (install via [Windows SDK](https://developer.microsoft.com/en-us/windows/downloads/windows-10-sdk/)):

  ```cmd
  windbg -z dump_20250101_120000.dmp
  ```
* Common WinDbg commands:

  ```
  !analyze -v         # Automatic analysis
  ~* kb               # Show stack traces for all threads
  lm                  # List loaded modules (verify hf_xet.pdb loaded)
  ```
* Ensure `hf_xet.pdb` is installed in the `hf_xet` package directory so symbols load correctly.

---

⚠️ **Tip:** Share the full `diag_<command>_<timestamp>/` directory when reporting issues — it contains logs, environment info, and dumps needed to reproduce and diagnose problems.


### Debugging

To limit the size our our built binaries, we are releasing python wheels with binaries that are stripped of debugging symbols. If you encounter a panic while running hf-xet, you can use the debug symbols to help identify the part of the library that failed. 

Here are the recommended steps:

1. Download and unzip our [debug symbols package](https://github.com/huggingface/xet-core/releases/download/latest/dbg-symbols.zip).
2. Determine the location of the hf-xet package using `pip show hf-xet`. The `Location` field will show the location of all the site packages. The `hf_xet` package will be within that directory.
3. Determine the symbols to copy based on the system you are running:
   * Windows: use `hf_xet.pdb`
   * Mac: use `libhf_xet-macosx-x86_64.dylib.dSYM` for Intel based Macs and `libhf_xet-macosx-aarch64.dylib.dSYM` for Apple Silicon.
   * Linux: the choice will depend on the architecture and wheel distribution used. To get this information, `cat` the `WHEEL` file name within the `hf_xet.dist-info` directory in your site packages. The wheel file will have the linux build and architecture in the file name. Eg: `cat /home/ubuntu/.venv/lib/python3.12/site-packages/hf_xet-*.dist-info/WHEEL`. You will use the file named `hf_xet-<manylinux | musllinux>-<x86_64 | arm64>.abi3.so.dbg` choosing the distribution and platform that matches your wheel. Eg: `hf_xet-manylinux-x86_64.abi3.so.dbg`.
4. Copy the symbols to the site package path from step 2 above + `hf_xet`. Eg: `cp -r hf_xet-1.1.2-manylinux-x86_64.abi3.so.dbg /home/ubuntu/.venv/lib/python3.12/site-packages/hf_xet`
5. Run your python binary with `RUST_BACKTRACE=full` and recreate your failure.

#### Debugging Environment Variables

To enable logging and see more debugging / diagnostics information, set the following:

```
RUST_BACKTRACE=full
RUST_LOG=info
HF_XET_LOG_FILE=/tmp/xet.log
```

Note: HF_XET_LOG_FILE expects a full writable path. If one isn't found it will use stdout console for logging.

## Local Development

### Repo Organization - Rust Crates

* [cas_client](./cas_client): communication with CAS backend services, which include APIs for Xorbs and Shards.
* [cas_object](./cas_object): CAS object (Xorb) format and associated APIs, including chunks (ranges within Xorbs).
* [cas_types](./cas_types): common types shared across crates in xet-core and xetcas.
* [chunk_cache](./chunk_cache): local disk cache of Xorb chunks.
* [chunk_cache_bench](./chunk_cache_bench): benchmarking crate for chunk_cache.
* [data](./data): main driver for client operations - FilePointerTranslator drives hydrating or shrinking files, chunking + deduplication here.
* [error_printer](./error_printer): utility for printing errors conveniently.
* [file_utils](./file_utils): SafeFileCreator utility, used by chunk_cache.
* [hf_xet](./hf_xet): Python integration with Rust code, uses maturin to build `hf-xet` Python package. Main integration with HF Hub Python package.
* [mdb_shard](./mdb_shard): Shard operations, including Shard format, dedupe probing, benchmarks, and utilities.
* [merklehash](./merklehash): MerkleHash type, 256-bit hash, widely used across many crates.
* [progress_reporting](./progress_reporting): offers ReportedWriter so progress for Writer operations can be displayed.
* [utils](./utils): general utilities, including singleflight, progress, serialization_utils and threadpool.

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

## References & History

* [Technical Blog posts](https://xethub.com/)
* [Git is for Data 'CIDR paper](https://xethub.com/blog/git-is-for-data-published-in-cidr-2023)
* History: xet-core is adapted from [xet-core](https://github.com/xetdata/xet-core), which contains deep git integration, along with very different backend services implementation.
