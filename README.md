# Xet-Core

## Purpose

Xet-core is the repo responsible for Rust-based code running on the client machine. This includes the deduplication/chunking implementation, network communications to backend services (ex. CAS), and local disk caching. There are some crates in this repo that are shared with [xetcas](https://github.com/huggingface-internal/xetcas).

## Included Crates

* [cas_client](src/cas_client): communication with CAS backend services, which include APIs for Xorbs and Shards.
* [cas_object](src/cas_object): CAS object (Xorb) format and associated APIs, including chunks (ranges within Xorbs).
* [cas_types](src/cas_types): common types shared across crates in xet-core and xetcas.
* [chunk_cache](src/chunk_cache): local disk cache of Xorb chunks.
* [chunk_cache_bench](src/chunk_cache_bench): benchmarking crate for chunk_cache.
* [data](src/data): main driver for client operations - FilePointerTranslator drives hydrating or shrinking files.
* [error_printer](src/error_printer): utility for printing errors conveniently.
* [file_utils](src/file_utils): SafeFileCreator utility, used by chunk_cache.
* [hf_xet](src/hf_xet): Python integration with Rust code, uses maturin to build hfxet Python package. Main integration with HF Hub Python package.
* [mdb_shard](src/mdb_shard): Shard operations, including Shard format, dedupe probing, benchmarks, and utilities.
* [merkledb](src/merkdledb): Chunking + deduplication implementation. Scanning files, building chunks, organizing them, etc.
* [merklehash](src/merklehash): DataHash type, 256-bit hash, widely used across many crates.
* [parutils](src/parutils): Provides parallel execution utilities relying on Tokio (ex. parallel foreach).
* [progress_reporting](src/progress_reporting): offers ReportedWriter so progress for Writer operations can be displayed.
* [utils](src/utils): general utilities - **unclear how much is currently in use**
* [xet_error](src/xet_error): Error utility crate, widely used for anyhow! logging in other crates.

## Local Development

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

### Building Python package and running locally:

1. Create Python3 VirtualEnv: `python3 -mvenv ~/venv`
2. Activate virtualenv: `source ~/venv/bin/activate`
3. Install maturin: `pip3 install maturin ipython`
4. Go to hf_xet crate: `cd hf_xet`
5. Build: `maturin develop`
6. Test: 
```
ipython
import hfxet 
hfxet.upload_files()
hfxet.download_files()
```

## Testing

Unit-tests are run with `cargo test`, benchmarks are run with `cargo bench`. Some crates have a main.rs that can be run for manual testing.

## References

* [HF+Xet Storage Architecture](https://www.notion.so/huggingface2/Introduction-To-XetHub-Storage-Architecture-And-The-Integration-Path-54c3d14c682c4e41beab2364f273fc35?pvs=4)
* [Chunk Cache Design](https://www.notion.so/huggingface2/Client-Data-Cache-10b1384ebcac80b59516d5ae258a17a7?pvs=4)

### Historical Design Documents
* [Shard Server Design](https://www.notion.so/huggingface2/MerkleDBv2-Xet-CLI-Shard-Server-b634a45c35fc4832ae5ac477b99f50de?pvs=4)
* [MerkleDB v2 Design](https://www.notion.so/huggingface2/MerkleDB-v2-1130ea85f14d49dc8de8ebef64cc33dd?pvs=4)
* [Git is for Data 'CIDR paper](https://xethub.com/blog/git-is-for-data-published-in-cidr-2023)

## Repo History

A trimmed version of [xet-core](https://github.com/xetdata/xet-core). The xetdata/xet-core repo contains deep git-integration, along with very different backend services implementation.