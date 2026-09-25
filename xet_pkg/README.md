# hf-xet

[![crates.io](https://img.shields.io/crates/v/hf-xet.svg)](https://crates.io/crates/hf-xet)
[![docs.rs](https://docs.rs/hf-xet/badge.svg)](https://docs.rs/hf-xet)
[![License](https://img.shields.io/crates/l/hf-xet.svg)](https://github.com/huggingface/xet-core/blob/main/LICENSE)

Client library for the [Hugging Face Xet](https://github.com/huggingface/xet-core)
data storage system. Provides the high-level session API for uploading and
downloading files with chunk-based deduplication. 

## Overview

- **XetSession** — Top-level session managing authentication, configuration,
  and concurrent file transfers
- **Upload & download** — Stream files to/from Hugging Face Hub with automatic
  chunking, deduplication, and local caching

## Crate Ecosystem

`hf-xet` ties together the lower-level xet-core crates:

| Crate | Role |
|-------|------|
| [`xet-runtime`](https://crates.io/crates/xet-runtime) | Async runtime, config, logging |
| [`xet-core-structures`](https://crates.io/crates/xet-core-structures) | Merkle hashes, shards, Xorb objects |
| [`xet-client`](https://crates.io/crates/xet-client) | HTTP client for CAS and Hub APIs |
| [`xet-data`](https://crates.io/crates/xet-data) | Chunking, dedup, file reconstruction |

This crate is part of [xet-core](https://github.com/huggingface/xet-core),
the Rust backend for [huggingface_hub](https://github.com/huggingface/huggingface_hub).

## Feature flags

`logging` (**off** by default) provides [`init_logging`], which installs the
global `tracing` subscriber: console, rolling-file, and JSON sinks on native
targets, and the browser console on `wasm32-unknown-unknown`. It is off by
default because a library should not choose the subscriber for the binary it
ends up in, and cargo gives a consumer no way to un-enable a default. Turn it on
if you want this crate to set logging up for you:

```toml
hf-xet = { version = "1", features = ["logging"] }
```

Leaving it off keeps `tracing-subscriber`, `tracing-appender`, and their
transitive dependencies out of the build - 16 crates in total. The spans and
events this crate emits are unaffected either way; only the setup code goes
away, so your own subscriber still sees everything.

[`init_logging`]: https://docs.rs/hf-xet/latest/xet/fn.init_logging.html

## License

Apache-2.0
