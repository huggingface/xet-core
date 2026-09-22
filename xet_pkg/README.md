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

`logging` (on by default) provides [`init_logging`], which installs the global
`tracing` subscriber along with the console, rolling-file, and JSON sinks.
Consumers that install their own subscriber can turn it off, dropping
`tracing-subscriber`, `tracing-appender`, and their transitive dependencies -
16 crates in total:

```toml
hf-xet = { version = "1", default-features = false, features = ["rustls-tls"] }
```

The spans and events this crate emits are unaffected; only the setup code goes
away. Because `logging` is a default feature, `default-features = false` turns
it off even when it was only used to select a TLS backend, so re-enable it
explicitly if you still want it:

```toml
hf-xet = { version = "1", default-features = false, features = ["native-tls", "logging"] }
```

[`init_logging`]: https://docs.rs/hf-xet/latest/xet/fn.init_logging.html

## License

Apache-2.0
