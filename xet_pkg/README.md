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

| Feature | Default | Description |
|---------|---------|-------------|
| `upload` | ✓ | Upload functionality: upload commits, chunking/deduplication, and the CAS shard/xorb upload pipeline |
| `rustls-tls` | ✓ | TLS via rustls (see below for the native-TLS alternative) |

Consumers that only **download** can opt out of the upload pipeline to shed
the chunking/deduplication code and its dependencies:

```toml
hf-xet = { version = "1", default-features = false, features = ["rustls-tls"] }
```

In that configuration the download API (`XetSession::new_file_download_group`,
`XetSession::new_download_stream_group`, and the `legacy` download helpers) is
unaffected, while all upload API (`XetSession::new_upload_commit`,
`XetUploadCommit`, upload helpers in `legacy`) is not compiled in. Note that
the `local://` and `memory://` CAS endpoints (backed by the simulation
clients) also require the `upload` feature.

To use the platform-native TLS stack instead of rustls, depend on this crate
with `default-features = false` and enable `native-tls` (or
`native-tls-vendored`); see the `[features]` section of `Cargo.toml` for the
full list (including `python`, `tokio-console`, and `fd-track`).

## License

Apache-2.0
