# xet-client

[![crates.io](https://img.shields.io/crates/v/xet-client.svg)](https://crates.io/crates/xet-client)
[![docs.rs](https://docs.rs/xet-client/badge.svg)](https://docs.rs/xet-client)
[![License](https://img.shields.io/crates/l/xet-client.svg)](https://github.com/huggingface/xet-core/blob/main/LICENSE)

HTTPS client for communicating with Hugging Face Xet storage servers.

## Overview

- **CAS client** — Upload and download Xorb objects and metadata shards
  to/from the Content-Addressed Storage backend
- **Hub client** — Interact with Hugging Face Hub APIs for repository and
  access-token management
- **Chunk cache** — Local disk cache for Xorb chunks with LRU eviction
- **Retry & resilience** — Automatic retries, connection pooling, and
  concurrent transfers

This crate is part of [xet-core](https://github.com/huggingface/xet-core),
the Rust backend for [huggingface_hub](https://github.com/huggingface/huggingface_hub).

## License

Apache-2.0
