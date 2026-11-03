# xet-core-structures

[![crates.io](https://img.shields.io/crates/v/xet-core-structures.svg)](https://crates.io/crates/xet-core-structures)
[![docs.rs](https://docs.rs/xet-core-structures/badge.svg)](https://docs.rs/xet-core-structures)
[![License](https://img.shields.io/crates/l/xet-core-structures.svg)](https://github.com/huggingface/xet-core/blob/main/LICENSE)

Core data structures for the
[Hugging Face Xet](https://github.com/huggingface/xet-core) storage system,
including Merkle hashes, metadata shards, and Xorb objects.

## Overview

- **MerkleHash** — 256-bit content-addressed hash used throughout the system
- **Metadata shards** — Compact shard format mapping file ranges to Xorb chunks
- **Xorb objects** — Content-addressed storage objects with byte-grouping compression
- **Data structures** — Specialized hash maps and utilities for deduplication

This crate is part of [xet-core](https://github.com/huggingface/xet-core),
the Rust backend for [huggingface_hub](https://github.com/huggingface/huggingface_hub).

## License

Apache-2.0
