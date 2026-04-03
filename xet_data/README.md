# xet-data

[![crates.io](https://img.shields.io/crates/v/xet-data.svg)](https://crates.io/crates/xet-data)
[![docs.rs](https://docs.rs/xet-data/badge.svg)](https://docs.rs/xet-data)
[![License](https://img.shields.io/crates/l/xet-data.svg)](https://github.com/huggingface/xet-core/blob/main/LICENSE)

Data processing pipeline for chunking, deduplication, and file reconstruction.  Intended to be used through the API in the hf-xet package. 

## Overview

- **Content-defined chunking** — Gear-hash based chunking for deduplication
- **Deduplication** — Probe and register chunks against metadata shards
- **File reconstruction** — Reassemble files from deduplicated chunk references
- **Progress tracking** — Hooks for upload/download progress reporting

This crate is part of [xet-core](https://github.com/huggingface/xet-core).

## License

Apache-2.0
