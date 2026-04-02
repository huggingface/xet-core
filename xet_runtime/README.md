# xet-runtime

[![crates.io](https://img.shields.io/crates/v/xet-runtime.svg)](https://crates.io/crates/xet-runtime)
[![docs.rs](https://docs.rs/xet-runtime/badge.svg)](https://docs.rs/xet-runtime)
[![License](https://img.shields.io/crates/l/xet-runtime.svg)](https://github.com/huggingface/xet-core/blob/main/LICENSE)

Async runtime, configuration, logging, and utility infrastructure for the
[Hugging Face Xet](https://github.com/huggingface/xet-core) storage tools.

## Overview

`xet-runtime` provides the shared foundation used by all crates in the
xet-core ecosystem:

- **Async runtime** — Tokio-based runtime with configurable thread pools
- **Configuration** — Hierarchical configuration for Xet clients
- **Structured logging** — Tracing-based logging with file and console outputs
- **Error handling** — `RuntimeError` type for the runtime layer
- **Utilities** — File operations, sync primitives, and platform abstractions

This crate is part of [xet-core](https://github.com/huggingface/xet-core),
the Rust backend for [huggingface_hub](https://github.com/huggingface/huggingface_hub).

## License

Apache-2.0
