# xet-client

[![crates.io](https://img.shields.io/crates/v/xet-client.svg)](https://crates.io/crates/xet-client)
[![docs.rs](https://docs.rs/xet-client/badge.svg)](https://docs.rs/xet-client)
[![License](https://img.shields.io/crates/l/xet-client.svg)](https://github.com/huggingface/xet-core/blob/main/LICENSE)

Client for communicating with Hugging Face Xet storage servers.

## Overview

Upload and download data and metadata objects from the backend Hugging Face Xet storage servers.  Features automatic concurrency adaptations, connection pooling, and retry resiliency.  Intended to be used through the API in the hf-xet package.

This crate is part of [xet-core](https://github.com/huggingface/xet-core).

## License

Apache-2.0
