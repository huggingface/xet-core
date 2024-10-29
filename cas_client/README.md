# CAS client

This package is responsible for handling all communication with the CAS services.

## Layout

Check out the traits published by this crate to understand how it is intended to be used. These are stored in [src/interface.rs].

### Main impl of Client trait

- [src/remote_client.rs]: This is the main impl of Client - and is responsible for communicating with a remote CAS.
- [src/local_client.rs]: This is an impl of Client for local filesystem usage. It is currently only used for testing.

### Caching

Caching happens locally using the [chunk_cache](../chunk_cache) crate, specifically using the ChunkCache. When RemoteClient is provided a ChunkCache then it will use this on download calls (the ReconstructionClient trait `get_file` and `get_file_byte_range`).

## Overall CAS Communication Design

### Authentication

Authentication is done using AuthMiddleware, which sets an Authorization Header and refreshes it periodically with CAS. See [src/http_client.rs].

### Retry

HTTP operations are retried using a RetryPolicy defined in [src/http_client.rs]. This is implemented as Middleware for the reqwest HTTP clients.

### Operations

CAS offers a set of services used by the client to upload and download user files. These files are stored using two different
types of storage objects, Xorbs and Shards. Xorbs contain chunks and Shards contain mappings of files to Xorb chunks.

### Logging / Tracing

Logging & Tracing is done through the tracing crate, with info!, warn!, and debug! macros widely used in the code.