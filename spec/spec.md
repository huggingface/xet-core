# Xet Protocol Specification

> Version 0.1.0 (1.0.0 on release)  
> The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 [RFC2119](https://www.ietf.org/rfc/rfc2119.txt) [RFC8174](https://www.ietf.org/rfc/rfc8174.txt)
when, and only when, they appear in all capitals, as shown here.

This specification defines the end-to-end Xet protocol for content-addressed data: chunking and hashing rules, deduplication strategy, xorb and shard object formats, file reconstruction semantics, authentication, and the CAS APIs for upload and download.
Its goal is interoperability and determinism: independent implementations MUST produce the same hashes, objects, and API behavior so data written by one client can be read by another with integrity and performance.
Implementors can create their own clients, SDKs, and tools that speak the Xet protocol and interface with the CAS service, as long as they MUST adhere to the requirements defined here.

## Building a client library for xet storage

- [Upload Protocol](../spec/upload_protocol.md): End-to-end top level description of the upload flow.
- [Download Protocol](../spec/download_protocol.md): Instructions for the download procedure.
- [CAS API](../spec/api.md): HTTP endpoints for reconstruction, global chunk dedupe, xorb upload, and shard upload, including error semantics.
- [Authentication and Authorization](../spec/auth.md): How to obtain Xet tokens from the Hugging Face Hub, token scopes, and security considerations.
- [Hugging Face Hub Files Conversion to Xet File ID's](../spec/file_id.md): How to obtain a Xet file id from the Hugging Face Hub for a particular file in a model or dataset repository.

## Overall Xet architecture

- [Content-Defined Chunking](../spec/chunking.md): Gearhash-based CDC with parameters, boundary rules, and performance optimizations.
- [Hashing Methods](../spec/hashing.md): Descriptions and definitions of the different hashing functions used for chunks, xorbs and term verification entries.
- [File Reconstruction](../spec/file_reconstruction.md): Defining "term"-based representation of files using xorb hash + chunk ranges.
- [Xorb Format](../spec/xorb.md): Explains grouping chunks into xorbs, 64 MiB limits, binary layout, and compression schemes.
- [Shard Format](../spec/shard.md): Binary shard structure (header, file info, CAS info, footer), offsets, HMAC key usage, and bookends.
- [Deduplication](../spec/deduplication.md): Explanation of chunk level dedupe including global system-wide chunk level dedupe.

## Reference implementation

The primary reference implementation of the protocol written in rust 🦀 lives in the [xet-core](https://github.com/huggingface/xet-core) repository under multiple crates:

- [cas_types](https://github.com/huggingface/xet-core/tree/main/cas_types) - Common re-usable types for interacting with CAS API's
- [cas_client](https://github.com/huggingface/xet-core/tree/main/cas_client) - Client interface that calls CAS API's, including comprehensive implementation of download protocol.
- [mdb_shard](https://github.com/huggingface/xet-core/tree/main/mdb_shard) - Library for interacting with shards and the shard binary format.
- [deduplication](https://github.com/huggingface/xet-core/tree/main/deduplication) - Exposes interfaces to deduplicate chunks locally and using global deduplication
  - [deduplication/src/chunking.rs](https://github.com/huggingface/xet-core/blob/main/deduplication/src/chunking.rs) - The reference implementation of the chunking algorithm.
- [merklehash](https://github.com/huggingface/xet-core/tree/main/merklehash) - Exports a `MerkleHash` type extensively used to represent hashes. Exports functions to compute the different hashes used to track chunks, xorbs and files.
- [data](https://github.com/huggingface/xet-core/tree/main/data) - Comprehensive package exposing interfaces to upload and download contents
- [hf_xet](https://github.com/huggingface/xet-core/tree/main/hf_xet) - Python bindings to use the Xet protocol for uploads and downloads with the Hugging Face Hub.
