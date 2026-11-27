# Chunk layout of a reconstruction range

**Date**: 2026-10-26
**Crates**: `xet-client` (`cas_types`, `Client` trait)

## What changed

New `Client::get_chunk_layout(file_id, bytes_range, serialized)`, for the CAS endpoint
`GET /v2/chunk-layout/{file_id}` (xetcas#1414). For a byte range of at most 1 GiB, it returns the
terms that `get_reconstruction` returns for the same range, in the same order, with the decoded
size of each chunk and, when `serialized` is true, its size in the xorb. A client that holds the
plan of a range can then derive the exact plan of any sub-range locally.

New types in `cas_types`: `ChunkLayoutResponse` and `TermChunkLayout`.

Like `get_reconstruction`, the call returns `Ok(None)` when the range starts past the end of the
file. A CAS that does not serve the endpoint answers 404, returned as an error without retries.

## Migration

`Client` gains a required method. Implementations outside this repository must add it; a wrapper
forwards it to its inner client.
