# File Reconstruction: Term-based Representation

This document describes how a file can be represented and reconstructed from a compact, deduplicated form using a series of terms. Each term specifies where to source data (a content-addressed container called a xorb) and which chunk indices within that container are required.

## Glossary

- **Xorb**: A content-addressed container holding a sequence of chunks. It is identified by a cryptographic hash (the “xorb hash”).
- **Chunk**: A unit of data inside a xorb. Chunks are ordered and individually decompressible/decodable into raw bytes.
- **Chunk index**: The 0-based position of a chunk within a particular xorb.
- **Term**: A pair that identifies a xorb and a contiguous chunk index range within that xorb: `(xorb hash, chunk range [start, end))`.
- **Reconstruction**: A list of terms.

## Core Idea

After following the [chunking procedure](../spec/chunking.md) a file can be represented as an ordering of chunks.
Those chunks are then packed into [xorbs](../spec/xorb_formation.md) and given the set of xorbs we convert the file representation to "reconstruction" made up of "terms".
When forming xorbs the ordering and grouping of chunks prioritizes contiguous runs of chunks that appear in a file such that when referencing a xorb we maximize the term range length.

Any file’s raw bytes can be described as the concatenation of data produced by a sequence of terms.
Each term references a contiguous range of chunks within a particular xorb.
The file is reconstructed by retrieving those chunk ranges, decoding them to raw bytes, and concatenating in order.

## Term Format

Each term consists of:

- **Xorb hash**: A 32 byte hash value that is the key to the xorb in the CAS (Content Addressed Store).
- **Chunk range**: A half-open interval [start, end) of chunk indices within that xorb. The range includes the chunk at index `start` and excludes the chunk at index `end`.

## Reconstruction Rules

Given an ordered list of terms describing a file:

1. For each term, fetch the specified chunk range from the identified xorb.
2. Decode/decompress the chunks into raw bytes, preserving the original order.
3. If reconstructing the entire file, concatenate the decoded outputs of all terms in their listed order.
4. If reconstructing a byte sub-range of the file, the first and last terms may be partially used:
   - Skip a prefix of bytes within the first term’s decoded output so the file-level range starts at the requested offset.
   - Truncate the tail of the last term’s decoded output so the file-level range ends at the requested position.

### Ordering and Coverage

- Terms are ordered according to the file’s byte order. Concatenating their decoded outputs yields the requested file region.
- Gaps are not allowed. If gaps exist, the reconstruction would not produce a contiguous byte stream.

### Multiple Terms per Xorb and Coalescing

- A file may contain multiple terms that reference the same xorb, potentially with disjoint chunk ranges. This enables deduplication across distant parts of the file.
- When multiple terms target overlapping or adjacent chunk ranges within the same xorb, implementations may coalesce these into a single retrieval to reduce I/O and request overhead, while preserving the term-level reconstruction semantics.

### Chunk and Byte Boundaries

- Chunk ranges are specified in chunk index space, not byte offsets. The decoded size of each chunk is not required to be uniform.
- The byte length contributed by a term equals the sum of the decoded sizes of its referenced chunks, minus any initial skip or final truncation when reconstructing sub-ranges.
- Slicing within a term (for sub-range reconstruction) happens at byte granularity in the decoded output of the addressed chunk sequence.

### Determinism and Integrity

- The xorb hash binds the identity of the underlying chunk set to its content. If the xorb hash is correct and the specified chunk range is retrieved and decoded as defined, the resulting bytes are deterministic.
- A file-level identity (e.g., a cryptographic hash of the reconstructed bytes) can be validated by reconstructing and hashing the result.

### Example (Conceptual)

Assume a file is represented by the following ordered terms:

| Term | Xorb hash (conceptual) | Chunk range |
|------|-------------------------|-------------|
| 1    | X1                      | [0, 5)      |
| 2    | X2                      | [3, 8)      |
| 3    | X1                      | [9, 12)     |

Reconstruction proceeds by obtaining chunks 0,1,2,3,4 from xorb X1, chunks 3,4,5,6,7 from xorb X2, and chunks 9,10,11 from xorb X1, decoding each contiguous range, and concatenating in the term order 1 → 2 → 3.

## Serialization and Deserialization

This section summarizes how the term-based reconstruction is persisted and exchanged.

### Serialization into shards (file info section)

A file’s reconstruction can be serialized into a shard as part of its file info section.
Conceptually, this section encodes the complete set of terms that describe the file.
When stored this way, the representation is canonical and sufficient to reconstruct the full file solely from its referenced xorb ranges.

Reference: [shard format file info](../spec/shard.md#2-file-info-section)

### Deserialization from the reconstruction API (JSON)

A reconstruction API can return a JSON object that carries the full reconstruction.
This response is represented by a structure named “QueryReconstructionResponse”, where the `terms` key enumerates the ordered list of terms required to reconstruct the entire file.
The `terms` list contains, for each term, the xorb identifier and the contiguous chunk index range to retrieve.
Other fields may provide auxiliary details (such as offsets or fetch hints) that optimize retrieval without altering the meaning of the `terms` sequence.

Reference: [api.md](../spec/api.md), [download protocol](../spec/download_protocol.md)

## Fragmentation and Why Longer Ranges Matter

Fragmentation refers to representing a file with many very short, scattered ranges across many xorbs. While this can maximize deduplication opportunities, it often harms read performance and increases overhead.

- Meaning of fragmentation: The file’s byte stream is assembled from numerous small term ranges, potentially spanning many xorbs. This implies more lookups, more range fetches, and poorer locality.
- Costs of fragmentation:
  - Larger reconstruction objects
  - Potentially more network requests (overhead per request)
- Why prioritize longer ranges:
  - Fewer, longer ranges reduce round-trips and enable more sequential reads
  - Simpler scheduling and write patterns during reconstruction

In practice there is a balance: longer ranges improve reconstruction performance, while finer granularity can increase deduplication savings.
Favoring longer contiguous chunk ranges within the same xorb, and coalescing adjacent or overlapping ranges when feasible, helps maintain good read performance without sacrificing correctness.
In `xet-core` we use a fragmentation prevention mechanism that targets that the average term contains 8 chunks.
