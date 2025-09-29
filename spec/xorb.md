# Xorb Formation & Serialization Format

A "Xorb" (Xet Orb, pronounced like "zorb") is a sequence of chunks and a serialization format for a series of chunks.

## Collecting Chunks

Using the chunking algorithm a file is mapped to a series of chunks, once those chunks are found, they need to be collected into collections of Xorbs.

It is advantageous to collect series of chunks in Xorbs such that they can be referred to as a whole range of chunks.

Suppose a file is chunked into chunks A, B, C, D in the order ABCD. Then create a Xorb X1 with chunks A, B, C, D in this order (starting at chunk index 0), let's say this Xorb's hash is X1. Then to reconstruct the file we ask for Xorb X1 chunk range `[0, 4)`.

While there's no explicit limit on the number of chunks in a Xorb, there is a limit of 64MiB on the total size of the Xorb as serialized.
Since some chunks will get compressed, it is generally advised to collect chunks until their total uncompressed length is near 64 MiB then serialize the struct.
Namely, Xorbs point to roughly 64 MiB worth of data.
(Recall that the target chunk size is 64 KiB so expect roughly ~1024 chunks per Xorb).

The CAS server will reject Xorb uploads that exceed the 64 MiB serialized size limit.

It is RECOMMENDED to pack chunks from multiple files into a Xorb if the size requirements allow, i.e. file X and Y both produced 10 new chunks each totalling a total of ~128000 bytes, then all those chunks can fit in a new Xorb.

## Xorb Format

A Xorb is a series of "Chunks" that is serialized according to a specific format that enables accessing chunks of ranges and builds in chunk level compression.

```txt
┌─────────┬─────────────────────────────────┬─────────┬─────────────────────────────────┬─────────┬─────────────────────────────────┬──────────
│  Chunk  │                                 │  Chunk  │                                 │  Chunk  │                                 │
│  Header │      Compressed Chunk Data      │  Header │      Compressed Chunk Data      │  Header │      Compressed Chunk Data      │   ...
│         │                                 │         │                                 │         │                                 │
└─────────┴─────────────────────────────────┴─────────┴─────────────────────────────────┴─────────┴─────────────────────────────────┴───────────
│                 Chunk 0                   │                 Chunk 1                   │                 Chunk 2                   │   ...
```

### Chunk Addressing

Each chunk has an index within the Xorb it is in, starting at 0.
Chunks can be addressed individually by their index but are usually addressed or fetched in range.
Chunk ranges are always specified start inclusive and end exclusive i.e. `[start, end)`.

## Chunk Format

A chunk consists of a header followed by compressed data. The header contains metadata about the chunk, particularly the compression scheme required to know how to deserialize the chunk.

### Chunk Header Structure

The chunk header is serialized as follows:

- **Version** (1 byte): Protocol version, currently `0`
- **Compressed Size** (3 bytes): Size of data after compression as a 3 byte little-endian unsigned integer.
- **Compression Type** (1 byte): Algorithm used for compression (See mapping below)
- **Uncompressed Size** (3 bytes): Size of raw chunk data (before compression) as a 3 byte little-endian unsigned integer.

Both Compressed and Uncompressed Size can fit in a 3 byte integer, given that that a raw uncompressed chunk can be 128KiB at most,
requiring 18 binary digits to represent.
If utilizing the intended compression scheme results in a larger compressed chunk then the chunk SHOULD be stored uncompressed with then
the uncompressed size also being at a maximum of 128KiB.

#### Chunk Header Layout

```txt
┌─────────┬─────────────────────────────────┬──────────────┬─────────────────────────────────┐
│ Version │        Compressed Size          │ Compression  │       Uncompressed Size         │
│ 1 byte  │           3 bytes               │    Type      │           3 bytes               │
│         │      (little-endian)            │   1 byte     │      (little-endian)            │
└─────────┴─────────────────────────────────┴──────────────┴─────────────────────────────────┘
0         1                                 4              5                                 8
```

### Chunk Compression Schemes

| Value | Name | Description |
|-------|------|-------------|
| `0` | `None` | No compression - data is stored as-is |
| `1` | `LZ4` | Standard LZ4 compression |
| `2` | `ByteGrouping4LZ4` | Byte grouping with 4-byte groups followed by LZ4 compression. Optimized for floating-point and other structured data where grouping bytes by position improves compression ratios |

#### Byte Grouping LZ4 Compression

Byte grouping LZ4 compression is an optimization technique that improves compression ratios for structured data like floating-point numbers, integers, and other data types where values have similar byte patterns at specific positions.

1. **Byte Grouping Phase**: The input data is reorganized by grouping bytes by their position within each 4-byte groups:
   Create 4 buffers, for each 4 bytes of the chunk data (B1, B2, B3, B4) append each byte to their respective group i.e. in order from 1 to 4. Then concatenate the groups in order (1, 2, 3, 4).

   Example:

   - Original data: `[A1, A2, A3, A4, B1, B2, B3, B4, C1, C2, C3, C4, ...]`
   - Grouped data: `[A1, B1, C1, ..., A2, B2, C2, ..., A3, B3, C3, ..., A4, B4, C4, ...]`

   If the total number of bytes in the chunk is not a multiple of 4, append the remaining bytes following the pattern (1 byte to each group) to the first 1-3 groups until there are no more bytes left in the chunk.

2. **LZ4 Compression**: The grouped data is then compressed using standard LZ4 compression.

#### Chunk Data

Following the header is the compressed data block, exactly `compressed_size` bytes long.

### Picking a Compression Scheme

Picking the chunk compression scheme for the Xorb is a task left to the client when uploading the Xorb.
The goal is to minimize the overall size of the Xorb for faster transmission at the cost of resources to decompress a chunk on the receiving end.

When picking a compression scheme for the chunk there are a number of strategies and implementors MAY make their decisions as to how to pick a compression scheme.
Note that a Xorb MAY contain chunks that utilize different compression schemes.

1. **Brute Force**

    Try all possible compression schemes, pick the best one.
    The best one MAY be the one producing the smallest compressed chunk or the fastest to decompress.

2. **Best Effort Prediction**

    In `xet-core`, to predict if BG4 will be useful we maximum KL divergence between the distribution of per-byte pop-counts on a sample of each of the 4 groups that would be formed.
    You can read more about it in [bg4_prediction.rs](../cas_object/src/byte_grouping/bg4_prediction.rs) and accompanying scripts.

    If the predictor does not show that BG4 will be better, we use Lz4 and in either case we will store the chunk as the uncompressed version if the compression scheme used does not show any benefit.

#### Example Chunk Serialization

```python
VERSION = 0
buffer = bytes()

for chunk in xorb.chunks:
    uncompressed_length = len(chunk)
    compressed, compression_scheme = pick_compression_scheme_and_compress(chunk)
    header = Header(VERSION, len(compressed), compression_scheme, uncompressed_length)
    buffer.write(header)
    buffer.write(compressed)
```

## Xorb Format Sample

For a sample of a serialized xorb object see [eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb).
