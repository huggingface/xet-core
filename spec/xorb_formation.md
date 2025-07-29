# Xorb Formation

## Collecting Chunks

Using the chunking algorithm a file is mapped to a series of chunks, once those chunks are found, they need to be collected into collections of chunks called each called a "Xorb" (Xet Orb, pronounced like "zorb").

It is advantageous to collect series of chunks in xorbs such that they can be referred to as a whole range of chunks.

Suppose a file is chunked into chunks A, B, C, D in the order ABCD. Then create a xorb X1 with chunks A, B, C, D in this order (starting at chunk index 0), let's say this xorb's hash is X1. Then to reconstruct the file we ask for xorb X1 chunk range `[0, 4)`.

While there's no explicit limit on the number of chunks in a xorb, there is a limit of 64MiB on the total size of the xorb as serialized. Since some chunks will get compressed, it is generally advised to collect chunks until their total uncompressed length is near 64 MiB then serialize the struct. Namely, xorbs point to roughly 64 MiB worth of data. (Recall that the target chunk size is 64 KiB so expect roughly ~1024 chunks per xorb).

The CAS server will reject xorb uploads that exceed the 64 MiB serialized size limit.

## Xorb Format

A xorb is a series of "Chunks" that is serialized according to a specific format that enables accessing chunks of ranges and builds in chunk level compression.

### Chunk Addressing

Each chunk has an index within the xorb it is in, starting at 0. Chunks can be addressed individually by their index but are usually addressed or fetched in range. Chunk ranges are always specified start inclusive and end exclusive i.e. `[start, end)`.

### Chunk Format

A chunk consists of a header followed by compressed data. The header contains metadata about the chunk, particularly the compression scheme required to know how to deserialize the chunk.

#### Chunk Header Structure

The chunk header is serialized as follows:

- **Version** (1 byte): Protocol version, currently `0`
- **Compressed Size** (3 bytes): Size of data after compression as a 3 byte little-endian unsigned integer.
- **Compression Type** (1 byte): Algorithm used for compression (See mapping below)
- **Uncompressed Size** (3 bytes): Size of raw chunk data (before compression) as a 3 byte little-endian unsigned integer.

Both Compressed and Uncompressed Size can fit in a 3 byte integer, given that

#### Chunk Compression Schemes

| Value | Name | Description |
|-------|------|-------------|
| `0` | `None` | No compression - data is stored as-is |
| `1` | `LZ4` | Standard LZ4 compression |
| `2` | `ByteGrouping4LZ4` | Byte grouping with 4-byte groups followed by LZ4 compression. Optimized for floating-point and other structured data where grouping bytes by position improves compression ratios |

##### Byte Grouping LZ4 Compression

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

### Example Chunk Serialization

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
