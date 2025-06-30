# Xet Protocol Specification

## Xorb Format

A xorb is a series of "Chunks".

### Chunk Format

A chunk consists of a header followed by compressed data. The header contains metadata about the chunk, particularly the compression scheme required to know how to deserialize the chunk.

### Chunk Header Structure

The chunk header is serialized as follows:

- **Version** (1 byte): Protocol version, currently `0`
- **Compressed Size** (3 bytes): Size of data after compression as a 3 byte little-endian unsigned integer.
- **Compression Type** (1 byte): Algorithm used for compression (See mapping below)
- **Compressed Size** (3 bytes): Size of raw chunk data (before compression) as a 3 byte little-endian unsigned integer.

### Chunk Compression Schemes

| Value | Name | Description |
|-------|------|-------------|
| `0` | `None` | No compression - data is stored as-is |
| `1` | `LZ4` | Standard LZ4 compression using lz4_flex |
| `2` | `ByteGrouping4LZ4` | Byte grouping with 4-byte groups followed by LZ4 compression. Optimized for floating-point and other structured data where grouping bytes by position improves compression ratios |

#### Byte Grouping LZ4 Compression

Byte grouping LZ4 compression is an optimization technique that improves compression ratios for structured data like floating-point numbers, integers, and other data types where values have similar byte patterns at specific positions.

1. **Byte Grouping Phase**: The input data is reorganized by grouping bytes by their position within each data element. For example, with 4-byte groups:
   - Original data: `[A1, A2, A3, A4, B1, B2, B3, B4, C1, C2, C3, C4, ...]`
   - Grouped data: `[A1, B1, C1, ..., A2, B2, C2, ..., A3, B3, C3, ..., A4, B4, C4, ...]`

2. **LZ4 Compression**: The grouped data is then compressed using standard LZ4 compression.

### Chunk Data

Following the header is the compressed data block, exactly `compressed_size` bytes long.

### Example Chunk Serialization

## Shard Format

## Download Protocol

## Example QueryReconstructionResponse JSON

TODO: explain the download endpoint

Here's an example of a serialized `QueryReconstructionResponse` struct that shows how file reconstruction would work across multiple xorbs:

```json
{
  "offset_into_first_range": 0,
  "terms": [
    {
      "hash": "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
      "unpacked_length": 263873,
      "range": {
        "start": 0,
        "end": 4
      }
    },
    {
      "hash": "fedcba0987654321098765432109876543210fedcba098765432109876543",
      "unpacked_length": 143890,
      "range": {
        "start": 0,
        "end": 2
      }
    }
  ],
  "fetch_info": {
    "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456": [
      {
        "range": {
          "start": 0,
          "end": 4
        },
        "url": "https://transfer.xethub.hf.co/xorb/default/a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130721%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=d6796aa6097c82ba7e33b4725e8396f8a9638f7c3d4b5a6b7c8d9e0f1a2b3c4d",
        "url_range": {
          "start": 0,
          "end": 131071
        }
      }
    ],
    "fedcba0987654321098765432109876543210fedcba098765432109876543": [
      {
        "range": {
          "start": 0,
          "end": 2
        },
        "url": "https://transfer.xethub.hf.co/xorb/default/fedcba0987654321098765432109876543210fedcba098765432109876543?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130721%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=d6796aa6097c82ba7e33b4725e8396f8a9638f7c3d4b5a6b7c8d9e0f1a2b3c4d",
        "url_range": {
          "start": 0,
          "end": 65670
        }
      }
    ]
  }
}
```

This example shows reconstruction of a file that requires:

- Chunks `[0, 3)` from the first xorb (~264KB of unpacked data)
- Chunks `[0, 1)` from the second xorb (~144KB of unpacked data)

The `fetch_info` provides the HTTP URLs and byte ranges needed to download the required chunk data from each xorb. The ranges provided within fetch_info and term sections are always end-exclusive i.e. `{ "start": 0, "end": 4 }` is a range of 4 chunks at indices 0, 1, 2, and 3. The ranges provided under a fetch_info items' url_range key are to be used to form the `Range` header when downloading the chunk range. A "url_range" value of `{ "start": X, "end": Y }` creates a `Range` header value of `bytes=X-Y`.
