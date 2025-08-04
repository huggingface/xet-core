# File Reconstruction

After a file is mapped into a series of chunks, and those chunks are tracked by xorbs, we can define the file reconstruction or the recipe to re-materialize the file.

For every chunk of the file we need to know which xorb this chunk is in and its index (starting at 0) in that xorb and list out all the chunks in this way to re-create the file.

Now since xorbs contain multiple chunks, we can condense the listing of each range of chunks that are contiguous in the same xorb together and this map the listing of chunks that form the file to a series of sub-ranges of chunks within xorbs.

## Example 1

Suppose that a file has 26 chunks, A-Z
Chunks A-G are in order in xorb X1 starting at index 0.
Chunks H-K are in order in xorb X2 starting at index 0.
Chunks K-Z are in order in xorb X3 starting at index 645.

Then to reconstruct the file the following chunk ranges are needed:

Xorb X1 chunks `[0, 8)`. (need chunk A at chunk index 0, through chunk G at chunk index 7)
Xorb X2 chunks `[0, 4)`. (need chunk H at chunk index 0, through chunk J at chunk index 3)
Xorb X3 chunks `[645, 662)`. (need chunk K at chunk index 645, through chunk Z at chunk index 661)

TODO: write here

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

- Chunks `[0, 4)` from the first xorb (~264KB of unpacked data)
- Chunks `[0, 2)` from the second xorb (~144KB of unpacked data)

The `fetch_info` provides the HTTP URLs and byte ranges needed to download the required chunk data from each xorb. The ranges provided within fetch_info and term sections are always end-exclusive i.e.
`{ "start": 0, "end": 4 }` is a range of 4 chunks at indices 0, 1, 2, and 3. The ranges provided under a fetch_info items' url_range key are to be used to form the `Range` header when downloading the chunk range.
A "url_range" value of `{ "start": X, "end": Y }` creates a `Range` header value of `bytes=X-Y`.
