# Download Protocol

This document describes the complete process of downloading a single file from the Xet protocol using the CAS (Content Addressable Storage) reconstruction API.

## Overview

File download in the Xet protocol is a two-stage process:

1. **Reconstruction Query**: Query the CAS API to get file reconstruction metadata
2. **Data Fetching**: Download and reassemble the file using the reconstruction metadata

## Stage 1: Calling the Reconstruction API

### Single File Reconstruction

To download a file given a file hash, first call the reconstruction API to get the file reconstruction. Follow the steps in [api.md](../spec/api.md#1-get-file-reconstruction).

Note that you will need at least a `read` scope auth token, [auth reference](../spec/auth.md).

> For large files is may be recommended to download the request the reconstruction in batches i.e. the first 10GB, download all the data, then the next 10GB and so on. Use the `Range` header to specify a range of file data.

## Stage 2: Understanding the Reconstruction Response

The reconstruction API returns a `QueryReconstructionResponse` object with three key components:

### QueryReconstructionResponse Structure

Scroll

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
    ...
  ],
  "fetch_info": {
    "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456": [
      {
        "range": {
          "start": 0,
          "end": 4
        },
        "url": "https://transfer.xethub.hf.co/xorb/default/a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
        "url_range": {
          "start": 0,
          "end": 131071
        }
      },
      ...
    ],
    ...
  }
}
```

### Fields

#### offset_into_first_range

- Type: `number`
- For a full file or when the specified range start is 0, then this is guaranteed to be `0`
- For range queries this is the byte offset into the first term (deserialized/chunks decompressed) to start to keep data from.
  - since the requested range may start in the middle of a chunk, and data must be downloaded in full chunks (since they may need to be deserialized) then this offset tells a client how many bytes to skip in the first chunk (or possibly multiple chunks within the first term).

#### terms

- Type: `Array<CASReconstructionTerm>`
- Ordered list of reconstruction terms describing what chunks to download from which xorb
- Each `CASReconstructionTerm` contains:
  - `hash`: The xorb hash (64-character lowercase hex string)
  - `range`: Chunk index range`{ start: number, end: number }` within the xorb; end-exclusive `[start, end)`
  - `unpacked_length`: Expected length after decompression (for validation)

#### fetch_info

- Type: `Map<Xorb Hash (64 character lowercase hex string), Array<CASReconstructionFetchInfo>>`
- Maps xorb hashes to required information to download some of their chunks.
  - The mapping is to an array of 1 or more `CASReconstructionFetchInfo`
- Each `CASReconstructionFetchInfo` contains:
  - `url`: HTTP URL for downloading the xorb data, presigned url containing authorization information
  - `url_range` (bytes_start, bytes_end): Byte range `{ start: number, end: number }` for the Range header; end-inclusive `[start, end]`
    - The range header must be set as `Range: bytes=<start>-<end>` when downloading this chunk range
  - `range` (index_start, index_end): Chunk index range `{ start: number, end: number }` that this URL provides; end-exclusive `[start, end)`
    - This range indicates which range of chunk indices within this xorb that this fetch info term is describing

## Stage 3: Downloading and Reconstructing the File

### Process Overview

1. Process each `CASReconstructionTerm` in order from the `terms` array
2. For each `CASReconstructionTerm`, find matching fetch info using the term's hash

    1. get the list of fetch_info items under the xorb hash from the `CASReconstructionTerm`. The xorb hash is guaranteed to exist as a key in the fetch_info map.
    2. linearly iterate through the list of `CASReconstructionFetchInfo` and find one which refers to a chunk range that is equal or encompassing the term's chunk range.
        - Such a fetch_info item is guaranteed to exist. If none exist the server is at fault.
3. Download the required data using HTTP `GET` request with the `Range` header set
4. Deserialize the downloaded xorb data to extract chunks

    1. This series of chunks contains chunks at indices specified by the `CASReconstructionFetchInfo`'s `range` field. Trim chunks at the beginning or end to match the chunks specified by the reconstruction term's `range` field.
    2. (for the first term only) skip `offset_into_first_range` bytes
5. Concatenate the results in term order to reconstruct the file

### Detailed Download Process

#### Download Reconstruction

- use the reconstruction api to download the reconstruction object for a given file

```python
file_id = "0123...abcdef"
api_endpoint, token = get_token() # follow auth.md instructions
url = api_endpoint + "/reconstructions/" + file_id
reconstruction = get(url, headers={"Authorization": "Bearer: " + token})

# break the reconstruction into components
terms = reconstruction["terms"]
fetch_info = reconstruction["fetch_info"]
offset_into_first_range = reconstruction["offset_into_first_range"]
```

#### Match Terms to Fetch Info

For each `CASReconstructionTerm` in the `terms` array:

- Look up the term's `hash` in the `fetch_info` map to get a list of `CASReconstructionFetchInfo`
- Find a `CASReconstructionFetchInfo` entry where the fetch info's `range` contains the term's `range`
  - linearly search through the array of `CASReconstructionFetchInfo` and find the element where the range block (`{ "start": number, "end": number }`) of the `CASReconstructionFetchInfo` has start <= term's range start AND end >= term's range end.
  - The server is meant to guarantee a match, if there isn't a match this download is considered failed and the server made an error.

```python
for term in terms:
  xorb_hash = term["hash"]
  fetch_info_entries = fetch_info[xorb_hash]
  fetch_info_entry = None
  for entry in fetch_info_entries:
    if entry["range"][start] <= term["range"]["start"] and entry["range"]["end"] >= term["range"]["end"]:
      fetch_info_entry = entry
      break
  if fetch_info_entry is None:
    # Error!
```

#### Step 2: Download Xorb Data

For each matched fetch info:

1. Make an HTTP GET request to the `url` in the fetch info entry
2. Include a `Range` header: `bytes={url_range.start}-{url_range.end}`

```python
for term in terms:
  ...
  data_url = fetch_info_entry["url"]
  range_header = "bytes=" + fetch_info_entry["url_range"]["start"] + "-" + fetch_info_entry["url_range"]["end"]
  data = get(data_url, headers={"Range": range_header})
```

#### Deserialize Downloaded Data

The downloaded data is in xorb format and must be deserialized:

1. **Parse xorb structure**: The data contains compressed chunks with headers
2. **Decompress chunks**: Each chunk has a header followed by compressed data
3. **Extract byte indices**: Track byte boundaries between chunks for range extraction
4. **Validate length**: Ensure decompressed length matches `unpacked_length` from the term

**Note**: The specific deserialization process depends on the [Xorb format](../xorb.md).

```python
for term in terms:
  ...
  chunks = {}
  for i in range(fetch_info_entry["range"]["start"], fetch_info_entry["range"]["end"]):
    chunk = deserialize_chunk(data) # assume data is a reader that advances forwards
    chunks[i] = chunk
  # at this point data should be fully consumed
```

#### Step 4: Extract Term Data

From the deserialized xorb data:

1. Use the term's `range` to identify which chunks are needed
2. Extract only the chunks specified by `range.start` to `range.end-1` (end-exclusive)
3. Apply any range offsets if processing a partial file download

```python
file_chunks = []
for term in terms:
  ...
  for i in range(term["range"]["start"], term["range"]["end"]):
    chunk = chunks[i]
    # it is possible that the offset captures multiple chunks, so we may need to skip whole chunks
    if offset_into_first_range > len(chunk):
      offset_into_first_range -= len(chunk)
      continue
    if offset_info_first_range > 0:
      chunk = chunk[offset_into_first_range:]
      offset_info_first_range = 0
    
    file_chunks.push(chunk)
```

#### Step 5: Stitch Results Together

Write all of the chunks to the output file or buffer.

If a range was specified then the total data will need to be truncated to the amount of bytes requested.
When a range is specified but the range does not end on a chunk boundary the last byte of the requested range will be in the middle of the last chunk.
A client knows the start of the data from `offset_into_first_range` and can then use the length of the specified range to know end end offset.

```python
with open(file_path) as f:
  for chunk in file_chunks:
    f.write(chunk)
```

## Range Downloads

For partial file downloads, the reconstruction API supports range queries:

- Include `Range: bytes=start-end` header in reconstruction request
- The `offset_into_first_range` field indicates where your range starts within the first term
- The end of the content will need to be truncated to fit the requested range.
  - Except if the requested range exceeds the total file length, then the returned content will be shorter and no truncation is necessary.

When downloading individual term data:

A client must include the range header formed with the values from the url_range field to specify the exact range of data of a xorb that they are accessing. Not specifying this header will cause result in an authorization failure.

Xet global deduplication requires that access to xorbs is only granted to authorized ranges.
Not specifying this header will result in an authorization failure.

## Performance Considerations

- **Range coalescing**: Multiple terms may share the same fetch info for efficiency, check where else you may need to use a term
- **Parallel downloads**: Terms can be downloaded in parallel, but must be assembled in order
  - On file systems with fast seeking, it may be advantageous to open the output file in different threads and writing contents at different offsets
- **Caching**: Consider caching downloaded xorb ranges to avoid redundant requests
- **Retry logic**: Implement exponential backoff for transient failures

### Caching recommendations

1. It can be ineffective to cache the reconstruction object
    1. The fetch_info section provides short-expiration pre-signed url's hence you cannot cache them for much time at all
    2. To get those url's to access the data you will need to call the reconstruction API again anyway
2. Cache chunks by range not just individually
    1. If you need a chunk from a xorb it is very likely that you will need another, so cache them close
3. Caching helps when downloading similar contents. May not be worth to cache data if you are always downloading different things

## More complex QueryReconstruction Example

Here's an example of a serialized `QueryReconstructionResponse` struct that shows how file reconstruction would work across multiple xorbs:

```json
{
  "offset_into_first_range": 0,
  "terms": [
    {
      "hash": "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
      "unpacked_length": 263873,
      "range": {
        "start": 1,
        "end": 4
      }
    },
    {
      "hash": "fedcba0987654321098765432109876543210fedcba098765432109876543",
      "unpacked_length": 143890,
      "range": {
        "start": 0,
        "end": 3
      }
    },
    {
      "hash": "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
      "unpacked_length": 3063572,
      "range": {
        "start": 3,
        "end": 43
      }
    },
  ],
  "fetch_info": {
    "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456": [
      {
        "range": {
          "start": 1,
          "end": 43
        },
        "url": "https://transfer.xethub.hf.co/xorb/default/a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130721%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=d6796aa6097c82ba7e33b4725e8396f8a9638f7c3d4b5a6b7c8d9e0f1a2b3c4d",
        "url_range": {
          "start": 57980,
          "end": 1433008
        }
      }
    ],
    "fedcba0987654321098765432109876543210fedcba098765432109876543": [
      {
        "range": {
          "start": 0,
          "end": 3
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

- Chunks `[1, 4)` from the first xorb (~264KB of unpacked data)
- Chunks `[0, 2)` from the second xorb (~144KB of unpacked data)
- Chunks `[3, 43)` from the same xorb from the first term (~3MB of unpacked data)

The `fetch_info` provides the HTTP URLs and byte ranges needed to download the required chunk data from each xorb. The ranges provided within fetch_info and term sections are always end-exclusive i.e. `{ "start": 0, "end": 3 }` is a range of 3 chunks at indices 0, 1 and 2.
The ranges provided under a fetch_info items' url_range key are to be used to form the `Range` header when downloading the chunk range.
A `"url_range"` value of `{ "start": X, "end": Y }` creates a `Range` header value of `bytes=X-Y`.

When downloading and deserializing the chunks from xorb `a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456` we will have the chunks at indices `[1, 43)`.
We will need to only use the chunks at `[1, 4)` to fulfill the first term and then chunks `[3, 43)` to fulfill the third term.
Note that in this example the chunk at index 3 is used twice! This is the benefit of deduplication; we only need to download the chunk content once.

## Download Protocol Diagram (TODO check)

```mermaid
sequenceDiagram
  autonumber
  actor Client as "Client"
  participant CAS as "CAS API"
  participant Transfer as "Transfer Service (Xet storage)"

  Client->>CAS: "GET /reconstructions/{file_id}\nAuthorization: Bearer <token>\nRange: bytes=start-end (optional)"
  CAS-->>Client: "200 OK\nQueryReconstructionResponse {offset_into_first_range, terms[], fetch_info{}}"

  loop "For each term in terms (ordered)"
    Client->>Client: "Find fetch_info by xorb hash; entry whose range contains term.range"
    Client->>Transfer: "GET {url}\nRange: bytes=url_range.start-url_range.end"
    Transfer-->>Client: "206 Partial Content\nxorb byte range"
    Client->>Client: "Deserialize xorb → chunks for fetch_info.range"
    Client->>Client: "Trim to term.range; apply offset for first term"
    Client->>Client: "Append chunks to output"
  end

  alt "Range requested"
    Client->>Client: "Truncate output to requested length"
  end
```
