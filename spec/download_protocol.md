# File Download Protocol

This document describes the complete process of downloading a single file from the Xet protocol using the CAS (Content Addressable Storage) reconstruction API.

## Overview

File download in the Xet protocol is a two-stage process:

1. **Reconstruction Query**: Query the CAS API to get file reconstruction metadata
2. **Data Fetching**: Download and reassemble the file using the reconstruction metadata

## Stage 1: Calling the Reconstruction API

### Single File Reconstruction

To download a file, first call the reconstruction API to get the metadata needed for reconstruction:

**Endpoint**: `GET /v1/reconstruction/{file_id}`

**Parameters**:

- `file_id`: The file's MerkleHash in hex format (64-character hex string)
- `Range` (header, optional): Byte range in format `bytes={start}-{end}` (inclusive end)

**Authentication**: Bearer token required via `Authorization` header

**Example Request**:

```http
GET /v1/reconstruction/a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456 HTTP/1.1
Host: cas-server.xethub.hf.co:8080
Authorization: Bearer YOUR_TOKEN_HERE
Range: bytes=0-1023
```

**Response**: JSON object conforming to `QueryReconstructionResponse` schema

### Batch File Reconstruction

For multiple files, use the batch endpoint:

**Endpoint**: `GET /v1/reconstructions?file_id={hash1}&file_id={hash2}...`

## Stage 2: Understanding the Reconstruction Response

The reconstruction API returns a `QueryReconstructionResponse` object with three key components:

### QueryReconstructionResponse Structure

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
    }
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
      }
    ]
  }
}
```

### Key Fields

#### offset_into_first_range

- Type: `u64/number`
- For a full file or when the specified range start is 0, then this is guaranteed to be `0`
- For range queries this is the byte offset into the first term (deserialized/chunks decompressed) to keep data from.
  - since the requested range may start in the middle of a chunk, and data must be downloaded in full chunks (since they may need to be deserialized) then this offset tells a client how many bytes to skip in the first chunk (or possibly multiple chunks within the first term).

#### terms

- Type: `Array<CASReconstructionTerm>`
- Ordered list of reconstruction terms describing what chunks to download from which xorb
- Each term contains:
  - `hash`: The xorb hash (64-character hex string)
  - `range`: Chunk index range`{ start: number, end: number }` within the xorb; end-exclusive `[start, end)`
  - `unpacked_length`: Expected length after decompression (for validation)

#### fetch_info

- Type: `Map<Xorb Hash (64 character hex string), Array<CASReconstructionFetchInfo>>`
- Maps xorb hashes to download information
- Each fetch info entry contains:
  - `url`: HTTP URL for downloading the xorb data, presigned url containing authorization information
  - `url_range`: Byte range `{ start: number, end: number }` for the Range header; end-inclusive `[start, end]`
    - The range header must be set as `Range: bytes=<start>-<end>` when downloading this chunk range
  - `range`: Chunk range `{ start: number, end: number }` that this URL provides; end-exclusive `[start, end)`
    - This range indicates which range of chunk indices within this xorb that this fetch info term is describing

## Stage 3: Downloading and Reconstructing the File

### Process Overview

1. Process each term in order from the `terms` array
2. For each term, find matching fetch info using the term's hash
  i. get the list of fetch_info items under the xorb hash from the reconstruction term. The xorb hash is guaranteed to exist as a key in the fetch_info map.
  ii. linearly iterate through the fetch_info items and find one which refers to a chunk range that is equal or encompassing the term's chunk range. Such a fetch_info item is guaranteed to exist.
3. Download the required data using HTTP `GET` request with the `Range` header set
4. Deserialize the downloaded xorb data to extract chunks
  i. This series of chunks contains chunks at indices specified by the fetch_info item's `range` field. Trim chunks at the beginning or end to match the chunks specified by the reconstruction term's `range` field.
5. Concatenate the results in term order to reconstruct the file

### Detailed Download Process

#### Step 1: Match Terms to Fetch Info

For each `CASReconstructionTerm` in the `terms` array:

1. Look up the term's `hash` in the `fetch_info` map
2. Find a `CASReconstructionFetchInfo` entry where the fetch info's `range` contains the term's `range`
3. Use this fetch info for downloading

#### Step 2: Download Xorb Data

For each matched fetch info:

1. Make an HTTP GET request to the `url`
2. Include a `Range` header: `bytes={url_range.start}-{url_range.end}`
3. The response contains compressed xorb data for the specified byte range

**Example HTTP Request**:

```http
GET /xorb/default/a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456 HTTP/1.1
Host: transfer.xethub.hf.co
Range: bytes=0-131071
```

#### Step 3: Deserialize Downloaded Data

The downloaded data is in xorb (serialized CAS object) format and must be deserialized:

1. **Parse xorb structure**: The data contains compressed chunks with headers
2. **Decompress chunks**: Each chunk has a header followed by compressed data
3. **Extract byte indices**: Track byte boundaries between chunks for range extraction
4. **Validate length**: Ensure decompressed length matches `unpacked_length` from the term

**Note**: The specific deserialization process depends on the CAS object format. Refer to the [xorb format documentation](spec/xorb_formation.md) for implementation details.

#### Step 4: Extract Term Data

From the deserialized xorb data:

1. Use the term's `range` to identify which chunks are needed
2. Extract only the chunks specified by `range.start` to `range.end-1` (end-exclusive)
3. Apply any range offsets if processing a partial file download

#### Step 5: Stitch Results Together

1. **Preserve order**: Process terms in the exact order they appear in the `terms` array
2. **Handle range offsets**: For the first term, skip `offset_into_first_range` bytes
3. **Concatenate**: Append each term's data to build the final file content
4. **Validate**: Ensure the final file size matches expectations

### Error Handling

- **403 Forbidden**: Presigned URLs may expire; re-fetch reconstruction info and retry
- **416 Range Not Satisfiable**: Requested range exceeds file size
- **404 Not Found**: File or xorb not found in CAS
- **Validation errors**: Decompressed data length doesn't match `unpacked_length`

### Performance Considerations

- **Parallel downloads**: Terms can be downloaded in parallel, but must be assembled in order
- **Caching**: Consider caching downloaded xorb ranges to avoid redundant requests
- **Range coalescing**: Multiple terms may share the same fetch info for efficiency
- **Retry logic**: Implement exponential backoff for transient failures

## Example Implementation Flow

```txt
1. GET /v1/reconstruction/{file_hash}
2. Parse QueryReconstructionResponse
3. For each term in terms[]:
   a. Find matching fetch_info entry
   b. HTTP GET with Range header
   c. Deserialize xorb data
   d. Extract chunks for term.range
4. Concatenate term results in order
5. Apply offset_into_first_range to first term
6. Return reconstructed file data
```

## Range Downloads

For partial file downloads, the reconstruction API supports range queries:

- Include `Range: bytes=start-end` header in reconstruction request
- The `offset_into_first_range` field indicates where your range starts within the first term
- Only download and process the terms needed for your range
- Extract the exact byte range from the reassembled terms

This allows efficient streaming and partial downloads without fetching the entire file reconstruction metadata.

## Pseudo-code of Simple Download Protocol

```python
reconstruction = get("https://cas-server.xethub.hf.co/v1/reconstruction/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
terms = reconstruction["terms"]
fetch_info = reconstruction["fetch_info"]
fetch_info_item = None
file = open(file_path)
for term in terms:
  fetch_info_items = fetch_info[term["hash"]]
  for item in fetch_info_items:
    if item["range"]["start"] <= term["range"]["start"] and item["range"]["end"] >= term["range"]["end"]:
      fetch_info_item = item
      break
  if fetch_info_item is None:
    # server returned bad reconstruction information
    raise Error()
  serialized_chunks_data = get(fetch_info_item["url"], headers={"Range": f"{fetch_info_item["url_range"]["start"]}-{fetch_info_item["url_range"]["end"]}"})
  deserialized_chunks = deserialize_chunks(serialized_chunks_data) # as List of chunks each chunk is decompressed bytes object
  # only use those chunks that are actually in the term; the fetch_info_item may refer to a larger range for other chunks within used by this file reconstruction
  for chunk in deserialized_chunks[term["range"]["start"] - fetch_info_item["range"]["start"] : term["range"]["start"] - fetch_info_item["range"]["end"]]
    file.write(chunk)

file.close()
```

## Extensions/optimizations

When downloading large files it is more efficient to request reconstructions of smaller runs of data rather than fetch the whole reconstruction at the beginning.

Parallelize the term download. We have also found it to be more efficient on SSD's to parallelize the term download and write each term independently by seeking to the correct position in the file and writing the data at that position rather than doing so linearly.

Cache chunk ranges locally:
TODO: explain some caching paradigms
