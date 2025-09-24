# CAS API Documentation

This document describes the HTTP API endpoints used by the CAS (Content Addressable Storage) client to interact with the remote CAS server.

## Authentication

To authenticate, authorize, and obtain the API base URL, follow the instructions in [Authentication](../spec/auth.md).

## Converting Hashes to Strings

Sometimes hashes are used in API paths as hexadecimal strings (reconstruction, xorb upload, global dedupe API).

To convert a 32 hash to a 64 hexadecimal character string to be used as part of an API path there is a specific procedure, MUST NOT directly convert each byte.

### Procedure

For every 8 bytes in the hash (indices 0-7, 8-15, 16-23, 24-31) reverse the order of each byte in those regions then concatenate the regions back in order.

Otherwise stated, consider each 8 byte part of a hash as a little endian 64 bit unsigned integer, then concatenate the hexadecimal representation of the 4 numbers in order (each padded with 0's to 16 characters).

### Example

Suppose a hash value is:
`[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]`

Then before converting to a string it will first have its bytes reordered to:
`[7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 23, 22, 21, 20, 19, 18, 17, 16, 31, 30, 29, 28, 27, 26, 25, 24]`

So the string value of the the provided hash [0..32] is **NOT** `000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f`.
It is: `07060504030201000f0e0d0c0b0a0908171615141312111f1e1d1c1b1a1918`.

## Endpoints

### 1. Get File Reconstruction

- **Description**: Retrieves reconstruction information for a specific file, includes byte range support when `Range` header is set.
- **Path**: `/v1/reconstructions/{file_id}`
- **Method**: `GET`
- **Parameters**:
  - `file_id`: File hash in hex format (64 lowercase hexadecimal characters).
See [file hashes](../spec/hashing.md#file-hashes) for computing the file hash and [converting hashes to strings](../spec/api.md#converting-hashes-to-strings).
- **Headers**:
  - `Range`: OPTIONAL. Format: `bytes={start}-{end}` (end is inclusive).
- **Minimum Token Scope**: `read`
- **Body**: None.
- **Response**: JSON (`QueryReconstructionResponse`)

  ```json
  {
    "offset_into_first_range": 0,
    "terms": [...],
    "fetch_info": {...}
  }
  ```

- **Error Responses**: See [Error Cases](../spec/api.md#error-cases)
  - `400 Bad Request`: Malformed `file_id` in the path. Fix the path before retrying.
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `404 Not Found`: The file does not exist. Not retryable.
  - `416 Range Not Satisfiable`: The requested byte range start exceeds the end of the file. Not retryable.

```txt
GET /v1/reconstructions/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
-H "Authorization: Bearer <token>"
OPTIONAL: -H Range: "bytes=0-100000"
```

### Example Response Body

See [QueryReconstructionResponse](../spec/download_protocol.md#queryreconstructionresponse-structure) for more details in the download protocol specification.

### 2. Query Chunk Deduplication (Global Deduplication)

- **Description**: Checks if a chunk exists in the CAS for deduplication purposes.
- **Path**: `/v1/chunks/{prefix}/{hash}`
- **Method**: `GET`
- **Parameters**:
  - `prefix`: The only acceptable prefix for the Global Deduplication API is `default-merkledb`.
  - `hash`: Chunk hash in hex format (64 lowercase hexadecimal characters).
See [Chunk Hashes](../spec/hashing.md#chunk-hashes) to compute the chunk hash and [converting hashes to strings](../spec/api.md#converting-hashes-to-strings).
- **Minimum Token Scope**: `read`
- **Body**: None.
- **Response**: Shard format bytes (`application/octet-stream`), deserialize as a [shard](../spec/shard.md#global-deduplication).
- **Error Responses**: See [Error Cases](../spec/api.md#error-cases)
  - `400 Bad Request`: Malformed hash in the path. Fix the path before retrying.
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `404 Not Found`: Chunk not already tracked by global deduplication. Not retryable.

```txt
GET /v1/chunks/default-merkledb/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
-H "Authorization: Bearer <token>"
```

#### Example Response Body

An example shard response body can be found in [Xet reference files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.shard.dedupe).

### 3. Upload Xorb

- **Description**: Uploads a serialized Xorb to the server; uploading real data in serialized format.
- **Path**: `/v1/xorbs/{prefix}/{hash}`
- **Method**: `POST`
- **Parameters**:
  - `prefix`: The only acceptable prefix for the Xorb upload API is `default`.
  - `hash`: Xorb hash in hex format (64 lowercase hexadecimal characters).
See [Xorb Hashes](../spec/hashing.md#xorb-hashes) to compute the hash, and [converting hashes to strings](../spec/api.md#converting-hashes-to-strings).
- **Minimum Token Scope**: `write`
- **Body**: Serialized Xorb bytes (`application/octet-stream`).
See [xorb format serialization](../spec/xorb.md).
- **Response**: JSON (`UploadXorbResponse`)

```json
{
  "was_inserted": true
}
```

- Note: `was_inserted` is `false` if the Xorb already exists; this is not an error.

- **Error Responses**: See [Error Cases](../spec/api.md#error-cases)
  - `400 Bad Request`: Malformed hash in the path, Xorb hash does not match the body, or body is incorrectly serialized.
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `403 Forbidden`: Token provided but does not have a wide enough scope (for example, a `read` token was provided). Clients MUST retry with a `write` scope token.

```txt
POST /v1/xorbs/default/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
-H "Authorization: Bearer <token>"
```

#### Example Request Body

An example xorb request body can be found in [Xet reference files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb).

### 4. Upload Shard

- **Description**: Uploads a Shard to the CAS.
Uploads file reconstructions and new xorb listing, serialized into the shard format; marks the files as uploaded.
- **Path**: `/v1/shards`
- **Method**: `POST`
- **Minimum Token Scope**: `write`
- **Body**: Serialized Shard data as bytes (`application/octet-stream`).
See [Shard format guide](../spec/shard.md#shard-upload).
- **Response**: JSON (`UploadShardResponse`)

```json
{
  "result": 0
}
```

- Where `result` is:
  - `0`: The Shard already exists.
  - `1`: `SyncPerformed` — the Shard was registered.

The value of `result` does not carry any meaning, if the upload shard API returns a `200 OK` status code, the upload was successful and the files listed are considered uploaded.

- **Error Responses**: See [Error Cases](../spec/api.md#error-cases)
  - `400 Bad Request`: Shard is incorrectly serialized or Shard contents failed verification.
    - Can mean that a referenced Xorb doesn't exist or the shard is too large
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `403 Forbidden`: Token provided but does not have a wide enough scope (for example, a `read` token was provided).

```txt
POST /v1/shards
-H "Authorization: Bearer <token>"
```

#### Example Request Body

An example shard request body can be found in [Xet reference files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.shard.verification-no-footer).

## Error Cases

### Non-Retryable Errors

- **400 Bad Request**: Returned when the request parameters are invalid (for example, invalid Xorb/Shard on upload APIs).
- **401 Unauthorized**: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
- **403 Forbidden**: Token provided but does not have a wide enough scope (for example, a `read` token was provided for an API requiring `write` scope).
- **404 Not Found**: Occurs on `GET` APIs where the resource (Xorb, file) does not exist.
- **416 Range Not Satisfiable**: Reconstruction API only; returned when byte range requests are invalid. Specifically, the requested start range is greater than or equal to the length of the file.

### Retryable Errors

- **Connection Errors**: Often caused by network issues. Retry if intermittent.
Clients SHOULD ensure no firewall blocks requests and SHOULD NOT use DNS overrides.
- **429 Rate Limiting**: Lower your request rate using a backoff strategy, then wait and retry.
Assume all APIs are rate limited.
- **500 Internal Server Error**: The server experienced an intermittent issue; clients SHOULD retry their requests.
- **503 Service Unavailable**: Service is temporarily unable to process requests; wait and retry.
- **504 Gateway Timeout**: Service took too long to respond; wait and retry.
