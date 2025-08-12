# CAS API Documentation

This document describes the HTTP API endpoints used by the CAS (Content Addressable Storage) client to interact with the remote CAS server.

## Authentication

To authenticate, authorize, and obtain the API base URL, follow the instructions in [Authentication](./auth.md).

## Endpoints

### 1. Get File Reconstruction

- **Description**: Retrieves reconstruction information for a specific file, optionally with byte range support.
- **Path**: `/v1/reconstructions/{file_id}`
- **Method**: `GET`
- **Parameters**:
  - `file_id`: MerkleHash in hex format (64 lowercase hexadecimal characters).
- **Headers**:
  - `Range`: Optional. Format: `bytes={start}-{end}` (end is inclusive).
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

- **Error Responses**:
  - `400 Bad Request`: Malformed `file_id` in the path. Fix the path before retrying.
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `404 Not Found`: The file does not exist. Not retryable.
  - `416 Range Not Satisfiable`: The requested byte range start exceeds the end of the file. Not retryable.

### 2. Query Chunk Deduplication (Global Deduplication)

- **Description**: Checks if a chunk exists in the CAS for deduplication purposes.
- **Path**: `/v1/chunks/{prefix}/{hash}`
- **Method**: `GET`
- **Parameters**:
  - `prefix`: The only acceptable prefix for the Global Deduplication API is `default-merkledb`.
  - `hash`: Chunk hash in hex format (64 lowercase hexadecimal characters). See [Chunk Hashes](./hashing.md#chunk-hashes).
- **Minimum Token Scope**: `read`
- **Body**: None.
- **Response**: Raw bytes in Shard format (chunk data, if it exists).
- **Error Responses**:
  - `400 Bad Request`: Malformed hash in the path. Fix the path before retrying.
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `404 Not Found`: Chunk not already tracked by global deduplication. Not retryable.

### 3. Upload Xorb

- **Description**: Uploads a serialized Xorb to the server with progress tracking. See [Xorb Hashes](./hashing.md#xorb-hashes).
- **Path**: `/v1/xorbs/{prefix}/{hash}`
- **Method**: `POST`
- **Parameters**:
  - `prefix`: The only acceptable prefix for the Xorb upload API is `default`.
  - `hash`: MerkleHash in hex format. See [Xorb Hashes](./hashing.md#xorb-hashes).
- **Minimum Token Scope**: `write`
- **Body**: Serialized Xorb bytes.
- **Response**: JSON (`UploadXorbResponse`)

```json
{
  "was_inserted": true
}
```

- Note: `was_inserted` is `false` if the Xorb already exists; this is not an error.

- **Error Responses**:
  - `400 Bad Request`: Malformed hash in the path, Xorb hash does not match the body, or body is incorrectly serialized.
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `403 Forbidden`: Token provided but does not have a wide enough scope (for example, a `read` token was provided). Retry with a `write` scope token.

### 4. Upload Shard

- **Description**: Uploads a Shard to the CAS with optional forced synchronization.
- **Path**: `/v1/shards`
- **Method**: `POST`
- **Minimum Token Scope**: `write`
- **Body**: Raw bytes (Shard data). See [Shard](./shard.md).
- **Response**: JSON (`UploadShardResponse`)

```json
{
  "result": 0
}
```

- Where `result` is:
  - `0`: The Shard already exists.
  - `1`: `SyncPerformed` — the Shard was registered. See `UploadShardResponseType`.

- **Error Responses**:
  - `400 Bad Request`: Shard is incorrectly serialized or Shard contents failed verification.
  - `401 Unauthorized`: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
  - `403 Forbidden`: Token provided but does not have a wide enough scope (for example, a `read` token was provided).

## Error Cases/Codes

### Non-Retryable Errors

- **400 Bad Request**: Returned when the request parameters are invalid (for example, invalid Xorb/Shard on upload APIs).
- **401 Unauthorized**: Refresh the token to continue making requests, or provide a token in the `Authorization` header.
- **403 Forbidden**: Token provided but does not have a wide enough scope (for example, a `read` token was provided for an API requiring `write` scope).
- **404 Not Found**: Occurs on `GET` APIs where the resource (Xorb, file) does not exist.
- **416 Range Not Satisfiable**: Reconstruction API only; returned when byte range requests are invalid. Specifically, the requested start range is greater than or equal to the length of the file.

### Retryable Errors

- **Connection Errors**: Often caused by network issues. Retry if intermittent. Ensure you do not have a firewall blocking requests or DNS overrides.
- **429 Rate Limiting**: Lower your request rate using a backoff strategy, then wait and retry. Assume all APIs are rate limited.
- **500 Internal Server Error**: The server experienced an intermittent issue; clients should retry their requests.
- **503 Service Unavailable**: Service is temporarily unable to process requests; wait and retry.
- **504 Gateway Timeout**: Service took too long to respond; wait and retry.
