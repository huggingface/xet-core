# CAS API Documentation

This document describes the HTTP API endpoints used by the CAS (Content Addressable Storage) client to interact with the remote CAS server.

## Authentication

In order to be authenticated and authorized to invoke any of the following API's as well as determine the API endpoint url, follow the instructions in [../auth.md]

## Endpoints

### 1. Get File Reconstruction

**Description**: Retrieves reconstruction information for a specific file, optionally with byte range support.

- **Path**: `/v1/reconstructions/{file_id}`
- **Method**: `GET`
- **Parameters**:
  - `file_id`: MerkleHash in hex format (64 lowercase hexadecimal character string)
- **Headers**:
  - `Range`: Optional. Format: `bytes={start}-{end}` (inclusive end)
- **Minimum Token Scope**: `read`
- **Body**: None
- **Response**: JSON (`QueryReconstructionResponse`)

  ```json
  {
    "offset_into_first_range": 0,
    "terms": [...],
    "fetch_info": {...}
  }
  ```

- **Error Responses**:
  - 400 bad request (such as the file_id not being matching an existing file)
  - 404 File not found
  - 416 Range Not Satisfiable: When requested byte range start exceeds the end of a file

### 2. Query Chunk Deduplication (Global Deduplication)

**Description**: Checks if a chunk exists in the CAS for deduplication purposes.

- **Path**: `/v1/chunks/{prefix}/{hash}`
- **Method**: `GET`
- **Parameters**:
  - `prefix`: Currently only acceptable prefix for the Global Deduplication API is `default-merkledb`.
  - `hash`: Chunk hash in hex format (64 lowercase hexadecimal character string). Review [how to compute chunk hash](../hashing.md#Chunk%20Hashes) to compute chunk hashes
- **Minimum Token Scope**: `read`
- **Body**: None
- **Response**: Raw bytes in shard format (chunk data if exists)
- **Error Responses**:
  - `400 Bad request`: malformed hash in path
  - `404 Not Found`: Chunk not already tracked by global deduplication

### 3. Upload XORB

**Description**: Uploads a serialized CAS object (XORB) to the server with progress tracking. Review [how to compute xorb hash](../hashing.md#Xorb%20Hashes) to compute xorb hashes.

- **Path**: `/v1/xorbs/{prefix}/{hash}`
- **Method**: `POST`
- **Parameters**:
  - `prefix`: Currently the only acceptable prefix for the Xorb upload API is `default`.
  - `hash`: MerkleHash in hex format. Review [how to compute xorb hash](../hashing.md#Xorb%20Hashes) to compute xorb hashes.
- **Minimum Token Scope**: `write`
- **Body**: Serialized Xorb
- **Response**: JSON (`UploadXorbResponse`)

```json
{
  "was_inserted": true
}
```

  was_inserted is false if the xorb already exists, this is not an error

- **Error Responses**:
  - `400 Bad Request`: malformed hash in path, xorb hash is incorrect for body, body is incorrectly serialized
  - `403 Forbidden`: authentication token missing `write` scope.

### 4. Upload Shard

**Description**: Uploads a shard to the CAS with optional forced synchronization.

- **Path**: `/v1/shards`
- **Method**: `POST`
- **Minimum Token Scope**: `write`
- **Body**: Raw bytes (shard data). See [how to serialize a shard](../shard.md).
- **Response**: JSON (`UploadShardResponse`)

  ```json
  {
    "result": 0 | 1
  }
  ```

  Where 0 indicates the shard already exists and 1 indicates "SyncPerformed" meaning that the shard was registered (UploadShardResponseType).

- **Error Responses**:
  - `400 Bad Request`: shard is incorrectly serialized, shard contents failed verification
  - `403 Forbidden`: authentication token missing `write` scope.

## Common Error Cases/Codes

- **Connection Errors**: Often caused by network issues.
- **400 Bad Request**: Returned when the request parameters are invalid, e.g. invalid xorb/shard on upload API's.
- **401 Unauthorized**: Need to get a refreshed token to continue making requests.
- **404 Not Found**: Occurs on GET api's where the resource (xorb, file) do not exist.
- **416 Range Not Satisfiable**: Returned when byte range requests are invalid; specifically the requested start range is greater than or equal to the length of the file.
- **429 Rate Limiting**: Assume there is rate limiting on all API's and lower your request rate using a backoff delay strategy
