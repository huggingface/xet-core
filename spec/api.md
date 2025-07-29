# CAS API Documentation

This document describes the HTTP API endpoints used by the CAS (Content Addressable Storage) client to interact with the remote CAS server.

## Authentication

In order to be authenticated and authorized to invoke any of the following API's as well as determine the API endpoint url, follow the instructions in [../auth.md]

## Endpoints

### 1. Get File Reconstruction

**Description**: Retrieves reconstruction information for a specific file, optionally with byte range support.

- **Path**: `/v1/reconstruction/{file_id}`
- **Method**: `GET`
- **Parameters**:
  - `file_id`: MerkleHash in hex format
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
  - 400 bad request
  - 404 File not found
  - 416 Range Not Satisfiable: When requested byte range start exceeds the end of a file

### 2. Batch Get Reconstruction

**Description**: Retrieves reconstruction information for multiple files in a single request.

- **Path**: `/v1/reconstructions`
- **Method**: `GET`
- **Query Parameters**:
  - `file_id`: MerkleHash(es) in hex format (can be repeated)
  - Example: `/v1/reconstructions?file_id=abc123&file_id=def456`
- **Headers**: None (beyond authentication)
- **Minimum Token Scope**: `read`
- **Body**: None
- **Response**: JSON (`BatchQueryReconstructionResponse`)

- **Error Responses**:
  - 400 bad request
  - 404 File not found if any file is not found

### 3. Query Chunk Deduplication (Global Deduplication)

**Description**: Checks if a chunk exists in the CAS for deduplication purposes.

- **Path**: `/v1/chunk/default-merkledb/{hash}`
- **Method**: `GET`
- **Parameters**:
  - `hash`: Chunk hash in hex format. Review [how to compute chunk hash](../hashing.md#Chunk%20Hashes) to compute chunk hashes
- **Headers**: None (beyond authentication)
- **Minimum Token Scope**: `read`
- **Body**: None
- **Response**: Raw bytes in shard format (chunk data if exists)
- **Error Responses**:
  - 404 - Chunk not already tracked by global deduplication

### 4. Upload XORB

**Description**: Uploads a serialized CAS object (XORB) to the server with progress tracking. Review [how to compute xorb hash](../hashing.md#Xorb%20Hashes) to compute xorb hashes.

- **Path**: `/v1/xorb/default/{hash}`
- **Method**: `POST`
- **Parameters**:
  - `hash`: MerkleHash in hex format. Review [how to compute xorb hash](../hashing.md#Xorb%20Hashes) to compute xorb hashes.
- **Headers**:
  - `Content-Length`: Size of upload data
- **Minimum Token Scope**: `write`
- **Body**: Serialized Xorb
- **Response**: JSON (`UploadXorbResponse`)

```json
{
  "was_inserted": true
}
```

  was_inserted is false if the xorb already exists, this is not an error

### 5. Check XORB Existence

**Description**: Checks if an XORB exists in the Content Address Store (CAS). Use of this API is optional but can save the process of uploading all of the xorb data.

- **Path**: `/v1/xorb/default/{hash}`
- **Method**: `HEAD`
- **Parameters**:
  - `hash`: MerkleHash in hex format. Review [how to compute xorb hash](../hashing.md#Xorb%20Hashes) to compute xorb hashes.
- **Headers**: None (beyond authentication)
- **Minimum Token Scope**: `read`
- **Body**: None
- **Response**: Status code only
  - `200 OK`: XORB exists
  - `404 Not Found`: XORB does not exist

### 6. Upload Shard

**Description**: Uploads a shard to the CAS with optional forced synchronization.

- **Path**: `/v1/shard`
- **Method**: `POST`
- **Headers**:
  - `Content-Length`: Size of upload data
- **Minimum Token Scope**: `write`
- **Body**: Raw bytes (shard data). See [how to serialize a shard](../shard.md).
- **Response**: JSON (`UploadShardResponse`)

  ```json
  {
    "result": 0 | 1
  }
  ```

  Where 0 indicates the shard already exists and 1 indicates "SyncPerformed" meaning that the shard was registered (UploadShardResponseType).

## Common Error Cases/Codes

- **Connection Errors**: Often caused by network issues.
- **400 Bad Request**: Returned when the request parameters are invalid, e.g. invalid xorb/shard on upload API's.
- **401 Unauthorized**: Need to get a refreshed token to continue making requests.
- **404 Not Found**: Occurs on GET/HEAD api's where the resource (xorb, file) do not exist.
- **416 Range Not Satisfiable**: Returned when byte range requests are invalid; specifically the requested start range is greater than or equal to the length of the file.
- **429 Rate Limiting**: Assume there is rate limiting on all API's and lower your request rate using a backoff delay strategy
