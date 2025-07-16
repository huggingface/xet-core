# CAS API Documentation

This document describes the HTTP API endpoints used by the CAS (Content Addressable Storage) client to interact with the remote CAS server.

## Base Configuration

- **Default Endpoint**: `http://localhost:8080`
- **Default Prefix**: `default`

## Authentication

Most endpoints require authentication through headers added by the authenticated HTTP client.

## Endpoints

### 1. Get File Reconstruction

**Description**: Retrieves reconstruction information for a specific file, optionally with byte range support.

- **Path**: `/reconstruction/{file_id}`
- **Method**: `GET`
- **Parameters**:
  - `file_id`: MerkleHash in hex format
- **Headers**:
  - `Range`: Optional. Format: `bytes={start}-{end}` (inclusive end)
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
  - `416 Range Not Satisfiable`: When requested byte range is invalid

### 2. Batch Get Reconstruction

**Description**: Retrieves reconstruction information for multiple files in a single request.

- **Path**: `/reconstructions`
- **Method**: `GET`
- **Query Parameters**:
  - `file_id`: MerkleHash(es) in hex format (can be repeated)
  - Example: `/reconstructions?file_id=abc123&file_id=def456`
- **Headers**: None (beyond authentication)
- **Body**: None
- **Response**: JSON (`BatchQueryReconstructionResponse`)

### 3. Query Chunk Deduplication (Global Deduplication)

**Description**: Checks if a chunk exists in the CAS for deduplication purposes.

- **Path**: `/chunk/{key}`
- **Method**: `GET`
- **Parameters**:
  - `key`: Formatted as `{prefix}:{hash}` where hash is MerkleHash in hex
- **Headers**: None (beyond authentication)
- **Body**: None
- **Response**: Raw bytes (chunk data if exists)
- **Error Responses**:
  - 404 - Chunk not already tracked by global deduplication

### 4. Upload XORB

**Description**: Uploads a serialized CAS object (XORB) to the server with progress tracking.

- **Path**: `/xorb/{key}`
- **Method**: `POST`
- **Parameters**:
  - `key`: Formatted as `{prefix}/{hash}` where hash is MerkleHash in hex
- **Headers**:
  - `Content-Length`: Size of upload data (required for streaming)
- **Body**: Serialized Xorb
- **Response**: JSON (`UploadXorbResponse`)

```json
{
  "was_inserted": true
}
```

  was_inserted is false if the xorb already exists, this is not an error

### 6. Check XORB Existence

**Description**: Checks if an XORB exists in the CAS without downloading it.

- **Path**: `/xorb/{key}`
- **Method**: `HEAD`
- **Parameters**:
  - `key`: Formatted as `{prefix}/{hash}` where hash is MerkleHash in hex
- **Headers**: None (beyond authentication)
- **Body**: None
- **Response**: Status code only
  - `200 OK`: XORB exists
  - `404 Not Found`: XORB does not exist

### 7. Upload Shard

**Description**: Uploads a shard to the CAS with optional forced synchronization.

- **Path**: `/shard/{key}`
- **Method**: `POST`/`PUT`
- **Parameters**:
  - `key`: Formatted as `{prefix}/{hash}` where hash is MerkleHash in hex
- **Headers**: None (beyond authentication)
- **Body**: Raw bytes (shard data)
- **Response**: JSON (`UploadShardResponse`)

  ```json
  {
    "result": "Exists" | "SyncPerformed"
  }
  ```

## Data Types

### Key Format

Keys are formatted as `{prefix}/{hash}` where:

- `prefix`: String identifier (default: "default")
- `hash`: MerkleHash represented as 64-character hex string

### Common Response Types

- **QueryReconstructionResponse**: Contains file reconstruction metadata including terms and fetch information
- **BatchQueryReconstructionResponse**: Contains multiple reconstruction responses
- **UploadXorbResponse**: Indicates whether the upload resulted in a new insertion
- **UploadShardResponse**: Indicates the result of shard upload operation

## Error Handling

The client implements retry logic for most endpoints with configurable retry policies. Common error scenarios:

- **Connection Errors**: Handled gracefully, often returning `None` or empty results
- **416 Range Not Satisfiable**: Returned when byte range requests are invalid
- **Authentication Errors**: Handled by the authenticated HTTP client middleware
- **Rate Limiting (429)**: Some endpoints have specific no-retry policies for 429 responses
