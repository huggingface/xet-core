# Upload protocol

This document describes how files are uploaded to the Content Addressable Storage (CAS) service. The flow converts input files into chunks, applies deduplication, groups chunks into xorbs, uploads xorbs, then forms and uploads shards that reference those xorbs. Content addressing uses hashes as stable keys for deduplication and integrity verification.

## Steps

### 1. Chunking

    - Input file bytes are partitioned into variable-length chunks as defined in the chunking specification. See: [chunking.md](../spec/chunking.md).
    - During this step, the system also computes each chunk's content hash (its key). See hashing details: [hashing.md](../spec/hashing.md).

### 2. Local deduplication

    - For some chunks, the client checks if already has seen the given chunk hash to determine if identical chunks already exist and can be reused. See: [deduplication.md](../spec/deduplication.md#level-1-local-session-deduplication).
    - Note that Deduplication is considered an optimization and is not a required component of the upload process, however it provides potential resource saving.

### 3. Global deduplication

    - For some chunks, the client queries a server API that returns a secure shard sketch/summary to determine if duplicates exist remotely. Matching chunks may be skipped for upload. See: [deduplication.md](../spec/deduplication.md) and the API overview: [api.md](../spec/api.md#2-query-chunk-deduplication-global-deduplication).
    - Note that Deduplication is considered an optimization and is not a required component of the upload process, however it provides potential resource saving.

### 4. Xorb formation

    - Contiguous runs of chunks are collected into xorbs (roughly 64 MiB total length per xorb), preserving order within each run. See formation rules: [xorb_formation.md](../spec/xorb_formation.md#collecting-chunks).

### 5. Xorb hashing

    - The xorb's content-addressed key is computed using the chunks in the xorb. See: [hashing.md](../spec/hashing.md).

### 6. Xorb serialization

    - Each xorb is serialized into its binary representation as defined by the xorb format. See: [xorb_formation.md](../spec/xorb_formation.md).

### 7. Xorb upload

    - The client uploads each xorb via a POST endpoint on the CAS server. Refer to API details: [api.md](../spec/api.md).

### 8. Shard formation, collect required components

    - Map each file to a reconstruction using available xorbs; compute file hashes.
    - Collect only new xorbs (omit those already present on the server via global dedupe).

### 9. Shard serialization

    - The shard is serialized to its binary on-disk/over-the-wire representation. See: [shard.md](../spec/shard.md).

### 10. Shard upload

    - The client uploads the shard via a POST endpoint on the CAS server. For this to succeed, all xorbs referenced by the shard must have already completed uploading. This API records files as uploaded. See: [api.md](../spec/api.md#4-upload-shard).

After all xorbs and all shards are successfully uploaded, the full upload is considered complete.
Files can then be downloaded by any client using the [download protocol](../spec/download_protocol.md).

## Ordering and concurrency

- Xorbs that do not deduplicate can be uploaded independently and in parallel after they are formed and serialized.
- Shard upload is strictly ordered after all of its referenced xorbs have been uploaded successfully.
- Multiple shards (for different groups of files) may be formed and uploaded independently, provided their referenced xorbs are available.

## Integrity and idempotency

- Hashing of chunks, xorbs, and shards ensures integrity and enables deduplication across local and global scopes. See: [hashing.md](../spec/hashing.md).
- Upload endpoints should be idempotent with respect to content-addressed keys; re-sending an already-present xorb or shard should be safe. See API expectations: [api.md](../spec/api.md).

TODO: add more notes
