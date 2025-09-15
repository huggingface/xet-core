# mdb_shard

> MDB -> Merkle Database

The mdb_shard crate exposes multiple interfaces for working with shards.

This includes particularly the shard file format as used as API payloads as well as used internally within xet-core to
manage and store state during and between processes to deduplicate and upload data.

## Serialization and Deserialization Interfaces

The mdb_shard crate provides multiple interfaces for serializing and deserializing shard data, organized by their purpose and usage patterns.
These interfaces allow you to work with shard data at different levels of abstraction, from low-level binary serialization to high-level streaming processing.

### Core Shard Format Interfaces

[**`src/shard_format.rs`**](src/shard_format.rs)

These interfaces handle the core shard file format and metadata:

- **`MDBShardInfo::load_from_reader()`** - Loads complete shard metadata (header + footer) from a reader
- **`MDBShardInfo::serialize_from()`** - Serializes an in-memory shard to binary format

### Streaming and Processing Interfaces

[**`src/streaming_shard.rs`**](src/streaming_shard.rs)

- **`MDBMinimalShard::from_reader()`** - Creates a minimal shard representation for lightweight operations from a reader
- **`MDBMinimalShard::from_reader_async()`** - Creates a minimal shard representation for lightweight operations, from an async reader

### File Handle Interfaces

[**`src/shard_file_handle.rs`**](src/shard_file_handle.rs)

- **`MDBShardFile::load_from_file()`** - Loads shard from a file path with caching
