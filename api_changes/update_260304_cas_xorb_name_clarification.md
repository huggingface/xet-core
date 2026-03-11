# API Updates: `cas` → `xorb` Terminology Rename

## Background

In this codebase, "CAS" (Content-Addressable Storage) and "XORB" (the data object) were previously used interchangeably. They have been separated:

- **`xorb`**: The data object itself, indexed by its content hash. All types, fields, methods, and crate names that refer to the data object now use `xorb`.
- **`cas`**: The remote Content-Addressable Storage server/client infrastructure. Names referring to the server, client, endpoints, configuration, and errors related to the remote service remain `cas` (e.g., `cas_client`, `cas_types`, `CasClientError`).

## Crate Rename

| Old Crate Name | New Crate Name | Notes |
|---|---|---|
| `cas_object` | `xorb_object` | Directory renamed, Cargo.toml `name` updated. All `use cas_object::*` → `use xorb_object::*`. |

The following crates were **NOT** renamed (they refer to the CAS server/client):
- `cas_client` (remote client)
- `cas_types` (shared types for CAS API)

## File Renames

| Old Path | New Path |
|---|---|
| `cas_object/` | `xorb_object/` |
| `cas_object/src/cas_chunk_format.rs` | `xorb_object/src/xorb_chunk_format.rs` |
| `cas_object/src/cas_chunk_format/deserialize_async.rs` | `xorb_object/src/xorb_chunk_format/deserialize_async.rs` |
| `cas_object/src/cas_object_format.rs` | `xorb_object/src/xorb_object_format.rs` |
| `mdb_shard/src/cas_structs.rs` | `mdb_shard/src/xorb_structs.rs` |

## Type and Struct Renames

### `xorb_object` crate (formerly `cas_object`)

| Old Name | New Name |
|---|---|
| `CasObjectError` | `XorbObjectError` |
| `CasObjectIdent` | `XorbObjectIdent` |
| `CasObjectInfoV0` | `XorbObjectInfoV0` |
| `CasObjectInfoV1` | `XorbObjectInfoV1` |
| `CasObject` | `XorbObject` |
| `SerializedCasObject` | `SerializedXorbObject` |
| `CASChunkHeader` | `XorbChunkHeader` |

### `mdb_shard` crate

| Old Name | New Name |
|---|---|
| `CASChunkSequenceHeader` | `XorbChunkSequenceHeader` |
| `CASChunkSequenceEntry` | `XorbChunkSequenceEntry` |
| `MDBCASInfo` | `MDBXorbInfo` |
| `MDBCASInfoView` | `MDBXorbInfoView` |

### `cas_types` crate

| Old Name | New Name |
|---|---|
| `CASReconstructionTerm` | `XorbReconstructionTerm` |
| `CASReconstructionFetchInfo` | `XorbReconstructionFetchInfo` |

### `cas_client::error::CasClientError`

| Old Variant | New Variant |
|---|---|
| `CasObjectError` | `XorbObjectError` |

## Constant Renames

### `xorb_object` crate

| Old Name | New Name |
|---|---|
| `CAS_CHUNK_HEADER_LENGTH` | `XORB_CHUNK_HEADER_LENGTH` |
| `CAS_OBJECT_FORMAT_IDENT` | `XORB_OBJECT_FORMAT_IDENT` |
| `CAS_OBJECT_FORMAT_VERSION` | `XORB_OBJECT_FORMAT_VERSION` |
| `CAS_OBJECT_FORMAT_VERSION_V0` | `XORB_OBJECT_FORMAT_VERSION_V0` |
| `CAS_OBJECT_FORMAT_IDENT_HASHES` | `XORB_OBJECT_FORMAT_IDENT_HASHES` |
| `CAS_OBJECT_FORMAT_IDENT_BOUNDARIES` | `XORB_OBJECT_FORMAT_IDENT_BOUNDARIES` |
| `CAS_OBJECT_FORMAT_HASHES_VERSION` | `XORB_OBJECT_FORMAT_HASHES_VERSION` |
| `CAS_OBJECT_FORMAT_BOUNDARIES_VERSION` | `XORB_OBJECT_FORMAT_BOUNDARIES_VERSION` |
| `CAS_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO` | `XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO` |

### `mdb_shard` crate

| Old Name | New Name |
|---|---|
| `MDB_DEFAULT_CAS_FLAG` | `MDB_DEFAULT_XORB_FLAG` |

## Module Path Changes

| Old Module Path | New Module Path |
|---|---|
| `mdb_shard::cas_structs` | `mdb_shard::xorb_structs` |
| `mdb_shard::cas_structs::CASChunkSequenceHeader` | `mdb_shard::xorb_structs::XorbChunkSequenceHeader` |
| `mdb_shard::cas_structs::CASChunkSequenceEntry` | `mdb_shard::xorb_structs::XorbChunkSequenceEntry` |
| `mdb_shard::cas_structs::MDBCASInfo` | `mdb_shard::xorb_structs::MDBXorbInfo` |
| `mdb_shard::cas_structs::MDBCASInfoView` | `mdb_shard::xorb_structs::MDBXorbInfoView` |

## Field Renames

### `XorbObjectInfoV0` / `XorbObjectInfoV1` (in `xorb_object`)

| Old Field | New Field |
|---|---|
| `cashash` | `xorb_hash` |

### `XorbChunkSequenceHeader` (in `mdb_shard::xorb_structs`)

| Old Field | New Field |
|---|---|
| `cas_hash` | `xorb_hash` |
| `num_bytes_in_cas` | `num_bytes_in_xorb` |

### `XorbChunkSequenceEntry` (in `mdb_shard::xorb_structs`)

| Old Field | New Field |
|---|---|
| `cas_flags` | `xorb_flags` |

## Method Renames

### `mdb_shard::shard_format::MDBShardInfo`

| Old Method | New Method |
|---|---|
| `get_cas_info_index_by_hash()` | `get_xorb_info_index_by_hash()` |
| `get_cas_info_index_by_chunk()` | `get_xorb_info_index_by_chunk()` |
| `read_all_cas_blocks()` | `read_all_xorb_blocks()` |
| `read_all_cas_blocks_full()` | `read_all_xorb_blocks_full()` |
| `read_full_cas_lookup()` | `read_full_xorb_lookup()` |
| `num_cas_entries()` | `num_xorb_entries()` |
| `cas_info_byte_range()` | `xorb_info_byte_range()` |
| `cas_lookup_byte_range()` | `xorb_lookup_byte_range()` |

### `mdb_shard::shard_format` (free functions, test utilities)

| Old Function | New Function |
|---|---|
| `gen_random_shard_with_cas_references()` | `gen_random_shard_with_xorb_references()` |
| `gen_random_file_info_with_cas_references()` | `gen_random_file_info_with_xorb_references()` |

### `mdb_shard::streaming_shard`

| Old Function/Method | New Function/Method |
|---|---|
| `process_shard_cas_info_section()` | `process_shard_xorb_info_section()` |
| `StreamingShardReader::num_cas()` | `StreamingShardReader::num_xorb()` |
| `StreamingShardReader::cas()` | `StreamingShardReader::xorb()` |

### `mdb_shard::shard_file_handle::MDBShardFile`

| Old Method | New Method |
|---|---|
| `read_all_cas_blocks()` | `read_all_xorb_blocks()` |
| `read_full_cas_lookup()` | `read_full_xorb_lookup()` |

### `mdb_shard::shard_in_memory::MDBInMemoryShard`

| Old Method | New Method |
|---|---|
| `add_cas_block()` | `add_xorb_block()` |
| `num_cas_entries()` | `num_xorb_entries()` |

### `mdb_shard::file_structs::FileDataSequenceEntry`

| Old Method | New Method |
|---|---|
| `from_cas_entries()` | `from_xorb_entries()` |

### `mdb_shard::streaming_shard::StreamingShardReader`

| Old Parameter (`from_reader`) | New Parameter |
|---|---|
| `include_cas: bool` | `include_xorb: bool` |

### `data::shard_interface::SessionShardInterface`

| Old Method | New Method |
|---|---|
| `add_cas_block()` | `add_xorb_block()` |
| `add_uploaded_cas_block()` | `add_uploaded_xorb_block()` |

### `MDBXorbInfoView` (in `mdb_shard::xorb_structs`)

| Old Method | New Method |
|---|---|
| `cas_hash()` | `xorb_hash()` |

## Trait Method Signature Changes

### `cas_client::interface::Client`

```rust
// Old:
async fn upload_xorb(
    &self,
    prefix: &str,
    serialized_cas_object: SerializedCasObject,  // ← renamed param and type
    progress_callback: Option<ProgressCallback>,
    upload_permit: ConnectionPermit,
) -> Result<u64>;

// New:
async fn upload_xorb(
    &self,
    prefix: &str,
    serialized_xorb_object: SerializedXorbObject,  // ← new param name and type
    progress_callback: Option<ProgressCallback>,
    upload_permit: ConnectionPermit,
) -> Result<u64>;
```

## Import Changes Summary

When updating downstream code, replace these import patterns:

```rust
// Crate import
use cas_object::*;          →  use xorb_object::*;

// Module import
use mdb_shard::cas_structs;          →  use mdb_shard::xorb_structs;
use mdb_shard::cas_structs::*;       →  use mdb_shard::xorb_structs::*;

// Specific type imports
use cas_object::CasObject;           →  use xorb_object::XorbObject;
use cas_object::SerializedCasObject; →  use xorb_object::SerializedXorbObject;
use cas_object::CasObjectError;      →  use xorb_object::XorbObjectError;
use cas_object::CasObjectInfoV0;     →  use xorb_object::XorbObjectInfoV0;
use cas_object::CasObjectInfoV1;     →  use xorb_object::XorbObjectInfoV1;
use cas_object::CASChunkHeader;      →  use xorb_object::XorbChunkHeader;

use mdb_shard::cas_structs::CASChunkSequenceHeader;  →  use mdb_shard::xorb_structs::XorbChunkSequenceHeader;
use mdb_shard::cas_structs::CASChunkSequenceEntry;   →  use mdb_shard::xorb_structs::XorbChunkSequenceEntry;
use mdb_shard::cas_structs::MDBCASInfo;              →  use mdb_shard::xorb_structs::MDBXorbInfo;
use mdb_shard::cas_structs::MDBCASInfoView;          →  use mdb_shard::xorb_structs::MDBXorbInfoView;

use cas_types::CASReconstructionTerm;      →  use cas_types::XorbReconstructionTerm;
use cas_types::CASReconstructionFetchInfo; →  use cas_types::XorbReconstructionFetchInfo;
```

## What Was NOT Renamed

The following names remain unchanged because they refer to the CAS **server/client infrastructure**, not the data object:

- Crate names: `cas_client`, `cas_types`
- Error types: `CasClientError`
- Client trait: `cas_client::Client`
- Server endpoints and configuration: `CAS_ENDPOINT`, `cas_url`, `cas_path`, `cas_dir`
- The `upload_xorb` method name (already used `xorb`)
- `UploadXorbResponse` (already used `xorb`)
- `MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG` (not `cas`-specific)
- `XORB_BLOCK_SIZE` constant (already used `xorb`)

## Metric Name Change

| Old Metric | New Metric |
|---|---|
| `filter_process_cas_bytes_produced` | `filter_process_xorb_bytes_produced` |

## Cargo.toml Dependency Updates

Any `Cargo.toml` that depended on `cas_object` must update:

```toml
# Old
cas_object = { path = "../cas_object" }

# New
xorb_object = { path = "../xorb_object" }
```

The root `Cargo.toml` workspace members list was updated from `"cas_object"` to `"xorb_object"`.

## Serialization Compatibility

- **Binary format**: The `mdb_shard` structs (`XorbChunkSequenceHeader`, `XorbChunkSequenceEntry`, `MDBXorbInfo`) use custom binary serialization. The field renames do not affect the binary wire format since serialization is positional, not name-based.
- **JSON/serde format**: The `cas_types` structs (`XorbReconstructionTerm`, `XorbReconstructionFetchInfo`) use `serde` for JSON serialization. The struct renames do not change JSON field names since `serde` serializes based on field names (which were not changed in `cas_types`) rather than struct names.
- **XORB binary format**: The on-disk/wire format of XORB objects is unchanged. The renamed constants (`XORB_OBJECT_FORMAT_IDENT`, etc.) retain the same byte values.
