# MDB Shard File Format Specification

## Overview

The MDB (Merkle Database) shard file format is a binary format used to store file metadata and content-addressable storage (CAS) information for efficient deduplication and retrieval. This document describes the binary layout and deserialization process for the shard format.

## File Structure

A shard file consists of the following sections in order:

```txt
┌─────────────────────┐
│ Header              │
├─────────────────────┤
│ File Info Section   │
├─────────────────────┤
│ CAS Info Section    │
├─────────────────────┤
│ File Lookup Table   │
├─────────────────────┤
│ CAS Lookup Table    │
├─────────────────────┤
│ Chunk Lookup Table  │
├─────────────────────┤
│ Footer              │
└─────────────────────┘
```

## Constants

- `MDB_SHARD_HEADER_VERSION`: 2
- `MDB_SHARD_FOOTER_VERSION`: 1
- `MDB_FILE_INFO_ENTRY_SIZE`: 48 bytes (size of each file info structure)
- `MDB_CAS_INFO_ENTRY_SIZE`: 48 bytes (size of each CAS info structure)
- `MDB_SHARD_HEADER_TAG`: 32-byte magic identifier

## Data Types

All multi-byte integers are stored in little-endian format.

- `u8`: 8-bit unsigned integer
- `u32`: 32-bit unsigned integer  
- `u64`: 64-bit unsigned integer
- `MerkleHash`: 32-byte hash value
- `HMACKey`: 32-byte HMAC key

## Deserialization Guide

### 1. Header (MDBShardFileHeader)

**Location**: Start of file (offset 0)
**Size**: 48 bytes

```rust
struct MDBShardFileHeader {
    tag: [u8; 32],           // Magic number identifier
    version: u64,            // Header version (must be 2)
    footer_size: u64,        // Size of footer in bytes
}
```

**Deserialization steps**:

1. Read 32 bytes for the magic tag
2. Verify tag matches `MDB_SHARD_HEADER_TAG`
3. Read 8 bytes for version (u64)
4. Verify version equals 2
5. Read 8 bytes for footer_size (u64)

### 2. Footer (MDBShardFileFooter)

**Location**: End of file minus footer_size
**Size**: 192 bytes

```rust
struct MDBShardFileFooter {
    version: u64,                    // Footer version (must be 1)
    file_info_offset: u64,           // Offset to file info section
    cas_info_offset: u64,            // Offset to CAS info section
    file_lookup_offset: u64,         // Offset to file lookup table
    file_lookup_num_entry: u64,      // Number of file lookup entries
    cas_lookup_offset: u64,          // Offset to CAS lookup table
    cas_lookup_num_entry: u64,       // Number of CAS lookup entries
    chunk_lookup_offset: u64,        // Offset to chunk lookup table
    chunk_lookup_num_entry: u64,     // Number of chunk lookup entries
    chunk_hash_hmac_key: [u64; 4],   // HMAC key for chunk hashes (32 bytes)
    shard_creation_timestamp: u64,   // Creation time (seconds since epoch)
    shard_key_expiry: u64,           // Expiry time (seconds since epoch)
    _buffer: [u64; 6],               // Reserved space (48 bytes)
    stored_bytes_on_disk: u64,       // Total bytes stored on disk
    materialized_bytes: u64,         // Total materialized bytes
    stored_bytes: u64,               // Total stored bytes
    footer_offset: u64,              // Offset where footer starts
}
```

**Deserialization steps**:

1. Seek to `file_size - footer_size`
2. Read all fields sequentially as u64 values
3. Verify version equals 1

### 3. File Info Section

**Location**: `footer.file_info_offset` to `footer.cas_info_offset`

This section contains a sequence of 0 or more file information blocks, each consisting of:

#### FileDataSequenceHeader

```rust
struct FileDataSequenceHeader {
    file_hash: [u64; 4],     // 32-byte file hash
    file_flags: u32,         // Flags indicating what follows
    num_entries: u32,        // Number of FileDataSequenceEntry structures
    _unused: u64,            // Reserved
}
```

**File Flags**:

- `MDB_FILE_FLAG_WITH_VERIFICATION` (0x80000000): Has verification entries
- `MDB_FILE_FLAG_WITH_METADATA_EXT` (0x40000000): Has metadata extension

#### FileDataSequenceEntry

```rust
struct FileDataSequenceEntry {
    cas_hash: [u64; 4],           // 32-byte CAS block hash
    cas_flags: u32,               // CAS flags
    unpacked_segment_bytes: u32,  // Size when unpacked
    chunk_index_start: u32,       // Start index in CAS block
    chunk_index_end: u32,         // End index in CAS block
}
```

#### FileVerificationEntry (optional)

```rust
struct FileVerificationEntry {
    range_hash: [u64; 4],    // 32-byte verification hash
    _unused: [u64; 2],       // Reserved (16 bytes)
}
```

#### FileMetadataExt (optional)

```rust
struct FileMetadataExt {
    sha256: [u64; 4],        // 32-byte SHA256 hash
    _unused: [u64; 2],       // Reserved (16 bytes)
}
```

#### File Info Bookend

The end of the file infos section is marked by a bookend entry.

The bookend entry is 48 bytes long where the first 32 bytes are all 0xFF, followed by 16 bytes of all 0x00.

Suppose you were attempting to deserialize a FileDataSequenceHeader and it's file hash was all 1 bits then this entry is a bookend entry and the next bytes start the next section.

Since the file info section immediately follows the header, a client does not need to deserialize the footer to know where it starts deserialize this section, it begins right after the header and ends when the bookend is reached.

**Deserialization steps**:

1. Seek to `footer.file_info_offset`
2. Read `FileDataSequenceHeader`
3. Check if `file_hash` is all 0xFF (bookend marker) - if so, stop
4. Read `num_entries` × `FileDataSequenceEntry` structures
5. If `file_flags & MDB_FILE_FLAG_WITH_VERIFICATION != 0`: read `num_entries` × `FileVerificationEntry`
6. If `file_flags & MDB_FILE_FLAG_WITH_METADATA_EXT != 0`: read 1 × `FileMetadataExt`
7. Repeat from step 2 until bookend found

### 4. CAS Info Section

**Location**: `footer.cas_info_offset` to `footer.file_lookup_offset`

This section contains CAS (Content Addressable Storage) block information:

#### CASChunkSequenceHeader

```rust
struct CASChunkSequenceHeader {
    cas_hash: [u64; 4],       // 32-byte CAS block hash
    cas_flags: u32,           // CAS flags
    num_entries: u32,         // Number of chunks in this CAS block
    num_bytes_in_cas: u32,    // Total bytes in CAS block
    num_bytes_on_disk: u32,   // Bytes stored on disk
}
```

#### CASChunkSequenceEntry

```rust
struct CASChunkSequenceEntry {
    chunk_hash: [u64; 4],           // 32-byte chunk hash
    unpacked_segment_bytes: u32,    // Size when unpacked
    chunk_byte_range_start: u32,    // Start position in CAS block
    _unused: u64,                   // Reserved
}
```

**Deserialization steps**:

1. Seek to `footer.cas_info_offset`
2. Read `CASChunkSequenceHeader`
3. Check if `cas_hash` is all 0xFF (bookend marker) - if so, stop
4. Read `num_entries` × `CASChunkSequenceEntry` structures
5. Repeat from step 2 until bookend found

#### CAS Info Bookend

The end of the cas infos section is marked by a bookend entry.

The bookend entry is 48 bytes long where the first 32 bytes are all 0xFF, followed by 16 bytes of all 0x00.

Suppose you were attempting to deserialize a CASChunkSequenceHeader and it's hash was all 1 bits then this entry is a bookend entry and the next bytes start the next section.

Since the file info section immediately follows the file info section, a client does not need to deserialize the footer to know where it starts deserialize this section, it begins right after the file info section bookend and ends when the cas info bookend is reached.

### 5. Lookup Tables

#### File Lookup Table

**Location**: `footer.file_lookup_offset`
**Size**: `footer.file_lookup_num_entry` entries

> Note that this section is optional, if this section is not present, then `footer.file_lookup_num_entry` is 0.

Each entry is 12 bytes:

```rust
struct FileLookupEntry {
    truncated_hash: u64,     // First 8 bytes of file hash
    file_index: u32,         // Index into file info section
}
```

#### CAS Lookup Table

**Location**: `footer.cas_lookup_offset`
**Size**: `footer.cas_lookup_num_entry` entries

> Note that this section is optional, if this section is not present, then `footer.cas_lookup_num_entry` is 0.

Each entry is 12 bytes:

```rust
struct CASLookupEntry {
    truncated_hash: u64,     // First 8 bytes of CAS hash
    cas_index: u32,          // Index into CAS info section
}
```

#### Chunk Lookup Table

**Location**: `footer.chunk_lookup_offset`
**Size**: `footer.chunk_lookup_num_entry` entries

> Note that this section is optional, if this section is not present, then `footer.chunk_lookup_num_entry` is 0.

Each entry is 16 bytes:

```rust
struct ChunkLookupEntry {
    truncated_hash: u64,     // First 8 bytes of chunk hash
    cas_index: u32,          // Index into CAS info section
    chunk_index: u32,        // Index within CAS block
}
```

## HMAC Key Protection

If `footer.chunk_hash_hmac_key` is non-zero, chunk hashes are protected with HMAC:

- The stored chunk hashes are HMAC(original_hash, hmac_key)
- To verify chunks, compute HMAC of the original hash and compare

## Complete Deserialization Algorithm

```rust
fn deserialize_shard(file: &mut File) -> Result<ShardData> {
    // 1. Read and validate header
    file.seek(SeekFrom::Start(0))?;
    let header = read_header(file)?;
    
    // 2. Read and validate footer
    file.seek(SeekFrom::End(-(header.footer_size as i64)))?;
    let footer = read_footer(file)?;
    
    // 3. Read file info section
    file.seek(SeekFrom::Start(footer.file_info_offset))?;
    let file_info = read_file_info_section(file, footer.cas_info_offset)?;
    
    // 4. Read CAS info section
    file.seek(SeekFrom::Start(footer.cas_info_offset))?;
    let cas_info = read_cas_info_section(file, footer.file_lookup_offset)?;
    
    // 5. Read lookup tables
    let file_lookup = read_file_lookup(file, &footer)?;
    let cas_lookup = read_cas_lookup(file, &footer)?;
    let chunk_lookup = read_chunk_lookup(file, &footer)?;
    
    Ok(ShardData {
        header,
        footer,
        file_info,
        cas_info,
        file_lookup,
        cas_lookup,
        chunk_lookup,
    })
}
```

## Usage Patterns

### Finding a File by Hash

1. Truncate file hash to first 8 bytes
2. Binary search in file lookup table
3. For each matching entry, read the full file info and verify complete hash
4. Return file reconstruction information

### Finding Chunks for Deduplication

1. Truncate chunk hash to first 8 bytes (apply HMAC if needed)
2. Binary search in chunk lookup table
3. For each match, read CAS block and verify full chunk hash
4. Return matching chunk information

### Reconstructing a File

1. Look up file hash in file lookup table
2. Read file info to get list of CAS blocks and chunk ranges
3. For each CAS reference, read the corresponding chunks
4. Concatenate chunk data in order to reconstruct file

## Version Compatibility

- Header version 2: Current format
- Footer version 1: Current format
- Files with different versions should be rejected during deserialization

## Error Handling

- Always verify magic numbers and versions
- Check that offsets are within file bounds  
- Verify that bookend markers are present where expected
- Validate that lookup tables are sorted by truncated hash
