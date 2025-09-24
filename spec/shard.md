# MDB Shard File Format Specification

A Shard is a serialized object containing file reconstruction information and xorb metadata for deduplication purposes.

The Shard format is the vehicle for uploading the file reconstruction upload and communicating information about xorbs and chunks that clients can deduplicate their data against.

## Overview

The MDB (Merkle Database) shard file format is a binary format used to store file metadata and content-addressable storage (CAS) information for efficient deduplication and retrieval.
This document describes the binary layout and deserialization process for the shard format.
Implementors of the xet protocol MUST use the shard format when implementing the [upload protocol](../spec/upload_protocol.md).
The shard format is used on the shard upload (record files) and global deduplication APIs.

## Use As API Request and Response Bodies

The shard format is used in the shard upload API as the request payload and in the global deduplication/chunk query API as the response payload.

### Shard Upload

The shard in this case is a serialization format that allows clients to denote the files that they are uploading.
Each file reconstruction maps to a File Info block in the File Info section.
Additionally, the listing of all new xorbs that the client created are mapped to items (CAS Info blocks) in the CAS Info section so that they may be deduplicated against in the future.

When uploading a shard the footer section MUST be omitted.

An example of a shard that can be used for file upload can be found in [Xet reference files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.shard.verification-no-footer).
A version of this shard that also contains the footer in [Xet reference files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.shard.verification) too, see the README for the reference files dataset for more context.

### Global Deduplication

Shards returned by the Global Deduplication API have an empty File Info Section, and only contain relevant information in the CAS Info section.
The CAS Info section returned by this API contains xorbs, where a xorb described in the CAS Info section contains the chunk that was queried.
Clients can deduplicate their content against any of the other xorbs described in any CAS Info block in the CAS Info section of the returned shard.
Other xorb descriptions returned in a shard are possibly more likely to reference content that the client has.

An example of a shard that can be returned for a global deduplication query can be found in [Xet reference files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.shard.dedupe).

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
│ Footer              │
└─────────────────────┘
```

## Overall File Layout with Byte Offsets

```txt
Offset 0:
┌───────────────────────────────────────────────────────┐
│                 Header (48 bytes)                     │ ← Fixed size
└───────────────────────────────────────────────────────┘

Offset footer.file_info_offset:
┌───────────────────────────────────────────────────────┐
│                                                       │
│              File Info Section                        │ ← Variable size
│            (Multiple file blocks +                    │
│               bookend entry)                          │
│                                                       │
└───────────────────────────────────────────────────────┘

Offset footer.cas_info_offset:
┌───────────────────────────────────────────────────────┐
│                                                       │
│               CAS Info Section                        │ ← Variable size
│            (Multiple CAS blocks +                     │
│               bookend entry)                          │
│                                                       │
└───────────────────────────────────────────────────────┘

Offset footer.footer_offset:
┌───────────────────────────────────────────────────────┐
│         Footer (200 bytes, sometimes omitted)         │ ← Fixed size
└───────────────────────────────────────────────────────┘
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
- Byte Array types are denoted like in rust as `[u8; N]` where `N` is the number of bytes in the array.
- Hash: 32-byte hash value, a special `[u8; 32]`

## 1. Header (MDBShardFileHeader)

**Location**: Start of file (offset 0)
**Size**: 48 bytes

```rust
struct MDBShardFileHeader {
    tag: [u8; 32],           // Magic number identifier
    version: u64,            // Header version (must be 2)
    footer_size: u64,        // Size of footer in bytes, set to 0 if footer is omitted
}
```

**Memory Layout**:

```txt
┌────────────────────────────────────────────────────────────────┬───────────┬───────────┐
│                          tag (32 bytes)                        │  version  │ footer_sz │
│                    Magic Number Identifier                     │ (8 bytes) │ (8 bytes) │
└────────────────────────────────────────────────────────────────┴───────────┴───────────┘
0                                                               32          40         48
```

**Deserialization steps**:

1. Read 32 bytes for the magic tag
2. Verify tag matches `MDB_SHARD_HEADER_TAG`
3. Read 8 bytes for version (u64)
4. Verify version equals 2
5. Read 8 bytes for footer_size (u64)

> when serializing, footer_size MUST be the number of bytes that make up the footer, or 0 if the footer is omitted.

## 2. File Info Section

**Location**: `footer.file_info_offset` to `footer.cas_info_offset` or directly after the header

This section contains a sequence of 0 or more file information (File Info) blocks, each consisting at least a header and at least 1 data sequence entry, and OPTIONAL verification entries and metadata extension section.
The file info section ends when reaching the bookend entry.

Each File Info block within the overall section is a serialization of a [file reconstruction](../spec/file_reconstruction.md) into a binary format.
For each file, there is a `FileDataSequenceHeader` and for each term a `FileDataSequenceEntry` with OPTIONAL a matching `FileVerificationEntry` and also OPTIONAL at the end a `FileMetadataExt`.

A shard File Info section can contain more than 1 File Info block in series, after completing reading all the content for 1 file description, the next one immediately begins.
If when reading the header of the next section a reader encounters the bookend entry that means the file info section is over; you have read the last file description in this shard.

### File Info Section Layout

**Without Optional Components**:

```txt
┌─────────────────────┐
│ FileDataSeqHeader   │ ← File 1
├─────────────────────┤
│ FileDataSeqEntry    │
├─────────────────────┤
│ FileDataSeqEntry    │
├─────────────────────┤
│        ...          │
├─────────────────────┤
│ FileDataSeqHeader   │ ← File 2
├─────────────────────┤
│ FileDataSeqEntry    │
├─────────────────────┤
│        ...          │
├─────────────────────┤
│   Bookend Entry     │ ← All 0xFF hash + zeros
└─────────────────────┘
```

**With All Optional Components**:

```txt
┌─────────────────────┐
│ FileDataSeqHeader   │ ← File 1 (flags indicate verification + metadata)
├─────────────────────┤
│ FileDataSeqEntry    │
├─────────────────────┤
│ FileDataSeqEntry    │
├─────────────────────┤
│        ...          │
├─────────────────────┤
│ FileVerifyEntry     │ ← One per FileDataSeqEntry
├─────────────────────┤
│ FileVerifyEntry     │
├─────────────────────┤
│        ...          │
├─────────────────────┤
│ FileMetadataExt     │ ← One per file (if flag set)
├─────────────────────┤
│ FileDataSeqHeader   │ ← File 2
├─────────────────────┤
│        ...          │
├─────────────────────┤
│   Bookend Entry     │ ← All 0xFF hash + zeros
└─────────────────────┘
```

### FileDataSequenceHeader

```rust
struct FileDataSequenceHeader {
    file_hash: Hash,      // 32-byte file hash
    file_flags: u32,      // Flags indicating conditional sections that follow
    num_entries: u32,     // Number of FileDataSequenceEntry structures
    _unused: [u8; 8],     // Reserved space 8 bytes
}
```

**File Flags**:

- `MDB_FILE_FLAG_WITH_VERIFICATION` (0x80000000 or 1 << 31): Has verification entries
- `MDB_FILE_FLAG_WITH_METADATA_EXT` (0x40000000 or 1 << 30): Has metadata extension

Given the `file_data_sequence_header.file_flags & MASK` (bitwise AND) operations, if the result != 0 then the effect is true.

**Memory Layout**:

```txt
┌────────────────────────────────────────────────────────────────┬──────────┬───────────┬────────────┐
│                       file_hash (32 bytes)                     │file_flags│num_entries│   _unused  │
│                        File Hash Value                         │(4 bytes) │(4 bytes)  │  (8 bytes) │
└────────────────────────────────────────────────────────────────┴──────────┴───────────┴────────────┘
0                                                                32         36         40           48
```

### FileDataSequenceEntry

Each `FileDataSequenceEntry` is 1 term is essentially the binary serialization of a [file reconstruction term](../spec/file_reconstruction.md#term-format).

```rust
struct FileDataSequenceEntry {
    cas_hash: Hash,               // 32-byte Xorb hash in the term
    cas_flags: u32,               // CAS flags (reserved for future, set to 0)
    unpacked_segment_bytes: u32,  // Term size when unpacked
    chunk_index_start: u32,       // Start chunk index within the Xorb for the term
    chunk_index_end: u32,         // End chunk index (exclusive) within the Xorb for the term
}
```

> Note that when describing a chunk range in a `FileDataSequenceEntry` use ranges that are start-inclusive but end-exclusive i.e. `[chunk_index_start, chunk_index_end)`

**Memory Layout**:

```txt
┌────────────────────────────────────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
│                       cas_hash (32 bytes)                      │cas_flags│unpacked │chunk_idx│chunk_idx│
│                      CAS Block Hash                            │(4 bytes)│seg_bytes│start    │end      │
│                                                                │         │(4 bytes)│(4 bytes)│(4 bytes)│
└────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┘
0                                                               32        36        40        44        48
```

### FileVerificationEntry (OPTIONAL)

Verification Entries MUST be set for shard uploads.

To generate verification hashes for shard upload read the section about [Verification Hashes](../hashing.md#Term%20Verification%20Hashes).

```rust
struct FileVerificationEntry {
    range_hash: Hash,   // 32-byte verification hash
    _unused: [u8; 16],  // Reserved (16 bytes)
}
```

**Memory Layout**:

```txt
┌────────────────────────────────────────────────────────────────┬────────────────────────────────┐
│                    range_hash (32 bytes)                       │       _unused (16 bytes)       │
│                      Verification Hash                         │         Reserved Space         │
└────────────────────────────────────────────────────────────────┴────────────────────────────────┘
0                                                              32                               48
```

When a shard has verification entries, all file info sections MUST have verification entries.
If only some subset of files in the shard have verification entries, the shard is considered invalid.
Every `FileDataSequenceEntry` will have a matching `FileVerificationEntry` in this case where the range_hash is computed with the chunk hashes for that range of chunks.

For any file the nth `FileVerificationEntry` correlates to the nth `FileDataSequenceEntry`, and like `FileDataSequenceEntries` if there are verification entries there will be `file_data_sequence_header.num_entries` verification entries (following the num_entries data sequence entries).

### FileMetadataExt (OPTIONAL)

This section is REQUIRED per file for shards uploaded through the shard upload API.

There is only 1 `FileMetadataExt` instance per file info block and it is the last component of that file info block when present.

The sha256 field is the 32 byte SHA256 of the file contents of the file described.

```rust
struct FileMetadataExt {
    sha256: Hash,      // 32-byte SHA256 hash
    _unused: [u8; 16], // Reserved (16 bytes)
}
```

**Memory Layout**:

```txt
┌────────────────────────────────────────────────────────────────┬────────────────────────────────┐
│                      sha256 (32 bytes)                         │       _unused (16 bytes)       │
│                     SHA256 Hash                                │         Reserved Space         │
└────────────────────────────────────────────────────────────────┴────────────────────────────────┘
0                                                               32                               48
```

### File Info Bookend

The end of the file info sections is marked by a bookend entry.

The bookend entry is 48 bytes long where the first 32 bytes are all `0xFF`, followed by 16 bytes of all `0x00`.

Suppose you were attempting to deserialize a `FileDataSequenceHeader` and it's file hash was all 1 bits then this entry is a bookend entry and the next bytes start the next section.

Since the file info section immediately follows the header, a client MAY skip deserializing the footer to know where it starts deserializing this section.
The file info section begins right after the header and ends when the bookend is reached.

**Deserialization steps**:

1. Seek to `footer.file_info_offset`
2. Read `FileDataSequenceHeader`
3. Check if `file_hash` is all `0xFF` (bookend marker) - if so, stop
4. Read `file_data_sequence_header.num_entries` × `FileDataSequenceEntry` structures
5. If `file_flags & MDB_FILE_FLAG_WITH_VERIFICATION != 0`: read `file_data_sequence_header.num_entries` × `FileVerificationEntry`
6. If `file_flags & MDB_FILE_FLAG_WITH_METADATA_EXT != 0`: read 1 × `FileMetadataExt`
7. Repeat from step 2 until bookend found

## 3. CAS Info Section

**Location**: `footer.cas_info_offset` to `footer.footer_offset` or directly after the file info section bookend

This section contains CAS (Content Addressable Storage) block information. Each CAS Info block represents a xorb by first having a `CASChunkSequenceHeader` which contains the number of `CASChunkSequenceEntries` to follow that make up this block. The CAS Info section ends when reaching the bookend entry.

### CAS Info Section Layout

```txt
┌─────────────────────┐
│ CASChunkSeqHeader   │ ← CAS Block 1
├─────────────────────┤
│ CASChunkSeqEntry    │
├─────────────────────┤
│ CASChunkSeqEntry    │
├─────────────────────┤
│        ...          │
├─────────────────────┤
│ CASChunkSeqHeader   │ ← CAS Block 2
├─────────────────────┤
│ CASChunkSeqEntry    │
├─────────────────────┤
│        ...          │
├─────────────────────┤
│   Bookend Entry     │ ← All 0xFF hash + zeros
└─────────────────────┘
```

**Deserialization steps**:

1. Seek to `footer.cas_info_offset`
2. Read `CASChunkSequenceHeader`
3. Check if `cas_hash` is all 0xFF (bookend marker) - if so, stop
4. Read `cas_chunk_sequence_header.num_entries` × `CASChunkSequenceEntry` structures
5. Repeat from step 2 until bookend found

### CASChunkSequenceHeader

```rust
struct CASChunkSequenceHeader {
    cas_hash: Hash,           // 32-byte Xorb hash
    cas_flags: u32,           // CAS flags (reserved for later, set to 0)
    num_entries: u32,         // Number of chunks in this Xorb
    num_bytes_in_cas: u32,    // Total size of all raw chunk bytes in this Xorb
    num_bytes_on_disk: u32,   // Length of the xorb after serialized when uploaded
}
```

**Memory Layout**:

```txt
┌────────────────────────────────────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
│                       cas_hash (32 bytes)                      │cas_flags│num_     │num_bytes│num_bytes│
│                      CAS Block Hash                            │(4 bytes)│entries  │in_cas   │on_disk  │
│                                                                │         │(4 bytes)│(4 bytes)│(4 bytes)│
└────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┘
0                                                               32        36        40        44        48
```

### CASChunkSequenceEntry

Every `CASChunkSequenceHeader` will have a `num_entries` number field.
This number is the number of `CASChunkSequenceEntry` items that should be deserialized that are associated with the xorb described by this CAS Info block.

```rust
struct CASChunkSequenceEntry {
    chunk_hash: Hash,             // 32-byte chunk hash
    chunk_byte_range_start: u32,  // Start position in CAS block
    unpacked_segment_bytes: u32,  // Size when unpacked
    _unused: [u8; 8],             // Reserved space 8 bytes
}
```

**Memory Layout**:

```txt
┌────────────────────────────────────────────────────────────────┬─────────┬─────────┬─────────────────┐
│                     chunk_hash (32 bytes)                      │chunk_   │unpacked │    _unused      │
│                        Chunk Hash                              │byte_    │segment_ │   (8 bytes)     │
│                                                                │range_   │bytes    │                 │
│                                                                │start    │(4 bytes)│                 │
│                                                                │(4 bytes)│         │                 │
└────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────────────┘
0                                                               32        36        40               48
```

### CAS Info Bookend

The end of the cas info sections is marked by a bookend entry.

The bookend entry is 48 bytes long where the first 32 bytes are all `0xFF`, followed by 16 bytes of all `0x00`.

Suppose you were attempting to deserialize a `CASChunkSequenceHeader` and it's hash was all 1 bits then this entry is a bookend entry and the next bytes start the next section.

Since the cas info section immediately follows the file info section bookend, a client MAY skip deserializing the footer to know where the cas info section starts starts deserialize this section, it begins right after the file info section bookend and ends when the next bookend is reached.

## 4. Footer (MDBShardFileFooter)

> MUST NOT include the footer when serializing the shard as the body for the shard upload API.

**Location**: End of file minus footer_size
**Size**: 200 bytes

```rust
struct MDBShardFileFooter {
    version: u64,                    // Footer version (must be 1)
    file_info_offset: u64,           // Offset to file info section
    cas_info_offset: u64,            // Offset to CAS info section
    _buffer: [u8; 48],               // Reserved space (48 bytes)
    chunk_hash_hmac_key: Hash,       // HMAC key for chunk hashes (32 bytes)
    shard_creation_timestamp: u64,   // Creation time (seconds since epoch)
    shard_key_expiry: u64,           // Expiry time (seconds since epoch)
    _buffer2: [u8; 72],              // Reserved space (72 bytes)
    footer_offset: u64,              // Offset where footer starts
}
```

**Memory Layout**:

> Fields are not exactly to scale

```txt
┌─────────┬─────────┬─────────┬─────────────────────────────────────────────────────────────┬─────────────────────────────────────┐
│ version │file_info│cas_info │                    _buffer (reserved)                       │        chunk_hash_hmac_key          │
│(8 bytes)│offset   │offset   │                      (48 bytes)                             │             (32 bytes)              │
│         │(8 bytes)│(8 bytes)│                                                             │                                     │
└─────────┴─────────┴─────────┴─────────────────────────────────────────────────────────────┴─────────────────────────────────────┘
0         8        16        24                                                           72                                    104

┌─────────┬──────────┬─────────────────────────────────────────────────────────────────────────────┬─────────┐
│creation │shard_    │                           _buffer (reserved)                                │footer_  │
│timestamp│key_expiry│                             (72 bytes)                                      │offset   │
│(8 bytes)│ (8 bytes)│                                                                             │(8 bytes)│
└─────────┴──────────┴─────────────────────────────────────────────────────────────────────────────┴─────────┘
104       112       120                                                                          192       200
```

**Deserialization steps**:

1. Seek to `file_size - footer_size`
2. Read all fields sequentially as u64 values
3. Verify version equals 1

### Use of Footer Fields

#### file_info_offset and cas_info_offset

These offsets allow you to seek into the shard data buffer to reach these sections without deserializing linearly.

#### HMAC Key Protection

If `footer.chunk_hash_hmac_key` is non-zero (as a response shard from the global dedupe API), chunk hashes in the CAS Info section are protected with [HMAC](https://en.wikipedia.org/wiki/HMAC):

- The stored chunk hashes are `HMAC(original_hash, footer.chunk_hash_hmac_key)`
- To check if a chunk of data that you have matches a chunk listed in the shard, compute `HMAC(chunk_hash, footer.chunk_hash_hmac_key)` for your chunk hash and search through the shard results.
If you find a match (matched_chunk) then you know the original chunk hash of your chunk and the matched_chunk is the same and you can deduplicate your chunk by referencing the xorb that matched_chunk belongs to.

#### Shard Key Expiry

The shard key expiry is a 64 bit unix timestamp of when the shard received is to be considered expired (usually in the order of days or weeks after the shard was sent back).

After this expiry time has passed clients SHOULD consider this shard expired and SHOULD NOT use it to deduplicate data.
Uploads that reference xorbs that were referenced by this shard can be rejected at the server's discretion.

## Complete Deserialization Algorithm

```text
// ** option 1, read linearly, streaming **
// assume shard is a read-able file-like object and the reader position is at start of shard
// 1. Read and validate header
header = read_header(shard)

// 2. Read file info section  
file_info = read_file_info_section(shard) // read through file info bookend

// 3. Read CAS info section
cas_info = read_cas_info_section(shard) // read through cas info bookend

// 4. Read footer
footer = read_footer(shard)

// shard reader should now be at EOF


// ** option 2, read footer and seek **
// assume shard is a read-able seek-able file-like object
// 1. Read and validate header
seek(start of shard)
header = read_header(shard)

// 2. Read and validate footer (needed for offsets)
seek(end of shard minus header.footer_size)
footer = read_footer(shard)

// 3. Read file info section  
seek(footer.file_info_offset)
file_info = read_file_info_section(shard) // until footer.cas_info_offset

// 4. Read CAS info section
seek(footer.cas_info_offset)
cas_info = read_cas_info_section(shard) // until footer.footer_offset
```

## Version Compatibility

- Header version 2: Current format
- Footer version 1: Current format
- Shards with different versions will be rejected

## Error Handling

- Always verify magic numbers and versions
- Check that offsets are within file bounds  
- Verify that bookend markers are present where expected
