// Type definitions for xorb and shard file formats

export interface MerkleHash {
  data: [bigint, bigint, bigint, bigint]; // 4 x 64-bit unsigned integers (32 bytes total)
}

// === XORB Types ===

export interface ChunkHeader {
  version: number;
  compressed_size: number;
  compression_type: number;
  uncompressed_size: number;
}

export interface Chunk {
  header: ChunkHeader;
  compressed_data: Uint8Array;
}

// === SHARD Types ===

export interface MDBShardFileHeader {
  tag: Uint8Array; // 32 bytes magic number
  version: number;
  footer_size: number;
}

export interface MDBShardFileFooter {
  version: number;
  file_info_offset: number;
  cas_info_offset: number;
  chunk_hash_hmac_key: MerkleHash;
  shard_creation_timestamp: number;
  shard_key_expiry: number;
  footer_offset: number;
}

export interface FileDataSequenceHeader {
  file_hash: MerkleHash;
  file_flags: number;
  num_entries: number;
  _unused: Uint8Array;
}

export interface FileDataSequenceEntry {
  cas_hash: MerkleHash;
  cas_flags: number;
  unpacked_segment_bytes: number;
  chunk_index_start: number;
  chunk_index_end: number;
}

export interface FileVerificationEntry {
  chunk_hash: MerkleHash;
  _unused: Uint8Array;
}

export interface FileMetadataExt {
  sha256: MerkleHash;
  _unused: Uint8Array;
}

export interface CASChunkSequenceHeader {
  cas_hash: MerkleHash;
  cas_flags: number;
  num_entries: number;
  num_bytes_in_cas: number;
  num_bytes_on_disk: number;
}

export interface CASChunkSequenceEntry {
  chunk_hash: MerkleHash;
  chunk_byte_range_start: number;
  unpacked_segment_bytes: number;
  _unused: number;
}

export interface MDBFileInfo {
  header: FileDataSequenceHeader;
  entries: FileDataSequenceEntry[];
  verification_entries?: FileVerificationEntry[];
  metadata_ext?: FileMetadataExt;
}

export interface MDBCASInfo {
  header: CASChunkSequenceHeader;
  entries: CASChunkSequenceEntry[];
}

export interface ShardData {
  header: MDBShardFileHeader;
  footer: MDBShardFileFooter;
  file_info: MDBFileInfo[];
  cas_info: MDBCASInfo[];
}

// === Parsed Metadata for Display ===

export interface ParsedFileMetadata {
  type: "xorb" | "shard";
  filename: string;
  fileSize: number;
  data: Chunk[] | ShardData;
  error?: string;
}

// File type detection
export const MDB_SHARD_HEADER_TAG = new Uint8Array([
  0x48, 0x46, 0x52, 0x65, 0x70, 0x6f, 0x4d, 0x65, 0x74, 0x61, 0x44, 0x61, 0x74,
  0x61, 0x00, 0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57, 0x83, 0xa5, 0xbd,
  0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
]);

export const XORB_IDENT = new Uint8Array([88, 69, 84, 66, 76, 79, 66]);
