// Binary parsers for xorb and shard files

import type {
  ParsedFileMetadata,
  Chunk,
  ChunkHeader,
  ShardData,
  MerkleHash,
  HMACKey,
  MDBShardFileHeader,
  MDBShardFileFooter,
  FileDataSequenceHeader,
  FileDataSequenceEntry,
  FileVerificationEntry,
  FileMetadataExt,
  CASChunkSequenceHeader,
  CASChunkSequenceEntry,
  MDBFileInfo,
  MDBCASInfo,
} from "./types.js";
import { MDB_SHARD_HEADER_TAG, XORB_IDENT } from "./types.js";

export class BinaryReader {
  private data: Uint8Array;
  private offset: number = 0;

  constructor(data: Uint8Array) {
    this.data = data;
  }

  readUint8(): number {
    if (this.offset >= this.data.length) {
      console.trace();
      throw new Error("Unexpected end of data");
    }
    return this.data[this.offset++];
  }

  readUint32LE(): number {
    if (this.offset + 4 > this.data.length) {
      console.trace();
      throw new Error("Unexpected end of data");
    }
    const result = new DataView(this.data.buffer).getUint32(this.offset, true);
    this.offset += 4;
    return result;
  }

  readUint64LE(): bigint {
    if (this.offset + 8 > this.data.length) {
      console.trace();
      throw new Error("Unexpected end of data");
    }
    const result = new DataView(this.data.buffer).getBigUint64(
      this.offset,
      true
    );
    this.offset += 8;
    return result;
  }

  readBytes(length: number): Uint8Array {
    if (this.offset + length > this.data.length) {
      console.trace();
      throw new Error("Unexpected end of data");
    }
    const result = this.data.slice(this.offset, this.offset + length);
    this.offset += length;
    return result;
  }

  readHash(): MerkleHash {
    return { data: this.readBytes(32) };
  }

  readHMACKey(): HMACKey {
    return { data: this.readBytes(32) };
  }

  readString(length: number): string {
    const bytes = this.readBytes(length);
    return new TextDecoder().decode(bytes);
  }

  seek(position: number): void {
    this.offset = position;
  }

  seekFromEnd(offsetFromEnd: number): void {
    this.offset = this.data.length - offsetFromEnd;
  }

  get position(): number {
    return this.offset;
  }

  get remaining(): number {
    return this.data.length - this.offset;
  }
}

function arraysEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function isBookendHash(hash: MerkleHash): boolean {
  // Bookend hash is all 0xFF bytes
  return hash.data.every((byte) => byte === 0xff);
}

function formatHash(hash: MerkleHash): string {
  return Array.from(hash.data)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// File type detection removed - type is now specified by user selection

function parseXorbFile(data: Uint8Array): Chunk[] {
  const reader = new BinaryReader(data);

  const chunks: Chunk[] = [];

  while (reader.remaining > 0) {
    // Check if we have enough bytes for a header
    if (reader.remaining < 8) {
      console.error("Unexpected end of data parsing xorb file");
      break;
    }

    const header_bytes = reader.readBytes(8);
    let is_xorb_ident = true;
    // Urgh how do I compare two Uint8Arrays?
    for (let i = 0; i < 7; i++) {
      if (header_bytes[i] !== XORB_IDENT[i]) {
        is_xorb_ident = false;
        break;
      }
    }
    if (is_xorb_ident) {
      // reached optional xorb footer, skip rest
      break;
    }

    const header = new DataView(header_bytes.buffer);

    const version = header.getUint8(0);
    const compressed_size =
      header.getUint8(1) |
      (header.getUint8(2) << 8) |
      (header.getUint8(3) << 16);
    const compression_type = header.getUint8(4);
    const uncompressed_size =
      header.getUint8(5) |
      (header.getUint8(6) << 8) |
      (header.getUint8(7) << 16);

    const chunkHeader: ChunkHeader = {
      version,
      compressed_size,
      compression_type,
      uncompressed_size,
    };

    const compressed_data = reader.readBytes(compressed_size);

    chunks.push({ header: chunkHeader, compressed_data });
  }

  return chunks;
}

function parseShardFile(data: Uint8Array): ShardData {
  const reader = new BinaryReader(data);

  // Parse header
  const tag = reader.readBytes(32);
  if (!arraysEqual(tag, MDB_SHARD_HEADER_TAG)) {
    throw new Error("Invalid shard file header tag");
  }

  const header: MDBShardFileHeader = {
    tag,
    version: Number(reader.readUint64LE()),
    footer_size: Number(reader.readUint64LE()),
  };

  if (header.version !== 2) {
    throw new Error(`Unsupported shard header version: ${header.version}`);
  }

  // Parse footer (from end of file)
  reader.seekFromEnd(header.footer_size);
  const version = Number(reader.readUint64LE());
  const file_info_offset = Number(reader.readUint64LE());
  const cas_info_offset = Number(reader.readUint64LE());

  // Skip first buffer (48 bytes)
  reader.readBytes(48);

  const chunk_hash_hmac_key = reader.readHMACKey();
  const shard_creation_timestamp = Number(reader.readUint64LE());
  const shard_key_expiry = Number(reader.readUint64LE());

  // Skip second buffer (72 bytes)
  reader.readBytes(72);

  const footer_offset = Number(reader.readUint64LE());

  const footer: MDBShardFileFooter = {
    version,
    file_info_offset,
    cas_info_offset,
    chunk_hash_hmac_key,
    shard_creation_timestamp,
    shard_key_expiry,
    footer_offset,
  };

  if (footer.version !== 1) {
    throw new Error(`Unsupported shard footer version: ${footer.version}`);
  }

  // Parse file info section
  const file_info: MDBFileInfo[] = [];
  reader.seek(footer.file_info_offset);

  while (reader.position < footer.cas_info_offset) {
    const pos = reader.position;
    const file_hash = reader.readHash();

    // Check for bookend
    if (isBookendHash(file_hash)) {
      reader.readBytes(16); // unused
      break;
    }

    const file_flags = reader.readUint32LE();
    const num_entries = reader.readUint32LE();
    const _unused = reader.readBytes(8);

    const header: FileDataSequenceHeader = {
      file_hash,
      file_flags,
      num_entries,
      _unused,
    };

    // Read entries
    const entries: FileDataSequenceEntry[] = [];
    for (let i = 0; i < num_entries; i++) {
      const pos = reader.position;
      const cas_hash = reader.readHash();
      const chunk_range_start = reader.readUint32LE();
      const chunk_range_end = reader.readUint32LE();
      const byte_range_start = reader.readUint32LE();
      const byte_range_end = reader.readUint32LE();
      entries.push({
        cas_hash,
        chunk_range_start,
        chunk_range_end,
        byte_range_start,
        byte_range_end,
      });
    }

    // Read verification entries if present
    let verification_entries: FileVerificationEntry[] | undefined;
    if (file_flags & 0x80000000) {
      verification_entries = [];
      for (let i = 0; i < num_entries; i++) {
        verification_entries.push({
          chunk_hash: reader.readHash(),
          _unused: reader.readBytes(16),
        });
      }
    }

    // Read metadata extension if present
    let metadata_ext: FileMetadataExt | undefined;
    if (file_flags & 0x40000000) {
      metadata_ext = {
        sha256: reader.readHash(),
        _unused: reader.readBytes(16),
      };
    }

    file_info.push({
      header,
      entries,
      verification_entries,
      metadata_ext,
    });
  }

  // Parse CAS info section
  const cas_info: MDBCASInfo[] = [];
  reader.seek(footer.cas_info_offset);

  while (reader.position < footer.footer_offset) {
    const cas_hash = reader.readHash();

    // Check for bookend
    if (isBookendHash(cas_hash)) {
      break;
    }

    const cas_flags = reader.readUint32LE();
    const num_entries = reader.readUint32LE();
    const num_bytes_in_cas = reader.readUint32LE();
    const num_bytes_on_disk = reader.readUint32LE();

    const header: CASChunkSequenceHeader = {
      cas_hash,
      cas_flags,
      num_entries,
      num_bytes_in_cas,
      num_bytes_on_disk,
    };

    // Read entries
    const entries: CASChunkSequenceEntry[] = [];
    for (let i = 0; i < num_entries; i++) {
      entries.push({
        chunk_hash: reader.readHash(),
        chunk_byte_range_start: reader.readUint32LE(),
        unpacked_segment_bytes: reader.readUint32LE(),
        _unused: Number(reader.readUint64LE()),
      });
    }

    cas_info.push({
      header,
      entries,
    });
  }

  return {
    header,
    footer,
    file_info,
    cas_info,
  };
}

export async function parseFile(
  file: File,
  fileType: "xorb" | "shard"
): Promise<ParsedFileMetadata> {
  try {
    const arrayBuffer = await file.arrayBuffer();
    const data = new Uint8Array(arrayBuffer);

    let parsedData: Chunk[] | ShardData;

    if (fileType === "xorb") {
      parsedData = parseXorbFile(data);
    } else {
      parsedData = parseShardFile(data);
    }

    return {
      type: fileType,
      filename: file.name,
      fileSize: file.size,
      data: parsedData,
    };
  } catch (error) {
    return {
      type: fileType,
      filename: file.name,
      fileSize: file.size,
      data: [] as any,
      error: error instanceof Error ? error.message : "Unknown error occurred",
    };
  }
}

// Helper functions for displaying data
export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

export function formatTimestamp(timestamp: number): string {
  return new Date(timestamp * 1000).toISOString();
}

export function formatHashShort(hash: MerkleHash): string {
  const fullHash = formatHash(hash);
  return fullHash.substring(0, 16) + "...";
}

export { formatHash };
