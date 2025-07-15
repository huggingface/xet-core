/**
 * MDB Shard File Format Implementation (TypeScript)
 *
 * This module provides TypeScript classes and functions to read and write MDB shard files
 * according to the specification in shard.md.
 */

// Constants from the specification
const MDB_SHARD_HEADER_VERSION = 2;
const MDB_SHARD_FOOTER_VERSION = 1;
const MDB_FILE_INFO_ENTRY_SIZE = 48;
const MDB_CAS_INFO_ENTRY_SIZE = 48;

// Magic tag for shard header validation
const MDB_SHARD_HEADER_TAG = new Uint8Array([
  0x48, 0x46, 0x52, 0x65, 0x70, 0x6f, 0x4d, 0x65, 0x74, 0x61, 0x44, 0x61, 0x74,
  0x61, 0x00, 0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57, 0x83, 0xa5, 0xbd,
  0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
]);

// File flags
const MDB_FILE_FLAG_WITH_VERIFICATION = 0x80000000;
const MDB_FILE_FLAG_WITH_METADATA_EXT = 0x40000000;

// Bookend marker (all 0xFF for first 32 bytes)
const BOOKEND_HASH = new Uint8Array(32).fill(0xff);

/**
 * 32-byte hash value
 */
class MerkleHash {
  public data: Uint8Array;

  constructor(data?: Uint8Array) {
    if (data) {
      if (data.length !== 32) {
        throw new Error(`MerkleHash must be 32 bytes, got ${data.length}`);
      }
      this.data = new Uint8Array(data);
    } else {
      this.data = new Uint8Array(32);
    }
  }

  static fromU64Array(values: bigint[]): MerkleHash {
    if (values.length !== 4) {
      throw new Error("MerkleHash requires exactly 4 u64 values");
    }
    const data = new Uint8Array(32);
    const view = new DataView(data.buffer);
    for (let i = 0; i < 4; i++) {
      view.setBigUint64(i * 8, values[i], true); // little-endian
    }
    return new MerkleHash(data);
  }

  toU64Array(): bigint[] {
    const view = new DataView(this.data.buffer, this.data.byteOffset);
    const result: bigint[] = [];
    for (let i = 0; i < 4; i++) {
      result.push(view.getBigUint64(i * 8, true)); // little-endian
    }
    return result;
  }

  isBookend(): boolean {
    return this.data.every((byte, index) => byte === BOOKEND_HASH[index]);
  }

  static bookend(): MerkleHash {
    return new MerkleHash(BOOKEND_HASH);
  }

  equals(other: MerkleHash): boolean {
    return this.data.every((byte, index) => byte === other.data[index]);
  }
}

/**
 * 32-byte HMAC key
 */
class HMACKey {
  public data: Uint8Array;

  constructor(data?: Uint8Array) {
    if (data) {
      if (data.length !== 32) {
        throw new Error(`HMACKey must be 32 bytes, got ${data.length}`);
      }
      this.data = new Uint8Array(data);
    } else {
      this.data = new Uint8Array(32);
    }
  }

  isZero(): boolean {
    return this.data.every((byte) => byte === 0);
  }
}

/**
 * Shard file header structure
 */
class MDBShardFileHeader {
  public tag: Uint8Array;
  public version: number;
  public footerSize: number;

  constructor(tag?: Uint8Array, version?: number, footerSize?: number) {
    this.tag = tag || new Uint8Array(MDB_SHARD_HEADER_TAG);
    this.version = version || MDB_SHARD_HEADER_VERSION;
    this.footerSize = footerSize || 192; // Size of MDBShardFileFooter
  }

  serialize(): Uint8Array {
    const data = new Uint8Array(48);
    data.set(this.tag, 0);
    const view = new DataView(data.buffer);
    view.setBigUint64(32, BigInt(this.version), true);
    view.setBigUint64(40, BigInt(this.footerSize), true);
    return data;
  }

  static deserialize(data: Uint8Array): MDBShardFileHeader {
    if (data.length < 48) {
      throw new Error("Insufficient data for header");
    }

    const tag = data.slice(0, 32);
    if (!tag.every((byte, index) => byte === MDB_SHARD_HEADER_TAG[index])) {
      throw new Error("Invalid magic tag");
    }

    const view = new DataView(data.buffer, data.byteOffset);
    const version = Number(view.getBigUint64(32, true));
    const footerSize = Number(view.getBigUint64(40, true));

    if (version !== MDB_SHARD_HEADER_VERSION) {
      throw new Error(`Unsupported header version: ${version}`);
    }

    return new MDBShardFileHeader(tag, version, footerSize);
  }
}

/**
 * Shard file footer structure
 */
class MDBShardFileFooter {
  public version: number = MDB_SHARD_FOOTER_VERSION;
  public fileInfoOffset: number = 0;
  public casInfoOffset: number = 0;
  public fileLookupOffset: number = 0;
  public fileLookupNumEntry: number = 0;
  public casLookupOffset: number = 0;
  public casLookupNumEntry: number = 0;
  public chunkLookupOffset: number = 0;
  public chunkLookupNumEntry: number = 0;
  public chunkHashHmacKey: HMACKey = new HMACKey();
  public shardCreationTimestamp: number = 0;
  public shardKeyExpiry: number = 0;
  public buffer: number[] = [0, 0, 0, 0, 0, 0]; // 6 u64 values
  public storedBytesOnDisk: number = 0;
  public materializedBytes: number = 0;
  public storedBytes: number = 0;
  public footerOffset: number = 0;

  serialize(): Uint8Array {
    const data = new Uint8Array(192);
    const view = new DataView(data.buffer);
    let offset = 0;

    view.setBigUint64(offset, BigInt(this.version), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.fileInfoOffset), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.casInfoOffset), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.fileLookupOffset), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.fileLookupNumEntry), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.casLookupOffset), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.casLookupNumEntry), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.chunkLookupOffset), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.chunkLookupNumEntry), true);
    offset += 8;

    data.set(this.chunkHashHmacKey.data, offset);
    offset += 32;

    view.setBigUint64(offset, BigInt(this.shardCreationTimestamp), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.shardKeyExpiry), true);
    offset += 8;

    for (let i = 0; i < 6; i++) {
      view.setBigUint64(offset, BigInt(this.buffer[i]), true);
      offset += 8;
    }

    view.setBigUint64(offset, BigInt(this.storedBytesOnDisk), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.materializedBytes), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.storedBytes), true);
    offset += 8;
    view.setBigUint64(offset, BigInt(this.footerOffset), true);

    return data;
  }

  static deserialize(data: Uint8Array): MDBShardFileFooter {
    if (data.length < 192) {
      throw new Error("Insufficient data for footer");
    }

    const view = new DataView(data.buffer, data.byteOffset);
    const footer = new MDBShardFileFooter();
    let offset = 0;

    footer.version = Number(view.getBigUint64(offset, true));
    offset += 8;
    if (footer.version !== MDB_SHARD_FOOTER_VERSION) {
      throw new Error(`Unsupported footer version: ${footer.version}`);
    }

    footer.fileInfoOffset = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.casInfoOffset = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.fileLookupOffset = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.fileLookupNumEntry = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.casLookupOffset = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.casLookupNumEntry = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.chunkLookupOffset = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.chunkLookupNumEntry = Number(view.getBigUint64(offset, true));
    offset += 8;

    footer.chunkHashHmacKey = new HMACKey(data.slice(offset, offset + 32));
    offset += 32;

    footer.shardCreationTimestamp = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.shardKeyExpiry = Number(view.getBigUint64(offset, true));
    offset += 8;

    for (let i = 0; i < 6; i++) {
      footer.buffer[i] = Number(view.getBigUint64(offset, true));
      offset += 8;
    }

    footer.storedBytesOnDisk = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.materializedBytes = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.storedBytes = Number(view.getBigUint64(offset, true));
    offset += 8;
    footer.footerOffset = Number(view.getBigUint64(offset, true));

    return footer;
  }
}

/**
 * File data sequence header
 */
class FileDataSequenceHeader {
  public fileHash: MerkleHash = new MerkleHash();
  public fileFlags: number = 0;
  public numEntries: number = 0;
  public unused: number = 0;

  containsVerification(): boolean {
    return (this.fileFlags & MDB_FILE_FLAG_WITH_VERIFICATION) !== 0;
  }

  containsMetadataExt(): boolean {
    return (this.fileFlags & MDB_FILE_FLAG_WITH_METADATA_EXT) !== 0;
  }

  isBookend(): boolean {
    return this.fileHash.isBookend();
  }

  static bookend(): FileDataSequenceHeader {
    const header = new FileDataSequenceHeader();
    header.fileHash = MerkleHash.bookend();
    return header;
  }

  serialize(): Uint8Array {
    const data = new Uint8Array(48);
    data.set(this.fileHash.data, 0);
    const view = new DataView(data.buffer);
    view.setUint32(32, this.fileFlags, true);
    view.setUint32(36, this.numEntries, true);
    view.setBigUint64(40, BigInt(this.unused), true);
    return data;
  }

  static deserialize(data: Uint8Array): FileDataSequenceHeader {
    if (data.length < 48) {
      throw new Error("Insufficient data for FileDataSequenceHeader");
    }

    const header = new FileDataSequenceHeader();
    header.fileHash = new MerkleHash(data.slice(0, 32));
    const view = new DataView(data.buffer, data.byteOffset);
    header.fileFlags = view.getUint32(32, true);
    header.numEntries = view.getUint32(36, true);
    header.unused = Number(view.getBigUint64(40, true));

    return header;
  }
}

/**
 * File data sequence entry
 */
class FileDataSequenceEntry {
  public casHash: MerkleHash = new MerkleHash();
  public casFlags: number = 0;
  public unpackedSegmentBytes: number = 0;
  public chunkIndexStart: number = 0;
  public chunkIndexEnd: number = 0;

  serialize(): Uint8Array {
    const data = new Uint8Array(48);
    data.set(this.casHash.data, 0);
    const view = new DataView(data.buffer);
    view.setUint32(32, this.casFlags, true);
    view.setUint32(36, this.unpackedSegmentBytes, true);
    view.setUint32(40, this.chunkIndexStart, true);
    view.setUint32(44, this.chunkIndexEnd, true);
    return data;
  }

  static deserialize(data: Uint8Array): FileDataSequenceEntry {
    if (data.length < 48) {
      throw new Error("Insufficient data for FileDataSequenceEntry");
    }

    const entry = new FileDataSequenceEntry();
    entry.casHash = new MerkleHash(data.slice(0, 32));
    const view = new DataView(data.buffer, data.byteOffset);
    entry.casFlags = view.getUint32(32, true);
    entry.unpackedSegmentBytes = view.getUint32(36, true);
    entry.chunkIndexStart = view.getUint32(40, true);
    entry.chunkIndexEnd = view.getUint32(44, true);

    return entry;
  }
}

/**
 * File verification entry
 */
class FileVerificationEntry {
  public rangeHash: MerkleHash = new MerkleHash();
  public unused: number[] = [0, 0];

  serialize(): Uint8Array {
    const data = new Uint8Array(48);
    data.set(this.rangeHash.data, 0);
    const view = new DataView(data.buffer);
    view.setBigUint64(32, BigInt(this.unused[0]), true);
    view.setBigUint64(40, BigInt(this.unused[1]), true);
    return data;
  }

  static deserialize(data: Uint8Array): FileVerificationEntry {
    if (data.length < 48) {
      throw new Error("Insufficient data for FileVerificationEntry");
    }

    const entry = new FileVerificationEntry();
    entry.rangeHash = new MerkleHash(data.slice(0, 32));
    const view = new DataView(data.buffer, data.byteOffset);
    entry.unused[0] = Number(view.getBigUint64(32, true));
    entry.unused[1] = Number(view.getBigUint64(40, true));

    return entry;
  }
}

/**
 * Extended file metadata
 */
class FileMetadataExt {
  public sha256: MerkleHash = new MerkleHash();
  public unused: number[] = [0, 0];

  serialize(): Uint8Array {
    const data = new Uint8Array(48);
    data.set(this.sha256.data, 0);
    const view = new DataView(data.buffer);
    view.setBigUint64(32, BigInt(this.unused[0]), true);
    view.setBigUint64(40, BigInt(this.unused[1]), true);
    return data;
  }

  static deserialize(data: Uint8Array): FileMetadataExt {
    if (data.length < 48) {
      throw new Error("Insufficient data for FileMetadataExt");
    }

    const entry = new FileMetadataExt();
    entry.sha256 = new MerkleHash(data.slice(0, 32));
    const view = new DataView(data.buffer, data.byteOffset);
    entry.unused[0] = Number(view.getBigUint64(32, true));
    entry.unused[1] = Number(view.getBigUint64(40, true));

    return entry;
  }
}

/**
 * Complete file information
 */
class MDBFileInfo {
  public metadata: FileDataSequenceHeader = new FileDataSequenceHeader();
  public segments: FileDataSequenceEntry[] = [];
  public verification: FileVerificationEntry[] = [];
  public metadataExt?: FileMetadataExt;
}

/**
 * CAS chunk sequence header
 */
class CASChunkSequenceHeader {
  public casHash: MerkleHash = new MerkleHash();
  public casFlags: number = 0;
  public numEntries: number = 0;
  public numBytesInCas: number = 0;
  public numBytesOnDisk: number = 0;

  isBookend(): boolean {
    return this.casHash.isBookend();
  }

  static bookend(): CASChunkSequenceHeader {
    const header = new CASChunkSequenceHeader();
    header.casHash = MerkleHash.bookend();
    return header;
  }

  serialize(): Uint8Array {
    const data = new Uint8Array(48);
    data.set(this.casHash.data, 0);
    const view = new DataView(data.buffer);
    view.setUint32(32, this.casFlags, true);
    view.setUint32(36, this.numEntries, true);
    view.setUint32(40, this.numBytesInCas, true);
    view.setUint32(44, this.numBytesOnDisk, true);
    return data;
  }

  static deserialize(data: Uint8Array): CASChunkSequenceHeader {
    if (data.length < 48) {
      throw new Error("Insufficient data for CASChunkSequenceHeader");
    }

    const header = new CASChunkSequenceHeader();
    header.casHash = new MerkleHash(data.slice(0, 32));
    const view = new DataView(data.buffer, data.byteOffset);
    header.casFlags = view.getUint32(32, true);
    header.numEntries = view.getUint32(36, true);
    header.numBytesInCas = view.getUint32(40, true);
    header.numBytesOnDisk = view.getUint32(44, true);

    return header;
  }
}

/**
 * CAS chunk sequence entry
 */
class CASChunkSequenceEntry {
  public chunkHash: MerkleHash = new MerkleHash();
  public unpackedSegmentBytes: number = 0;
  public chunkByteRangeStart: number = 0;
  public unused: number = 0;

  serialize(): Uint8Array {
    const data = new Uint8Array(48);
    data.set(this.chunkHash.data, 0);
    const view = new DataView(data.buffer);
    view.setUint32(32, this.chunkByteRangeStart, true);
    view.setUint32(36, this.unpackedSegmentBytes, true);
    view.setBigUint64(40, BigInt(this.unused), true);
    return data;
  }

  static deserialize(data: Uint8Array): CASChunkSequenceEntry {
    if (data.length < 48) {
      throw new Error("Insufficient data for CASChunkSequenceEntry");
    }

    const entry = new CASChunkSequenceEntry();
    entry.chunkHash = new MerkleHash(data.slice(0, 32));
    const view = new DataView(data.buffer, data.byteOffset);
    entry.chunkByteRangeStart = view.getUint32(32, true);
    entry.unpackedSegmentBytes = view.getUint32(36, true);
    entry.unused = Number(view.getBigUint64(40, true));

    return entry;
  }
}

/**
 * Complete CAS information
 */
class MDBCASInfo {
  public metadata: CASChunkSequenceHeader = new CASChunkSequenceHeader();
  public chunks: CASChunkSequenceEntry[] = [];
}

/**
 * File lookup table entry
 */
class FileLookupEntry {
  public truncatedHash: bigint = 0n;
  public fileIndex: number = 0;

  serialize(): Uint8Array {
    const data = new Uint8Array(12);
    const view = new DataView(data.buffer);
    view.setBigUint64(0, this.truncatedHash, true);
    view.setUint32(8, this.fileIndex, true);
    return data;
  }

  static deserialize(data: Uint8Array): FileLookupEntry {
    if (data.length < 12) {
      throw new Error("Insufficient data for FileLookupEntry");
    }

    const entry = new FileLookupEntry();
    const view = new DataView(data.buffer, data.byteOffset);
    entry.truncatedHash = view.getBigUint64(0, true);
    entry.fileIndex = view.getUint32(8, true);

    return entry;
  }
}

/**
 * CAS lookup table entry
 */
class CASLookupEntry {
  public truncatedHash: bigint = 0n;
  public casIndex: number = 0;

  serialize(): Uint8Array {
    const data = new Uint8Array(12);
    const view = new DataView(data.buffer);
    view.setBigUint64(0, this.truncatedHash, true);
    view.setUint32(8, this.casIndex, true);
    return data;
  }

  static deserialize(data: Uint8Array): CASLookupEntry {
    if (data.length < 12) {
      throw new Error("Insufficient data for CASLookupEntry");
    }

    const entry = new CASLookupEntry();
    const view = new DataView(data.buffer, data.byteOffset);
    entry.truncatedHash = view.getBigUint64(0, true);
    entry.casIndex = view.getUint32(8, true);

    return entry;
  }
}

/**
 * Chunk lookup table entry
 */
class ChunkLookupEntry {
  public truncatedHash: bigint = 0n;
  public casIndex: number = 0;
  public chunkIndex: number = 0;

  serialize(): Uint8Array {
    const data = new Uint8Array(16);
    const view = new DataView(data.buffer);
    view.setBigUint64(0, this.truncatedHash, true);
    view.setUint32(8, this.casIndex, true);
    view.setUint32(12, this.chunkIndex, true);
    return data;
  }

  static deserialize(data: Uint8Array): ChunkLookupEntry {
    if (data.length < 16) {
      throw new Error("Insufficient data for ChunkLookupEntry");
    }

    const entry = new ChunkLookupEntry();
    const view = new DataView(data.buffer, data.byteOffset);
    entry.truncatedHash = view.getBigUint64(0, true);
    entry.casIndex = view.getUint32(8, true);
    entry.chunkIndex = view.getUint32(12, true);

    return entry;
  }
}

/**
 * Complete shard data structure
 */
class ShardData {
  public header: MDBShardFileHeader = new MDBShardFileHeader();
  public footer: MDBShardFileFooter = new MDBShardFileFooter();
  public fileInfo: MDBFileInfo[] = [];
  public casInfo: MDBCASInfo[] = [];
  public fileLookup: FileLookupEntry[] = [];
  public casLookup: CASLookupEntry[] = [];
  public chunkLookup: ChunkLookupEntry[] = [];
}

/**
 * Reader helper class for sequential binary data reading
 */
class BinaryReader {
  private data: Uint8Array;
  private position: number = 0;

  constructor(data: Uint8Array) {
    this.data = data;
  }

  seek(position: number): void {
    this.position = position;
  }

  readExact(size: number): Uint8Array {
    if (this.position + size > this.data.length) {
      throw new Error(
        `Expected ${size} bytes, got ${this.data.length - this.position}`
      );
    }
    const result = this.data.slice(this.position, this.position + size);
    this.position += size;
    return result;
  }

  getPosition(): number {
    return this.position;
  }

  getLength(): number {
    return this.data.length;
  }
}

/**
 * Writer helper class for sequential binary data writing
 */
class BinaryWriter {
  private chunks: Uint8Array[] = [];
  private totalLength: number = 0;

  write(data: Uint8Array): void {
    this.chunks.push(data);
    this.totalLength += data.length;
  }

  getPosition(): number {
    return this.totalLength;
  }

  toArrayBuffer(): ArrayBuffer {
    const result = new Uint8Array(this.totalLength);
    let offset = 0;
    for (const chunk of this.chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    return result.buffer;
  }
}

/**
 * Truncate hash to first 8 bytes for lookup tables
 */
function truncateHash(hash: MerkleHash): bigint {
  const view = new DataView(hash.data.buffer, hash.data.byteOffset);
  return view.getBigUint64(0, true);
}

/**
 * Deserialize a complete shard from Uint8Array
 */
function deserializeShard(data: Uint8Array): ShardData {
  const reader = new BinaryReader(data);

  // Read header
  reader.seek(0);
  const headerData = reader.readExact(48);
  const header = MDBShardFileHeader.deserialize(headerData);

  // Read footer
  reader.seek(reader.getLength() - header.footerSize);
  const footerData = reader.readExact(header.footerSize);
  const footer = MDBShardFileFooter.deserialize(footerData);

  // Read file info section
  const fileInfo: MDBFileInfo[] = [];
  reader.seek(footer.fileInfoOffset);

  while (true) {
    const headerData = reader.readExact(48);
    const fileHeader = FileDataSequenceHeader.deserialize(headerData);

    if (fileHeader.isBookend()) {
      break;
    }

    // Read file data sequence entries
    const segments: FileDataSequenceEntry[] = [];
    for (let i = 0; i < fileHeader.numEntries; i++) {
      const entryData = reader.readExact(48);
      segments.push(FileDataSequenceEntry.deserialize(entryData));
    }

    // Read verification entries if present
    const verification: FileVerificationEntry[] = [];
    if (fileHeader.containsVerification()) {
      for (let i = 0; i < fileHeader.numEntries; i++) {
        const verifyData = reader.readExact(48);
        verification.push(FileVerificationEntry.deserialize(verifyData));
      }
    }

    // Read metadata extension if present
    let metadataExt: FileMetadataExt | undefined;
    if (fileHeader.containsMetadataExt()) {
      const extData = reader.readExact(48);
      metadataExt = FileMetadataExt.deserialize(extData);
    }

    const file = new MDBFileInfo();
    file.metadata = fileHeader;
    file.segments = segments;
    file.verification = verification;
    file.metadataExt = metadataExt;
    fileInfo.push(file);
  }

  // Read CAS info section
  const casInfo: MDBCASInfo[] = [];
  reader.seek(footer.casInfoOffset);

  while (true) {
    const headerData = reader.readExact(48);
    const casHeader = CASChunkSequenceHeader.deserialize(headerData);

    if (casHeader.isBookend()) {
      break;
    }

    // Read CAS chunk entries
    const chunks: CASChunkSequenceEntry[] = [];
    for (let i = 0; i < casHeader.numEntries; i++) {
      const chunkData = reader.readExact(48);
      chunks.push(CASChunkSequenceEntry.deserialize(chunkData));
    }

    const cas = new MDBCASInfo();
    cas.metadata = casHeader;
    cas.chunks = chunks;
    casInfo.push(cas);
  }

  // Read lookup tables
  const fileLookup: FileLookupEntry[] = [];
  if (footer.fileLookupNumEntry > 0) {
    reader.seek(footer.fileLookupOffset);
    for (let i = 0; i < footer.fileLookupNumEntry; i++) {
      const entryData = reader.readExact(12);
      fileLookup.push(FileLookupEntry.deserialize(entryData));
    }
  }

  const casLookup: CASLookupEntry[] = [];
  if (footer.casLookupNumEntry > 0) {
    reader.seek(footer.casLookupOffset);
    for (let i = 0; i < footer.casLookupNumEntry; i++) {
      const entryData = reader.readExact(12);
      casLookup.push(CASLookupEntry.deserialize(entryData));
    }
  }

  const chunkLookup: ChunkLookupEntry[] = [];
  if (footer.chunkLookupNumEntry > 0) {
    reader.seek(footer.chunkLookupOffset);
    for (let i = 0; i < footer.chunkLookupNumEntry; i++) {
      const entryData = reader.readExact(16);
      chunkLookup.push(ChunkLookupEntry.deserialize(entryData));
    }
  }

  const shard = new ShardData();
  shard.header = header;
  shard.footer = footer;
  shard.fileInfo = fileInfo;
  shard.casInfo = casInfo;
  shard.fileLookup = fileLookup;
  shard.casLookup = casLookup;
  shard.chunkLookup = chunkLookup;

  return shard;
}

/**
 * Serialize a shard to ArrayBuffer
 */
function serializeShard(shard: ShardData): ArrayBuffer {
  const writer = new BinaryWriter();

  // Write header
  const headerData = shard.header.serialize();
  writer.write(headerData);

  // Update footer with file info offset
  shard.footer.fileInfoOffset = writer.getPosition();

  // Write file info section
  for (const fileInfo of shard.fileInfo) {
    writer.write(fileInfo.metadata.serialize());

    for (const segment of fileInfo.segments) {
      writer.write(segment.serialize());
    }

    if (fileInfo.metadata.containsVerification()) {
      for (const verify of fileInfo.verification) {
        writer.write(verify.serialize());
      }
    }

    if (fileInfo.metadata.containsMetadataExt() && fileInfo.metadataExt) {
      writer.write(fileInfo.metadataExt.serialize());
    }
  }

  // Write file info bookend
  writer.write(FileDataSequenceHeader.bookend().serialize());

  // Update footer with CAS info offset
  shard.footer.casInfoOffset = writer.getPosition();

  // Write CAS info section
  for (const casInfo of shard.casInfo) {
    writer.write(casInfo.metadata.serialize());

    for (const chunk of casInfo.chunks) {
      writer.write(chunk.serialize());
    }
  }

  // Write CAS info bookend
  writer.write(CASChunkSequenceHeader.bookend().serialize());

  // Update footer with lookup table offsets
  shard.footer.fileLookupOffset = writer.getPosition();
  shard.footer.fileLookupNumEntry = shard.fileLookup.length;

  // Write file lookup table
  for (const entry of shard.fileLookup) {
    writer.write(entry.serialize());
  }

  shard.footer.casLookupOffset = writer.getPosition();
  shard.footer.casLookupNumEntry = shard.casLookup.length;

  // Write CAS lookup table
  for (const entry of shard.casLookup) {
    writer.write(entry.serialize());
  }

  shard.footer.chunkLookupOffset = writer.getPosition();
  shard.footer.chunkLookupNumEntry = shard.chunkLookup.length;

  // Write chunk lookup table
  for (const entry of shard.chunkLookup) {
    writer.write(entry.serialize());
  }

  // Update footer offset and write footer
  shard.footer.footerOffset = writer.getPosition();
  const footerData = shard.footer.serialize();
  writer.write(footerData);

  return writer.toArrayBuffer();
}

/**
 * Find file info by hash using lookup table if available
 */
function findFileByHash(
  shard: ShardData,
  fileHash: MerkleHash
): MDBFileInfo | undefined {
  if (shard.fileLookup.length > 0) {
    // Use lookup table for fast search
    const truncated = truncateHash(fileHash);
    for (const entry of shard.fileLookup) {
      if (entry.truncatedHash === truncated) {
        // Verify full hash
        if (entry.fileIndex < shard.fileInfo.length) {
          const fileInfo = shard.fileInfo[entry.fileIndex];
          if (fileInfo.metadata.fileHash.equals(fileHash)) {
            return fileInfo;
          }
        }
      }
    }
  } else {
    // Linear search through all files
    for (const fileInfo of shard.fileInfo) {
      if (fileInfo.metadata.fileHash.equals(fileHash)) {
        return fileInfo;
      }
    }
  }

  return undefined;
}

/**
 * Find CAS info by hash using lookup table if available
 */
function findCasByHash(
  shard: ShardData,
  casHash: MerkleHash
): MDBCASInfo | undefined {
  if (shard.casLookup.length > 0) {
    // Use lookup table for fast search
    const truncated = truncateHash(casHash);
    for (const entry of shard.casLookup) {
      if (entry.truncatedHash === truncated) {
        // Verify full hash
        if (entry.casIndex < shard.casInfo.length) {
          const casInfo = shard.casInfo[entry.casIndex];
          if (casInfo.metadata.casHash.equals(casHash)) {
            return casInfo;
          }
        }
      }
    }
  } else {
    // Linear search through all CAS blocks
    for (const casInfo of shard.casInfo) {
      if (casInfo.metadata.casHash.equals(casHash)) {
        return casInfo;
      }
    }
  }

  return undefined;
}

// Export all classes and functions
export {
  MerkleHash,
  HMACKey,
  MDBShardFileHeader,
  MDBShardFileFooter,
  FileDataSequenceHeader,
  FileDataSequenceEntry,
  FileVerificationEntry,
  FileMetadataExt,
  MDBFileInfo,
  CASChunkSequenceHeader,
  CASChunkSequenceEntry,
  MDBCASInfo,
  FileLookupEntry,
  CASLookupEntry,
  ChunkLookupEntry,
  ShardData,
  deserializeShard,
  serializeShard,
  findFileByHash,
  findCasByHash,
  truncateHash,
};
