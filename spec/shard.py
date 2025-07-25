#!/usr/bin/env python3
"""
MDB Shard File Format Implementation

This module provides Python classes and functions to read and write MDB shard files
according to the specification in shard.md.
"""

import struct
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, BinaryIO
from pathlib import Path

# Constants from the specification
MDB_SHARD_HEADER_VERSION = 2
MDB_SHARD_FOOTER_VERSION = 1
MDB_FILE_INFO_ENTRY_SIZE = 48
MDB_CAS_INFO_ENTRY_SIZE = 48

# Magic tag for shard header validation
MDB_SHARD_HEADER_TAG = bytes([
    0x48, 0x46, 0x52, 0x65, 0x70, 0x6f, 0x4d, 0x65, 0x74, 0x61, 0x44, 0x61, 0x74, 0x61, 0x00, 0x55,
    0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57, 0x83, 0xa5, 0xbd, 0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9
])

# File flags
MDB_FILE_FLAG_WITH_VERIFICATION = 0x80000000
MDB_FILE_FLAG_WITH_METADATA_EXT = 0x40000000

# Bookend marker (all 0xFF for first 32 bytes)
BOOKEND_HASH = b'\xff' * 32


@dataclass
class MerkleHash:
    """32-byte hash value"""
    data: bytes = field(default_factory=lambda: b'\x00' * 32)
    
    def __post_init__(self):
        if len(self.data) != 32:
            raise ValueError(f"MerkleHash must be 32 bytes, got {len(self.data)}")
    
    @classmethod
    def from_u64_array(cls, values: List[int]) -> 'MerkleHash':
        """Create from array of 4 u64 values"""
        if len(values) != 4:
            raise ValueError("MerkleHash requires exactly 4 u64 values")
        data = struct.pack('<4Q', *values)
        return cls(data)
    
    def to_u64_array(self) -> List[int]:
        """Convert to array of 4 u64 values"""
        return list(struct.unpack('<4Q', self.data))
    
    def is_bookend(self) -> bool:
        """Check if this hash is a bookend marker (all 0xFF)"""
        return self.data == BOOKEND_HASH
    
    @classmethod
    def bookend(cls) -> 'MerkleHash':
        """Create a bookend hash (all 0xFF)"""
        return cls(BOOKEND_HASH)


@dataclass
class HMACKey:
    """32-byte HMAC key"""
    data: bytes = field(default_factory=lambda: b'\x00' * 32)
    
    def __post_init__(self):
        if len(self.data) != 32:
            raise ValueError(f"HMACKey must be 32 bytes, got {len(self.data)}")
    
    def is_zero(self) -> bool:
        """Check if this key is all zeros (no HMAC protection)"""
        return self.data == b'\x00' * 32


@dataclass
class MDBShardFileHeader:
    """Shard file header structure"""
    tag: bytes = field(default_factory=lambda: MDB_SHARD_HEADER_TAG)
    version: int = MDB_SHARD_HEADER_VERSION
    footer_size: int = 192  # Size of MDBShardFileFooter
    
    def serialize(self) -> bytes:
        """Serialize to bytes"""
        return self.tag + struct.pack('<2Q', self.version, self.footer_size)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'MDBShardFileHeader':
        """Deserialize from bytes"""
        if len(data) < 48:
            raise ValueError("Insufficient data for header")
        
        tag = data[:32]
        if tag != MDB_SHARD_HEADER_TAG:
            raise ValueError("Invalid magic tag")
        
        version, footer_size = struct.unpack('<2Q', data[32:48])
        if version != MDB_SHARD_HEADER_VERSION:
            raise ValueError(f"Unsupported header version: {version}")
        
        return cls(tag, version, footer_size)


@dataclass
class MDBShardFileFooter:
    """Shard file footer structure"""
    version: int = MDB_SHARD_FOOTER_VERSION
    file_info_offset: int = 0
    cas_info_offset: int = 0
    file_lookup_offset: int = 0
    file_lookup_num_entry: int = 0
    cas_lookup_offset: int = 0
    cas_lookup_num_entry: int = 0
    chunk_lookup_offset: int = 0
    chunk_lookup_num_entry: int = 0
    chunk_hash_hmac_key: HMACKey = field(default_factory=HMACKey)
    shard_creation_timestamp: int = 0
    shard_key_expiry: int = 0
    _buffer: List[int] = field(default_factory=lambda: [0] * 6)
    stored_bytes_on_disk: int = 0
    materialized_bytes: int = 0
    stored_bytes: int = 0
    footer_offset: int = 0
    
    def serialize(self) -> bytes:
        """Serialize to bytes"""
        data = struct.pack('<Q', self.version)
        data += struct.pack('<Q', self.file_info_offset)
        data += struct.pack('<Q', self.cas_info_offset)
        data += struct.pack('<Q', self.file_lookup_offset)
        data += struct.pack('<Q', self.file_lookup_num_entry)
        data += struct.pack('<Q', self.cas_lookup_offset)
        data += struct.pack('<Q', self.cas_lookup_num_entry)
        data += struct.pack('<Q', self.chunk_lookup_offset)
        data += struct.pack('<Q', self.chunk_lookup_num_entry)
        data += self.chunk_hash_hmac_key.data
        data += struct.pack('<Q', self.shard_creation_timestamp)
        data += struct.pack('<Q', self.shard_key_expiry)
        data += struct.pack('<6Q', *self._buffer)
        data += struct.pack('<Q', self.stored_bytes_on_disk)
        data += struct.pack('<Q', self.materialized_bytes)
        data += struct.pack('<Q', self.stored_bytes)
        data += struct.pack('<Q', self.footer_offset)
        return data
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'MDBShardFileFooter':
        """Deserialize from bytes"""
        if len(data) < 192:
            raise ValueError("Insufficient data for footer")
        
        values = struct.unpack('<24Q', data)
        
        version = values[0]
        if version != MDB_SHARD_FOOTER_VERSION:
            raise ValueError(f"Unsupported footer version: {version}")
        
        # Extract HMAC key (4 u64 values = 32 bytes)
        hmac_key_values = values[9:13]
        hmac_key_data = struct.pack('<4Q', *hmac_key_values)
        hmac_key = HMACKey(hmac_key_data)
        
        return cls(
            version=version,
            file_info_offset=values[1],
            cas_info_offset=values[2],
            file_lookup_offset=values[3],
            file_lookup_num_entry=values[4],
            cas_lookup_offset=values[5],
            cas_lookup_num_entry=values[6],
            chunk_lookup_offset=values[7],
            chunk_lookup_num_entry=values[8],
            chunk_hash_hmac_key=hmac_key,
            shard_creation_timestamp=values[13],
            shard_key_expiry=values[14],
            _buffer=list(values[15:21]),
            stored_bytes_on_disk=values[21],
            materialized_bytes=values[22],
            stored_bytes=values[23],
            footer_offset=values[23]  # Last field is footer_offset
        )


@dataclass
class FileDataSequenceHeader:
    """File data sequence header"""
    file_hash: MerkleHash = field(default_factory=MerkleHash)
    file_flags: int = 0
    num_entries: int = 0
    _unused: int = 0
    
    def contains_verification(self) -> bool:
        return (self.file_flags & MDB_FILE_FLAG_WITH_VERIFICATION) != 0
    
    def contains_metadata_ext(self) -> bool:
        return (self.file_flags & MDB_FILE_FLAG_WITH_METADATA_EXT) != 0
    
    def is_bookend(self) -> bool:
        return self.file_hash.is_bookend()
    
    @classmethod
    def bookend(cls) -> 'FileDataSequenceHeader':
        return cls(file_hash=MerkleHash.bookend())
    
    def serialize(self) -> bytes:
        return self.file_hash.data + struct.pack('<2LQ', self.file_flags, self.num_entries, self._unused)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'FileDataSequenceHeader':
        if len(data) < 48:
            raise ValueError("Insufficient data for FileDataSequenceHeader")
        
        file_hash = MerkleHash(data[:32])
        file_flags, num_entries, unused = struct.unpack('<2LQ', data[32:48])
        
        return cls(file_hash, file_flags, num_entries, unused)


@dataclass
class FileDataSequenceEntry:
    """File data sequence entry"""
    cas_hash: MerkleHash = field(default_factory=MerkleHash)
    cas_flags: int = 0
    unpacked_segment_bytes: int = 0
    chunk_index_start: int = 0
    chunk_index_end: int = 0
    
    def serialize(self) -> bytes:
        return self.cas_hash.data + struct.pack('<4L', self.cas_flags, self.unpacked_segment_bytes, 
                                               self.chunk_index_start, self.chunk_index_end)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'FileDataSequenceEntry':
        if len(data) < 48:
            raise ValueError("Insufficient data for FileDataSequenceEntry")
        
        cas_hash = MerkleHash(data[:32])
        cas_flags, unpacked_segment_bytes, chunk_index_start, chunk_index_end = struct.unpack('<4L', data[32:48])
        
        return cls(cas_hash, cas_flags, unpacked_segment_bytes, chunk_index_start, chunk_index_end)


@dataclass
class FileVerificationEntry:
    """File verification entry"""
    range_hash: MerkleHash = field(default_factory=MerkleHash)
    _unused: List[int] = field(default_factory=lambda: [0, 0])
    
    def serialize(self) -> bytes:
        return self.range_hash.data + struct.pack('<2Q', *self._unused)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'FileVerificationEntry':
        if len(data) < 48:
            raise ValueError("Insufficient data for FileVerificationEntry")
        
        range_hash = MerkleHash(data[:32])
        unused = list(struct.unpack('<2Q', data[32:48]))
        
        return cls(range_hash, unused)


@dataclass
class FileMetadataExt:
    """Extended file metadata"""
    sha256: MerkleHash = field(default_factory=MerkleHash)
    _unused: List[int] = field(default_factory=lambda: [0, 0])
    
    def serialize(self) -> bytes:
        return self.sha256.data + struct.pack('<2Q', *self._unused)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'FileMetadataExt':
        if len(data) < 48:
            raise ValueError("Insufficient data for FileMetadataExt")
        
        sha256 = MerkleHash(data[:32])
        unused = list(struct.unpack('<2Q', data[32:48]))
        
        return cls(sha256, unused)


@dataclass
class MDBFileInfo:
    """Complete file information"""
    metadata: FileDataSequenceHeader = field(default_factory=FileDataSequenceHeader)
    segments: List[FileDataSequenceEntry] = field(default_factory=list)
    verification: List[FileVerificationEntry] = field(default_factory=list)
    metadata_ext: Optional[FileMetadataExt] = None


@dataclass
class CASChunkSequenceHeader:
    """CAS chunk sequence header"""
    cas_hash: MerkleHash = field(default_factory=MerkleHash)
    cas_flags: int = 0
    num_entries: int = 0
    num_bytes_in_cas: int = 0
    num_bytes_on_disk: int = 0
    
    def is_bookend(self) -> bool:
        return self.cas_hash.is_bookend()
    
    @classmethod
    def bookend(cls) -> 'CASChunkSequenceHeader':
        return cls(cas_hash=MerkleHash.bookend())
    
    def serialize(self) -> bytes:
        return self.cas_hash.data + struct.pack('<4L', self.cas_flags, self.num_entries, 
                                               self.num_bytes_in_cas, self.num_bytes_on_disk)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'CASChunkSequenceHeader':
        if len(data) < 48:
            raise ValueError("Insufficient data for CASChunkSequenceHeader")
        
        cas_hash = MerkleHash(data[:32])
        cas_flags, num_entries, num_bytes_in_cas, num_bytes_on_disk = struct.unpack('<4L', data[32:48])
        
        return cls(cas_hash, cas_flags, num_entries, num_bytes_in_cas, num_bytes_on_disk)


@dataclass
class CASChunkSequenceEntry:
    """CAS chunk sequence entry"""
    chunk_hash: MerkleHash = field(default_factory=MerkleHash)
    unpacked_segment_bytes: int = 0
    chunk_byte_range_start: int = 0
    _unused: int = 0
    
    def serialize(self) -> bytes:
        return self.chunk_hash.data + struct.pack('<2LQ', self.chunk_byte_range_start, 
                                                 self.unpacked_segment_bytes, self._unused)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'CASChunkSequenceEntry':
        if len(data) < 48:
            raise ValueError("Insufficient data for CASChunkSequenceEntry")
        
        chunk_hash = MerkleHash(data[:32])
        chunk_byte_range_start, unpacked_segment_bytes, unused = struct.unpack('<2LQ', data[32:48])
        
        return cls(chunk_hash, unpacked_segment_bytes, chunk_byte_range_start, unused)


@dataclass
class MDBCASInfo:
    """Complete CAS information"""
    metadata: CASChunkSequenceHeader = field(default_factory=CASChunkSequenceHeader)
    chunks: List[CASChunkSequenceEntry] = field(default_factory=list)


@dataclass
class FileLookupEntry:
    """File lookup table entry"""
    truncated_hash: int = 0
    file_index: int = 0
    
    def serialize(self) -> bytes:
        return struct.pack('<QL', self.truncated_hash, self.file_index)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'FileLookupEntry':
        if len(data) < 12:
            raise ValueError("Insufficient data for FileLookupEntry")
        
        truncated_hash, file_index = struct.unpack('<QL', data[:12])
        return cls(truncated_hash, file_index)


@dataclass
class CASLookupEntry:
    """CAS lookup table entry"""
    truncated_hash: int = 0
    cas_index: int = 0
    
    def serialize(self) -> bytes:
        return struct.pack('<QL', self.truncated_hash, self.cas_index)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'CASLookupEntry':
        if len(data) < 12:
            raise ValueError("Insufficient data for CASLookupEntry")
        
        truncated_hash, cas_index = struct.unpack('<QL', data[:12])
        return cls(truncated_hash, cas_index)


@dataclass
class ChunkLookupEntry:
    """Chunk lookup table entry"""
    truncated_hash: int = 0
    cas_index: int = 0
    chunk_index: int = 0
    
    def serialize(self) -> bytes:
        return struct.pack('<Q2L', self.truncated_hash, self.cas_index, self.chunk_index)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'ChunkLookupEntry':
        if len(data) < 16:
            raise ValueError("Insufficient data for ChunkLookupEntry")
        
        truncated_hash, cas_index, chunk_index = struct.unpack('<Q2L', data[:16])
        return cls(truncated_hash, cas_index, chunk_index)


@dataclass
class ShardData:
    """Complete shard data structure"""
    header: MDBShardFileHeader = field(default_factory=MDBShardFileHeader)
    footer: MDBShardFileFooter = field(default_factory=MDBShardFileFooter)
    file_info: List[MDBFileInfo] = field(default_factory=list)
    cas_info: List[MDBCASInfo] = field(default_factory=list)
    file_lookup: List[FileLookupEntry] = field(default_factory=list)
    cas_lookup: List[CASLookupEntry] = field(default_factory=list)
    chunk_lookup: List[ChunkLookupEntry] = field(default_factory=list)


def _read_exact(file: BinaryIO, size: int) -> bytes:
    """Read exactly size bytes from file, raising error if not available"""
    data = file.read(size)
    if len(data) != size:
        raise ValueError(f"Expected {size} bytes, got {len(data)}")
    return data


def _truncate_hash(hash_value: MerkleHash) -> int:
    """Truncate hash to first 8 bytes for lookup tables"""
    return struct.unpack('<Q', hash_value.data[:8])[0]


def deserialize_shard(file_path: str) -> ShardData:
    """Deserialize a complete shard file"""
    with open(file_path, 'rb') as f:
        return deserialize_shard_from_file(f)


def deserialize_shard_from_file(file: BinaryIO) -> ShardData:
    """Deserialize a shard from an open file object"""
    # Read header
    file.seek(0)
    header_data = _read_exact(file, 48)
    header = MDBShardFileHeader.deserialize(header_data)
    
    # Read footer
    file.seek(-header.footer_size, 2)  # Seek to end minus footer size
    footer_data = _read_exact(file, header.footer_size)
    footer = MDBShardFileFooter.deserialize(footer_data)
    
    # Read file info section
    file_info = []
    file.seek(footer.file_info_offset)
    
    while True:
        header_data = _read_exact(file, 48)
        file_header = FileDataSequenceHeader.deserialize(header_data)
        
        if file_header.is_bookend():
            break
        
        # Read file data sequence entries
        segments = []
        for _ in range(file_header.num_entries):
            entry_data = _read_exact(file, 48)
            segments.append(FileDataSequenceEntry.deserialize(entry_data))
        
        # Read verification entries if present
        verification = []
        if file_header.contains_verification():
            for _ in range(file_header.num_entries):
                verify_data = _read_exact(file, 48)
                verification.append(FileVerificationEntry.deserialize(verify_data))
        
        # Read metadata extension if present
        metadata_ext = None
        if file_header.contains_metadata_ext():
            ext_data = _read_exact(file, 48)
            metadata_ext = FileMetadataExt.deserialize(ext_data)
        
        file_info.append(MDBFileInfo(file_header, segments, verification, metadata_ext))
    
    # Read CAS info section
    cas_info = []
    file.seek(footer.cas_info_offset)
    
    while True:
        header_data = _read_exact(file, 48)
        cas_header = CASChunkSequenceHeader.deserialize(header_data)
        
        if cas_header.is_bookend():
            break
        
        # Read CAS chunk entries
        chunks = []
        for _ in range(cas_header.num_entries):
            chunk_data = _read_exact(file, 48)
            chunks.append(CASChunkSequenceEntry.deserialize(chunk_data))
        
        cas_info.append(MDBCASInfo(cas_header, chunks))
    
    # Read lookup tables
    file_lookup = []
    if footer.file_lookup_num_entry > 0:
        file.seek(footer.file_lookup_offset)
        for _ in range(footer.file_lookup_num_entry):
            entry_data = _read_exact(file, 12)
            file_lookup.append(FileLookupEntry.deserialize(entry_data))
    
    cas_lookup = []
    if footer.cas_lookup_num_entry > 0:
        file.seek(footer.cas_lookup_offset)
        for _ in range(footer.cas_lookup_num_entry):
            entry_data = _read_exact(file, 12)
            cas_lookup.append(CASLookupEntry.deserialize(entry_data))
    
    chunk_lookup = []
    if footer.chunk_lookup_num_entry > 0:
        file.seek(footer.chunk_lookup_offset)
        for _ in range(footer.chunk_lookup_num_entry):
            entry_data = _read_exact(file, 16)
            chunk_lookup.append(ChunkLookupEntry.deserialize(entry_data))
    
    return ShardData(header, footer, file_info, cas_info, file_lookup, cas_lookup, chunk_lookup)


def serialize_shard(shard: ShardData, file_path: str) -> None:
    """Serialize a complete shard to file"""
    with open(file_path, 'wb') as f:
        serialize_shard_to_file(shard, f)


def serialize_shard_to_file(shard: ShardData, file: BinaryIO) -> None:
    """Serialize a shard to an open file object"""
    byte_position = 0
    
    # Write header
    header_data = shard.header.serialize()
    file.write(header_data)
    byte_position += len(header_data)
    
    # Update footer with file info offset
    shard.footer.file_info_offset = byte_position
    
    # Write file info section
    for file_info in shard.file_info:
        file.write(file_info.metadata.serialize())
        byte_position += 48
        
        for segment in file_info.segments:
            file.write(segment.serialize())
            byte_position += 48
        
        if file_info.metadata.contains_verification():
            for verify in file_info.verification:
                file.write(verify.serialize())
                byte_position += 48
        
        if file_info.metadata.contains_metadata_ext() and file_info.metadata_ext:
            file.write(file_info.metadata_ext.serialize())
            byte_position += 48
    
    # Write file info bookend
    file.write(FileDataSequenceHeader.bookend().serialize())
    byte_position += 48
    
    # Update footer with CAS info offset
    shard.footer.cas_info_offset = byte_position
    
    # Write CAS info section
    for cas_info in shard.cas_info:
        file.write(cas_info.metadata.serialize())
        byte_position += 48
        
        for chunk in cas_info.chunks:
            file.write(chunk.serialize())
            byte_position += 48
    
    # Write CAS info bookend
    file.write(CASChunkSequenceHeader.bookend().serialize())
    byte_position += 48
    
    # Update footer with lookup table offsets
    shard.footer.file_lookup_offset = byte_position
    shard.footer.file_lookup_num_entry = len(shard.file_lookup)
    
    # Write file lookup table
    for entry in shard.file_lookup:
        file.write(entry.serialize())
        byte_position += 12
    
    shard.footer.cas_lookup_offset = byte_position
    shard.footer.cas_lookup_num_entry = len(shard.cas_lookup)
    
    # Write CAS lookup table
    for entry in shard.cas_lookup:
        file.write(entry.serialize())
        byte_position += 12
    
    shard.footer.chunk_lookup_offset = byte_position
    shard.footer.chunk_lookup_num_entry = len(shard.chunk_lookup)
    
    # Write chunk lookup table
    for entry in shard.chunk_lookup:
        file.write(entry.serialize())
        byte_position += 16
    
    # Update footer offset and write footer
    shard.footer.footer_offset = byte_position
    footer_data = shard.footer.serialize()
    file.write(footer_data)


def find_file_by_hash(shard: ShardData, file_hash: MerkleHash) -> Optional[MDBFileInfo]:
    """Find file info by hash using lookup table if available"""
    if shard.file_lookup:
        # Use lookup table for fast search
        truncated = _truncate_hash(file_hash)
        for entry in shard.file_lookup:
            if entry.truncated_hash == truncated:
                # Verify full hash
                if entry.file_index < len(shard.file_info):
                    file_info = shard.file_info[entry.file_index]
                    if file_info.metadata.file_hash.data == file_hash.data:
                        return file_info
    else:
        # Linear search through all files
        for file_info in shard.file_info:
            if file_info.metadata.file_hash.data == file_hash.data:
                return file_info
    
    return None


def find_cas_by_hash(shard: ShardData, cas_hash: MerkleHash) -> Optional[MDBCASInfo]:
    """Find CAS info by hash using lookup table if available"""
    if shard.cas_lookup:
        # Use lookup table for fast search
        truncated = _truncate_hash(cas_hash)
        for entry in shard.cas_lookup:
            if entry.truncated_hash == truncated:
                # Verify full hash
                if entry.cas_index < len(shard.cas_info):
                    cas_info = shard.cas_info[entry.cas_index]
                    if cas_info.metadata.cas_hash.data == cas_hash.data:
                        return cas_info
    else:
        # Linear search through all CAS blocks
        for cas_info in shard.cas_info:
            if cas_info.metadata.cas_hash.data == cas_hash.data:
                return cas_info
    
    return None


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python shard.py <shard_file>")
        sys.exit(1)
    
    try:
        shard_data = deserialize_shard(sys.argv[1])
        print(f"Successfully loaded shard: {sys.argv[1]}")
        print(f"Header version: {shard_data.header.version}")
        print(f"Footer version: {shard_data.footer.version}")
        print(f"Number of files: {len(shard_data.file_info)}")
        print(f"Number of CAS blocks: {len(shard_data.cas_info)}")
        print(f"File lookup entries: {len(shard_data.file_lookup)}")
        print(f"CAS lookup entries: {len(shard_data.cas_lookup)}")
        print(f"Chunk lookup entries: {len(shard_data.chunk_lookup)}")
        print(f"Stored bytes: {shard_data.footer.stored_bytes}")
        print(f"Materialized bytes: {shard_data.footer.materialized_bytes}")
        
        # Test round-trip serialization
        output_path = sys.argv[1] + ".copy"
        serialize_shard(shard_data, output_path)
        print(f"Round-trip test: wrote copy to {output_path}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1) 