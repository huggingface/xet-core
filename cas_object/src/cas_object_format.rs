use std::cmp::min;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem::{size_of, size_of_val};

use anyhow::anyhow;
use bytes::Buf;
use deduplication::test_utils::raw_xorb_to_vec;
use deduplication::RawXorbData;
use futures::AsyncReadExt;
use mdb_shard::chunk_verification::range_hash_from_chunks;
use merkledb::constants::{IDEAL_CAS_BLOCK_SIZE, TARGET_CDC_CHUNK_SIZE};
use merkledb::prelude::MerkleDBHighLevelMethodsV1;
use merkledb::{ChunkInfo, MerkleMemDB};
use merklehash::{DataHash, MerkleHash};
use more_asserts::*;
use serde::Serialize;
use tracing::warn;
use utils::serialization_utils::*;

use crate::cas_chunk_format::{deserialize_chunk, serialize_chunk};
use crate::constants::CAS_OBJECT_COMPRESSION_SCHEME_RETEST_INTERVAL;
use crate::error::{CasObjectError, Validate};
use crate::{CASChunkHeader, CompressionScheme};

pub type CasObjectIdent = [u8; 7];
pub(crate) const CAS_OBJECT_FORMAT_IDENT: CasObjectIdent = [b'X', b'E', b'T', b'B', b'L', b'O', b'B'];
pub(crate) const CAS_OBJECT_FORMAT_VERSION_V0: u8 = 0;
pub(crate) const CAS_OBJECT_FORMAT_IDENT_HASHES: CasObjectIdent = [b'X', b'B', b'L', b'B', b'H', b'S', b'H'];
pub(crate) const CAS_OBJECT_FORMAT_IDENT_BOUNDARIES: CasObjectIdent = [b'X', b'B', b'L', b'B', b'B', b'N', b'D'];
pub(crate) const CAS_OBJECT_FORMAT_VERSION: u8 = 1;
pub(crate) const CAS_OBJECT_FORMAT_HASHES_VERSION: u8 = 0;

// This is 1 as we can test on the struct using this version field whether we have the unpacked boundary lengths
pub(crate) const CAS_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO: u8 = 0;
pub(crate) const CAS_OBJECT_FORMAT_BOUNDARIES_VERSION: u8 = 1;
const _CAS_OBJECT_INFO_DEFAULT_LENGTH_V0: u32 = 60;
const CAS_OBJECT_INFO_DEFAULT_LENGTH: u32 = 92;

const AVERAGE_NUM_CHUNKS_PER_XORB: usize = IDEAL_CAS_BLOCK_SIZE / TARGET_CDC_CHUNK_SIZE;
// Decide array preallocation size based on the declared size, to prevent an adversarial
// giant size that leads to OOM on allocation.
#[inline]
fn prealloc_num_chunks(declared_size: usize) -> usize {
    // We add a bit buffer to the average size, hoping to reduce reallocation if
    // the actual number of chunks exceeds AVERAGE_NUM_CHUNKS_PER_XORB.
    declared_size.min(AVERAGE_NUM_CHUNKS_PER_XORB * 9 / 8)
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
/// Info struct for [CasObject]. This is stored at the end of the XORB.
/// DO NOT USE in any new code
pub struct CasObjectInfoV0 {
    /// CAS identifier: "XETBLOB"
    pub ident: CasObjectIdent,

    /// Format version, expected to be 0 right now.
    pub version: u8,

    /// 256-bits, 32-bytes, The CAS Hash of this Xorb.
    pub cashash: MerkleHash,

    /// Total number of chunks in the Xorb. Length of chunk_boundary_offsets & chunk_hashes vectors.
    pub num_chunks: u32,

    /// Byte offset marking the boundary of each chunk. Length of vector is num_chunks.
    ///
    /// This vector only contains boundaries, so assumes the first chunk starts at offset 0.
    /// The final entry in vector is the total length of the chunks.
    /// See example below.
    /// chunk[n] are bytes in [chunk_boundary_offsets[n-1], chunk_boundary_offsets[n])
    /// ```
    /// // ex.             chunks: [  0,   1,   2,   3 ]
    /// // chunk_boundary_offsets: [ 100, 200, 300, 400]
    /// ```
    pub chunk_boundary_offsets: Vec<u32>,

    /// Merklehash for each chunk stored in the Xorb. Length of vector is num_chunks.
    pub chunk_hashes: Vec<MerkleHash>,

    #[serde(skip)]
    /// Unused 16-byte buffer to allow for future extensibility.
    _buffer: [u8; 16],
}

impl Default for CasObjectInfoV0 {
    fn default() -> Self {
        CasObjectInfoV0 {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION_V0,
            cashash: MerkleHash::default(),
            num_chunks: 0,
            chunk_boundary_offsets: Vec::new(),
            chunk_hashes: Vec::new(),
            _buffer: Default::default(),
        }
    }
}

impl CasObjectInfoV0 {
    /// Serialize CasObjectInfoV0 to provided Writer.
    ///
    /// Assumes caller has set position of Writer to appropriate location for serialization.
    #[deprecated]
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, CasObjectError> {
        let mut total_bytes_written = 0;

        // Helper function to write data and update the byte count
        let mut write_bytes = |data: &[u8]| -> Result<(), CasObjectError> {
            writer.write_all(data)?;
            total_bytes_written += data.len();
            Ok(())
        };

        // Write fixed-size fields, in order: ident, version, cashash, num_chunks
        write_bytes(&self.ident)?;
        write_bytes(&[self.version])?;
        write_bytes(self.cashash.as_bytes())?;
        write_bytes(&self.num_chunks.to_le_bytes())?;

        // write variable field: chunk boundaries & hashes
        for offset in &self.chunk_boundary_offsets {
            write_bytes(&offset.to_le_bytes())?;
        }
        for hash in &self.chunk_hashes {
            write_bytes(hash.as_bytes())?;
        }

        // write closing metadata
        write_bytes(&self._buffer)?;

        Ok(total_bytes_written)
    }

    /// Construct CasObjectInfoV0 object from Read.
    ///
    /// Expects metadata struct is found at end of Reader, written out in struct order.
    #[deprecated]
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<(Self, u32), CasObjectError> {
        let mut total_bytes_read: u32 = 0;

        // Helper function to read data and update the byte count
        let mut read_bytes = |data: &mut [u8]| -> Result<(), CasObjectError> {
            reader.read_exact(data)?;
            total_bytes_read += data.len() as u32;
            Ok(())
        };

        let mut ident = [0u8; 7];
        read_bytes(&mut ident)?;

        if ident != CAS_OBJECT_FORMAT_IDENT {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident")));
        }

        let mut version = [0u8; 1];
        read_bytes(&mut version)?;

        if version[0] != CAS_OBJECT_FORMAT_VERSION_V0 {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version")));
        }

        let (s, bytes_read_v0) = Self::deserialize_v0(reader)?;

        Ok((s, total_bytes_read + bytes_read_v0))
    }

    pub fn deserialize_v0<R: Read>(reader: &mut R) -> Result<(Self, u32), CasObjectError> {
        let mut total_bytes_read: u32 = 0;

        // Helper function to read data and update the byte count
        let mut read_bytes = |data: &mut [u8]| -> Result<(), CasObjectError> {
            reader.read_exact(data)?;
            total_bytes_read += data.len() as u32;
            Ok(())
        };

        let mut buf = [0u8; size_of::<MerkleHash>()];
        read_bytes(&mut buf)?;
        let cashash = MerkleHash::from(&buf);

        let mut num_chunks = [0u8; size_of::<u32>()];
        read_bytes(&mut num_chunks)?;
        let num_chunks = u32::from_le_bytes(num_chunks);

        let mut chunk_boundary_offsets = Vec::with_capacity(prealloc_num_chunks(num_chunks as usize));
        for _ in 0..num_chunks {
            let mut offset = [0u8; size_of::<u32>()];
            read_bytes(&mut offset)?;
            chunk_boundary_offsets.push(u32::from_le_bytes(offset));
        }
        let mut chunk_hashes = Vec::with_capacity(prealloc_num_chunks(num_chunks as usize));
        for _ in 0..num_chunks {
            let mut hash = [0u8; size_of::<MerkleHash>()];
            read_bytes(&mut hash)?;
            chunk_hashes.push(MerkleHash::from(&hash));
        }

        let mut _buffer = [0u8; 16];
        read_bytes(&mut _buffer)?;

        Ok((
            CasObjectInfoV0 {
                ident: CAS_OBJECT_FORMAT_IDENT,
                version: CAS_OBJECT_FORMAT_VERSION_V0,
                cashash,
                num_chunks,
                chunk_boundary_offsets,
                chunk_hashes,
                _buffer,
            },
            total_bytes_read,
        ))
    }

    /// Construct CasObjectInfo object from AsyncRead.
    /// assumes that the ident and version have already been read and verified.
    ///
    /// verifies that the length of the footer data matches the length field at the very end of the buffer
    pub async fn deserialize_async<R: futures::io::AsyncRead + Unpin>(
        reader: &mut R,
        version: u8,
    ) -> Result<(Self, u32), CasObjectError> {
        // already read 8 bytes (ident + version)
        let mut total_bytes_read: u32 = (size_of::<CasObjectIdent>() + size_of::<u8>()) as u32;

        // Helper function to read data and update the byte count
        async fn read_bytes<R: futures::io::AsyncRead + Unpin>(
            reader: &mut R,
            total_bytes_read: &mut u32,
            buf: &mut [u8],
        ) -> Result<(), CasObjectError> {
            reader.read_exact(buf).await?;
            *total_bytes_read += buf.len() as u32;
            Ok(())
        }

        // notable difference from non-async version, we skip reading the ident and version
        // these fields have been verified before.

        let mut buf = [0u8; size_of::<MerkleHash>()];
        read_bytes(reader, &mut total_bytes_read, &mut buf).await?;
        let cashash = MerkleHash::from(&buf);

        let mut num_chunks = [0u8; size_of::<u32>()];
        read_bytes(reader, &mut total_bytes_read, &mut num_chunks).await?;
        let num_chunks = u32::from_le_bytes(num_chunks);

        let mut chunk_boundary_offsets = Vec::with_capacity(prealloc_num_chunks(num_chunks as usize));
        for _ in 0..num_chunks {
            let mut offset = [0u8; size_of::<u32>()];
            read_bytes(reader, &mut total_bytes_read, &mut offset).await?;
            chunk_boundary_offsets.push(u32::from_le_bytes(offset));
        }
        let mut chunk_hashes = Vec::with_capacity(prealloc_num_chunks(num_chunks as usize));
        for _ in 0..num_chunks {
            let mut hash = [0u8; size_of::<MerkleHash>()];
            read_bytes(reader, &mut total_bytes_read, &mut hash).await?;
            chunk_hashes.push(MerkleHash::from(&hash));
        }

        let mut _buffer = [0u8; 16];
        read_bytes(reader, &mut total_bytes_read, &mut _buffer).await?;

        Ok((
            CasObjectInfoV0 {
                ident: CAS_OBJECT_FORMAT_IDENT,
                version,
                cashash,
                num_chunks,
                chunk_boundary_offsets,
                chunk_hashes,
                _buffer,
            },
            total_bytes_read,
        ))
    }
}

#[allow(clippy::empty_line_after_doc_comments)]
#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
/// Info struct for [CasObject]. This is stored at the end of the XORB.
pub struct CasObjectInfoV1 {
    /// CAS identifier: "XETBLOB"
    pub ident: CasObjectIdent,

    /// Format version, expected to be 1 right now.
    pub version: u8,

    /// 256-bits, 32-bytes, The CAS Hash of this Xorb.
    pub cashash: MerkleHash,

    ///////////////////////////////////////////////////////////////////
    /// The hashes section

    /// CAS identifier: "XBLBHSH"
    pub ident_hash_section: CasObjectIdent,

    /// The version of the chunk hash section.
    pub hashes_version: u8,

    /// Total number of chunks in the Xorb.  Duplicated here.
    /// This only exists in the physical serialized layout.
    // _num_chunks_2: u32,

    /// Merklehash for each chunk stored in the Xorb. Length of vector is num_chunks.
    pub chunk_hashes: Vec<MerkleHash>,

    ///////////////////////////////////////////////////////////////////
    /// The boundaries and index metadata

    /// The identity for the metadata section; should be "XBLBMDT"
    pub ident_boundary_section: CasObjectIdent,

    /// The version of the boundary section.
    pub boundaries_version: u8,

    /// Total number of chunks in the Xorb.  Duplicated here.
    /// This only exists in the physical serialized layout
    // _num_chunks_3: u32,

    /// Byte offset marking the boundary of each chunk in physical layout including chunk header.
    /// Length of vector is num_chunks.
    ///
    /// This vector only contains boundaries, so assumes the first chunk starts at offset 0.
    /// The final entry in vector is the total length of the chunks.
    /// See example below.
    /// chunk[n] are bytes in [chunk_boundary_offsets[n-1], chunk_boundary_offsets[n])
    /// ```
    /// // ex.             chunks: [  0,   1,   2,   3 ]
    /// // chunk_boundary_offsets: [ 100, 200, 300, 400]
    /// ```
    pub chunk_boundary_offsets: Vec<u32>,

    /// The byte offsets marking the boundary of each chunk in uncompressed layout without header,
    /// assuming that each chunk gets unzipped and concatenated.
    /// Length of vector is num_chunks.
    /// This permits range queries on the contents of the xorb. The uncompressed length of
    /// chunk k can be determined by unpacked_chunk_offsets[k] - unpacked_chunk_offsets[k - 1].
    pub unpacked_chunk_offsets: Vec<u32>,

    /// Below this everything is fixed; these fields are in exactly the same place.
    ///
    /// Total number of chunks in the Xorb.  This is also duplicated in the serialization
    /// at the start of each section.
    pub num_chunks: u32,

    // The number of bytes from the end of this footer to the start of the hashes section
    pub hashes_section_offset_from_end: u32,

    // The number of bytes from the end of this footer to the start of the boundaries section
    pub boundary_section_offset_from_end: u32,

    #[serde(skip)]
    /// Unused 16-byte buffer to allow for future extensibility.
    _buffer: [u8; 16],
}

impl Default for CasObjectInfoV1 {
    fn default() -> Self {
        let mut s = CasObjectInfoV1 {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            cashash: MerkleHash::default(),

            ident_hash_section: CAS_OBJECT_FORMAT_IDENT_HASHES,
            hashes_version: CAS_OBJECT_FORMAT_HASHES_VERSION,
            chunk_hashes: Vec::new(),

            ident_boundary_section: CAS_OBJECT_FORMAT_IDENT_BOUNDARIES,
            boundaries_version: CAS_OBJECT_FORMAT_BOUNDARIES_VERSION,
            chunk_boundary_offsets: Vec::new(),
            unpacked_chunk_offsets: Vec::new(),

            num_chunks: 0,
            hashes_section_offset_from_end: 0,
            boundary_section_offset_from_end: 0,
            _buffer: Default::default(),
        };

        s.fill_in_boundary_offsets();
        s
    }
}

impl CasObjectInfoV1 {
    pub fn serialized_length(&self) -> usize {
        size_of::<CasObjectIdent>() * 3 // ident, ident_hash_section, ident_boundary_section
            + size_of::<u8>() * 3 // version, hashes_version, boundaries_version
            + size_of::<u32>() * 5 // num_chunks, hashes_section_offset_from_end, boundary_section_offset_from_end,
                                   // chunk_boundary_offsets, unpacked_chunk_offsets
            + size_of::<MerkleHash>() * self.chunk_hashes.len() // chunk_hashes
            + size_of_val(&self._buffer) // _buffer
            + self.chunk_boundary_offsets.len() * size_of::<u32>() // chunk_boundary_offsets
            + self.unpacked_chunk_offsets.len() * size_of::<u32>() // unpacked_chunk_offsets
            + size_of::<MerkleHash>() // cashash
    }

    /// Serialize CasObjectInfoV1 to provided Writer.
    ///
    /// Assumes caller has set position of Writer to appropriate location for serialization.
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, CasObjectError> {
        let mut counting_writer = countio::Counter::new(writer);
        let w = &mut counting_writer;

        //////////////////////////////////////////////////////////////////////////////////////////////
        // First section (Calf).  (Open to moving to another name.)

        write_bytes(w, &self.ident)?;
        write_u8(w, self.version)?;
        write_hash(w, &self.cashash)?;

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Hash section (Ankle)

        // Write fixed-size fields, in order: ident, version
        write_bytes(w, &self.ident_hash_section)?;
        write_u8(w, self.hashes_version)?;

        // Write number of chunks again.
        write_u32(w, self.num_chunks)?;

        if self.num_chunks as usize != self.chunk_hashes.len() {
            debug_assert_eq!(self.num_chunks as usize, self.chunk_hashes.len());
            return Err(CasObjectError::FormatError(anyhow!(
                "Chunk hash vector not correct length on serialization. ({}, expected {})",
                self.chunk_hashes.len(),
                self.num_chunks
            )));
        }

        for hash in &self.chunk_hashes {
            write_hash(w, hash)?;
        }

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Boundary Section (Foot).

        write_bytes(w, &self.ident_boundary_section)?;
        write_u8(w, self.boundaries_version)?;
        write_u32(w, self.num_chunks)?;

        // write variable field: chunk boundaries
        if self.num_chunks as usize != self.chunk_boundary_offsets.len() {
            debug_assert_eq!(self.num_chunks as usize, self.chunk_boundary_offsets.len());
            return Err(CasObjectError::FormatError(anyhow!(
                "Chunk boundary offset vector not correct length on serialization. ({}, expected {})",
                self.chunk_boundary_offsets.len(),
                self.num_chunks
            )));
        }
        write_u32s(w, &self.chunk_boundary_offsets)?;

        // write variable field: unpacked chunk data offsets
        if self.num_chunks as usize != self.unpacked_chunk_offsets.len() {
            debug_assert_eq!(self.num_chunks as usize, self.unpacked_chunk_offsets.len());
            return Err(CasObjectError::FormatError(anyhow!(
                "Unpacked chunk offset vector not correct length on serialization. ({}, expected {})",
                self.unpacked_chunk_offsets.len(),
                self.num_chunks
            )));
        }
        write_u32s(w, &self.unpacked_chunk_offsets)?;

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Constant length end of footer (Toes).

        // Write num_chunks here, though it's written out multiple places. Here as it applies all over
        // the place.
        write_u32(w, self.num_chunks)?;

        write_u32(w, self.hashes_section_offset_from_end)?;
        write_u32(w, self.boundary_section_offset_from_end)?;

        // write closing metadata
        write_bytes(w, &self._buffer)?;

        Ok(w.writer_bytes())
    }

    /// Construct CasObjectInfo object from Reader + Seek.
    ///
    /// Expects metadata struct is found at end of Reader, written out in struct order.
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<(Self, u32), CasObjectError> {
        let mut counting_reader = countio::Counter::new(reader);
        let r = &mut counting_reader;

        let mut s = Self::default();

        //////////////////////////////////////////////////////////////////////////////////////////////
        // First section.

        read_bytes(r, &mut s.ident)?;

        if s.ident != CAS_OBJECT_FORMAT_IDENT {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident")));
        }

        s.version = read_u8(r)?;

        if s.version == CAS_OBJECT_FORMAT_VERSION_V0 {
            let (sv0, _) = CasObjectInfoV0::deserialize_v0(r)?;
            // we don't have the missing info (unpacked_chunk_offsets), it's OK
            return Ok((Self::from_v0(sv0), r.reader_bytes() as u32));
        } else if s.version != CAS_OBJECT_FORMAT_VERSION {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version")));
        }

        s.cashash = read_hash(r)?;

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Hash section

        let hash_section_begin_byte_offset = r.reader_bytes();

        read_bytes(r, &mut s.ident_hash_section)?;

        if s.ident_hash_section != CAS_OBJECT_FORMAT_IDENT_HASHES {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident for Hash Metadata Section")));
        }

        s.hashes_version = read_u8(r)?;

        if s.hashes_version != CAS_OBJECT_FORMAT_HASHES_VERSION {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version for Hash Metadata Section")));
        }

        let num_chunks_2 = read_u32(r)?;

        // Read in the hashes.
        s.chunk_hashes.reserve(prealloc_num_chunks(num_chunks_2 as usize));
        for _ in 0..num_chunks_2 {
            s.chunk_hashes.push(read_hash(r)?);
        }

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Boundary Section (Foot).

        let boundary_section_begin_byte_offset = r.reader_bytes();

        read_bytes(r, &mut s.ident_boundary_section)?;

        if s.ident_boundary_section != CAS_OBJECT_FORMAT_IDENT_BOUNDARIES {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident for Boundary Metadata Section")));
        }

        s.boundaries_version = read_u8(r)?;

        if s.boundaries_version != CAS_OBJECT_FORMAT_BOUNDARIES_VERSION {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid Format Version for Boundaries Metadata Section"
            )));
        }

        let num_chunks_3 = read_u32(r)?;

        if num_chunks_2 != num_chunks_3 {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: inconsistent num_chunks between hashes and boundaries section."
            )));
        }

        s.chunk_boundary_offsets.reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.chunk_boundary_offsets.push(read_u32(r)?);
        }

        s.unpacked_chunk_offsets.reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.unpacked_chunk_offsets.push(read_u32(r)?);
        }

        // Now the final parts here.
        s.num_chunks = read_u32(r)?;

        if s.num_chunks != num_chunks_2 {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: inconsistent num_chunks between metadata and hashes section."
            )));
        }

        s.hashes_section_offset_from_end = read_u32(r)?;
        s.boundary_section_offset_from_end = read_u32(r)?;

        read_bytes(r, &mut s._buffer)?;

        let end_byte_offset = r.reader_bytes();

        if end_byte_offset - hash_section_begin_byte_offset != s.hashes_section_offset_from_end as usize {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: incorrect hashes_section_offset_from_end."
            )));
        }

        if end_byte_offset - boundary_section_begin_byte_offset != s.boundary_section_offset_from_end as usize {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: incorrect boundary_section_offset_from_end."
            )));
        }

        Ok((s, r.reader_bytes() as u32))
    }

    /// Construct CasObjectInfo object from Reader + Seek.
    ///
    /// Expects metadata struct is found at end of Reader, written out in struct order.
    pub fn deserialize_only_boundaries_section<R: Read + Seek>(reader: &mut R) -> Result<(Self, u32), CasObjectError> {
        let mut s = Self::default();

        // info_length + size of _buffer + size of u32 for offset field
        let offset_to_boundary_section_offset =
            size_of::<u32>() + size_of_val(&s._buffer) + size_of_val(&s.boundary_section_offset_from_end);
        reader.seek(SeekFrom::End(-(offset_to_boundary_section_offset as i64)))?;
        let mut boundary_section_offset_from_end = read_u32(reader)?;

        // add 4 bytes to offset from info_length at the end
        boundary_section_offset_from_end += size_of::<u32>() as u32;
        reader.seek(SeekFrom::End(-(boundary_section_offset_from_end as i64)))?;

        let mut counting_reader = countio::Counter::new(reader);
        let r = &mut counting_reader;

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Boundary Section (Foot).

        read_bytes(r, &mut s.ident_boundary_section)?;

        if s.ident_boundary_section != CAS_OBJECT_FORMAT_IDENT_BOUNDARIES {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident for Boundary Metadata Section")));
        }

        s.boundaries_version = read_u8(r)?;

        if s.boundaries_version != CAS_OBJECT_FORMAT_BOUNDARIES_VERSION {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid Format Version for Boundaries Metadata Section"
            )));
        }

        let num_chunks_boundaries_section = read_u32(r)?;

        s.chunk_boundary_offsets.resize(num_chunks_boundaries_section as usize, 0);
        read_u32s(r, &mut s.chunk_boundary_offsets)?;

        s.unpacked_chunk_offsets.resize(num_chunks_boundaries_section as usize, 0);
        read_u32s(r, &mut s.unpacked_chunk_offsets)?;

        // Now the final parts here.
        s.num_chunks = read_u32(r)?;

        if s.num_chunks != num_chunks_boundaries_section {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: inconsistent num_chunks between metadata and hashes section."
            )));
        }

        s.hashes_section_offset_from_end = read_u32(r)?;
        s.boundary_section_offset_from_end = read_u32(r)?;

        read_bytes(r, &mut s._buffer)?;

        let end_byte_offset = r.reader_bytes();

        if end_byte_offset != s.boundary_section_offset_from_end as usize {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: incorrect boundary_section_offset_from_end."
            )));
        }

        debug_assert!(s.chunk_hashes.is_empty());

        Ok((s, r.reader_bytes() as u32))
    }

    pub async fn deserialize_async_v1<R: futures::io::AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(Self, u32), CasObjectError> {
        // already read 8 bytes (ident + version)
        let total_bytes_read: u32 = (size_of::<CasObjectIdent>() + size_of::<u8>()) as u32;

        let mut counting_reader = countio::Counter::new(reader);
        let r = &mut counting_reader;

        // ident and version have been read already above
        let mut s = Self {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            cashash: read_hash_async(r).await?,
            ..Default::default()
        };

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Hash section

        let hash_section_begin_byte_offset = r.reader_bytes();

        read_bytes_async(r, &mut s.ident_hash_section).await?;

        if s.ident_hash_section != CAS_OBJECT_FORMAT_IDENT_HASHES {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident for Hash Metadata Section")));
        }

        s.hashes_version = read_u8_async(r).await?;

        if s.hashes_version != CAS_OBJECT_FORMAT_HASHES_VERSION {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version for Hash Metadata Section")));
        }

        let num_chunks_2 = read_u32_async(r).await?;

        // Read in the hashes.
        s.chunk_hashes.reserve(prealloc_num_chunks(num_chunks_2 as usize));
        for _ in 0..num_chunks_2 {
            s.chunk_hashes.push(read_hash_async(r).await?);
        }

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Boundary Section (Foot).

        let boundary_section_begin_byte_offset = r.reader_bytes();

        read_bytes_async(r, &mut s.ident_boundary_section).await?;

        if s.ident_boundary_section != CAS_OBJECT_FORMAT_IDENT_BOUNDARIES {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident for Boundary Metadata Section")));
        }

        s.boundaries_version = read_u8_async(r).await?;

        if s.boundaries_version != CAS_OBJECT_FORMAT_BOUNDARIES_VERSION {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid Format Version for Boundaries Metadata Section"
            )));
        }

        let num_chunks_3 = read_u32_async(r).await?;

        if num_chunks_2 != num_chunks_3 {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: inconsistent num_chunks between hashes and boundaries section."
            )));
        }

        s.chunk_boundary_offsets.reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.chunk_boundary_offsets.push(read_u32_async(r).await?);
        }

        s.unpacked_chunk_offsets.reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.unpacked_chunk_offsets.push(read_u32_async(r).await?);
        }

        s.num_chunks = read_u32_async(r).await?;

        if s.num_chunks != num_chunks_2 {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: inconsistent num_chunks between metadata and hashes section."
            )));
        }

        s.hashes_section_offset_from_end = read_u32_async(r).await?;
        s.boundary_section_offset_from_end = read_u32_async(r).await?;

        read_bytes_async(r, &mut s._buffer).await?;

        let end_byte_offset = r.reader_bytes();

        if end_byte_offset - hash_section_begin_byte_offset != s.hashes_section_offset_from_end as usize {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: incorrect hashes_section_offset_from_end."
            )));
        }

        if end_byte_offset - boundary_section_begin_byte_offset != s.boundary_section_offset_from_end as usize {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid: incorrect boundary_section_offset_from_end."
            )));
        }

        Ok((s, r.reader_bytes() as u32 + total_bytes_read))
    }
    /// Construct CasObjectInfo object from AsyncRead.
    /// assumes that the ident and version have already been read and verified.
    ///
    /// verifies that the length of the footer data matches the length field at the very end of the buffer
    pub async fn deserialize_async<R: futures::io::AsyncRead + Unpin>(
        reader: &mut R,
        version: u8,
    ) -> Result<(Self, u32), CasObjectError> {
        if version == 0 {
            let (s, n) = CasObjectInfoV0::deserialize_async(reader, 0).await?;
            // we don't have the missing info (unpacked_chunk_offsets), it's OK
            Ok((Self::from_v0(s), n))
        } else if version == 1 {
            Self::deserialize_async_v1(reader).await
        } else {
            Err(CasObjectError::FormatError(anyhow!(
                "Xorb Format Error: Version {version} not supported by this code version."
            )))
        }
    }

    pub fn from_v0(src: CasObjectInfoV0) -> Self {
        // Fill in all the appropriate fields from the V0 version.
        let mut s = Self {
            ident: src.ident,
            version: CAS_OBJECT_FORMAT_VERSION,
            cashash: src.cashash,
            ident_hash_section: CAS_OBJECT_FORMAT_IDENT_HASHES,
            hashes_version: CAS_OBJECT_FORMAT_HASHES_VERSION,
            chunk_hashes: src.chunk_hashes,
            ident_boundary_section: CAS_OBJECT_FORMAT_IDENT_BOUNDARIES,
            boundaries_version: CAS_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO,
            chunk_boundary_offsets: src.chunk_boundary_offsets,
            unpacked_chunk_offsets: Vec::new(),
            num_chunks: src.num_chunks,
            hashes_section_offset_from_end: 0,
            boundary_section_offset_from_end: 0,
            _buffer: src._buffer,
        };

        s.fill_in_boundary_offsets();
        s
    }

    pub fn from_v0_with_unpacked_chunk_offsets(src: CasObjectInfoV0, unpacked_chunk_offsets: Vec<u32>) -> Self {
        if unpacked_chunk_offsets.len() != src.chunk_boundary_offsets.len() {
            warn!(
                "unpacked_chunk_offsets len ({}) does not match src chunk_boundary_offsets len ({})",
                unpacked_chunk_offsets.len(),
                src.chunk_boundary_offsets.len()
            );
        }
        // Fill in all the appropriate fields from the V0 version.
        let mut s = Self {
            ident: src.ident,
            version: 1,
            cashash: src.cashash,
            ident_hash_section: CAS_OBJECT_FORMAT_IDENT_HASHES,
            hashes_version: CAS_OBJECT_FORMAT_HASHES_VERSION,
            chunk_hashes: src.chunk_hashes,
            ident_boundary_section: CAS_OBJECT_FORMAT_IDENT_BOUNDARIES,
            boundaries_version: CAS_OBJECT_FORMAT_BOUNDARIES_VERSION,
            chunk_boundary_offsets: src.chunk_boundary_offsets,
            unpacked_chunk_offsets,
            num_chunks: src.num_chunks,
            hashes_section_offset_from_end: 0,
            boundary_section_offset_from_end: 0,
            _buffer: Default::default(),
        };

        s.fill_in_boundary_offsets();
        s
    }

    pub fn fill_in_boundary_offsets(&mut self) {
        self.boundary_section_offset_from_end = (size_of_val(&self.ident_boundary_section)
            + size_of_val(&self.boundaries_version)
            + size_of::<u32>() // num_chunks_3
            + self.chunk_boundary_offsets.len() * size_of::<u32>()
            + self.unpacked_chunk_offsets.len() * size_of::<u32>()
            + size_of_val(&self.num_chunks)
            + size_of_val(&self.hashes_section_offset_from_end)
            + size_of_val(&self.boundary_section_offset_from_end)
            + size_of_val(&self._buffer)) as u32;

        self.hashes_section_offset_from_end = (size_of_val(&self.ident_hash_section)
            + size_of_val(&self.hashes_version)
            + size_of::<u32>() // num_chunks_2
            + self.chunk_hashes.len() * size_of::<MerkleHash>()) as u32
            + self.boundary_section_offset_from_end;
    }

    pub fn has_chunk_hashes(&self) -> bool {
        !self.chunk_hashes.is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
/// XORB: 16MB data block for storing chunks.
///
/// Has Info footer, and a set of functions that interact directly with XORB.
///
/// Physical layout of this object is as follows:
/// [START OF XORB]
/// <CHUNK 0>
/// <CHUNK 1>
/// <..>
/// <CHUNK N>
/// <CasObjectInfo>
/// CasObjectInfo length: u32
/// [END OF XORB]
pub struct CasObject {
    /// CasObjectInfo block see [CasObjectInfo] for details.
    pub info: CasObjectInfoV1,

    /// Length of entire info block.
    ///
    /// This is required to be at the end of the CasObject, so readers can read the
    /// final 4 bytes and know the full length of the info block.
    pub info_length: u32,
}

impl Default for CasObject {
    fn default() -> Self {
        Self {
            info: Default::default(),
            info_length: CAS_OBJECT_INFO_DEFAULT_LENGTH,
        }
    }
}

impl CasObject {
    /// Deserializes only the info length field of the footer to tell the user how many bytes
    /// make up the info portion of the xorb.
    ///
    /// Assumes reader has at least size_of::<u32>() bytes, otherwise returns an error.
    pub fn get_info_length<R: Read + Seek>(reader: &mut R) -> Result<u32, CasObjectError> {
        // Go to end of Reader and get length, then jump back to it, and read sequentially
        // read last 4 bytes to get length
        reader.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut info_length = [0u8; 4];
        reader.read_exact(&mut info_length)?;
        let info_length = u32::from_le_bytes(info_length);
        Ok(info_length)
    }

    /// Deserialize the CasObjectInfo struct, the metadata for this Xorb.
    ///
    /// This allows the CasObject to be partially constructed, allowing for range reads inside the CasObject.
    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CasObjectError> {
        let info_length = Self::get_info_length(reader)?;

        // now seek back that many bytes + size of length (u32) and read sequentially.
        reader.seek(SeekFrom::End(-(size_of::<u32>() as i64 + info_length as i64)))?;

        let (info, total_bytes_read) = CasObjectInfoV1::deserialize(reader)?;

        // validate that info_length matches what we read off of header
        if total_bytes_read != info_length {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Info Format Error")));
        }

        Ok(Self { info, info_length })
    }

    /// Construct CasObject object from AsyncRead.
    /// assumes that the ident and version have already been read and verified.
    pub async fn deserialize_async<R: futures::io::AsyncRead + Unpin>(
        reader: &mut R,
        version: u8,
    ) -> Result<Self, CasObjectError> {
        let (info, total_bytes_read) = CasObjectInfoV1::deserialize_async(reader, version).await?;

        let mut info_length_buf = [0u8; size_of::<u32>()];
        // not using read_bytes since we do not want to count these bytes in total_bytes_read
        // the info_length u32 is not counted in its value
        reader.read_exact(&mut info_length_buf).await?;
        let info_length = u32::from_le_bytes(info_length_buf);

        if info_length != total_bytes_read {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Info Format Error")));
        }

        // verify we've read to the end
        if reader.read(&mut [0u8; 8]).await? != 0 {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Reader has content past the end of serialized xorb"
            )));
        }

        Ok(Self { info, info_length })
    }

    pub fn serialize_given_info<W: Write>(w: &mut W, info: CasObjectInfoV1) -> Result<(Self, usize), CasObjectError> {
        let mut total_written_bytes: usize = 0;
        let info_length = info.serialize(w)? as u32;
        total_written_bytes += info_length as usize;
        write_u32(w, info_length)?;
        total_written_bytes += size_of::<u32>();

        let cas_object = Self { info, info_length };

        debug_assert_eq!(cas_object.info_length, info_length);
        debug_assert_eq!(cas_object.info_length as usize, cas_object.info.serialized_length());

        Ok((cas_object, total_written_bytes))
    }

    pub fn from_info(info: CasObjectInfoV1) -> Self {
        let info_length = info.serialized_length() as u32;
        Self { info, info_length }
    }

    /// Validate CasObject.
    /// Verifies each chunk is valid and correctly represented in CasObjectInfo, along with
    /// recomputing the hash and validating it matches CasObjectInfo.
    ///
    /// Returns Ok(Some(cas object)) if recomputed hash matches what is passed in.
    pub fn validate_cas_object<R: Read + Seek>(
        reader: &mut R,
        hash: &MerkleHash,
    ) -> Result<Option<CasObject>, CasObjectError> {
        // 1. deserialize to get Info
        // Errors can occur if either
        // - the object doesn't have at least 4 bytes for the "info_length";
        // - the object doesn't have enough bytes as claimed by "info_length";
        // - the object info format is incorrect (e.g. ident mismatch);
        // and we should reject instead of propagating the error.
        let Some(cas) = CasObject::deserialize(reader).ok_for_format_error()? else {
            return Ok(None);
        };

        // 2. walk chunks from Info
        let mut hash_chunks: Vec<ChunkInfo> = Vec::new();
        let mut cumulative_compressed_length: u32 = 0;
        let mut unpacked_chunk_offset = 0;

        let mut start_offset = 0;
        // Validate each chunk: iterate chunks, deserialize chunk, compare stored hash with
        // computed hash, store chunk hashes for cashash validation
        for idx in 0..cas.info.num_chunks {
            // deserialize each chunk
            reader.seek(SeekFrom::Start(start_offset as u64))?;
            // Reject if chunk is corrupted
            let Some((data, compressed_chunk_length, chunk_uncompressed_length)) =
                deserialize_chunk(reader).ok_for_format_error()?
            else {
                return Ok(None);
            };

            let chunk_hash = merklehash::compute_data_hash(&data);
            hash_chunks.push(ChunkInfo {
                hash: chunk_hash,
                length: chunk_uncompressed_length as usize,
            });

            cumulative_compressed_length += compressed_chunk_length as u32;
            unpacked_chunk_offset += chunk_uncompressed_length;

            // verify chunk hash
            if *cas.info.chunk_hashes.get(idx as usize).unwrap() != chunk_hash {
                warn!("XORB Validation: Chunk hash does not match Info object.");
                return Ok(None);
            }

            let boundary = *cas.info.chunk_boundary_offsets.get(idx as usize).unwrap();

            // verify that cas.chunk[n].len + 1 == cas.chunk_boundary_offsets[n]
            if (start_offset + compressed_chunk_length as u32) != boundary {
                warn!("XORB Validation: Chunk boundary byte index does not match Info object.");
                return Ok(None);
            }

            // set start offset of next chunk as the boundary of the current chunk
            start_offset = boundary;

            // verify unpacked chunk offsets
            if cas.info.boundaries_version == CAS_OBJECT_FORMAT_BOUNDARIES_VERSION
                && unpacked_chunk_offset != *cas.info.unpacked_chunk_offsets.get(idx as usize).unwrap()
            {
                warn!("XORB Validation: Chunk unpacked byte offset does not match Info object.");
                return Ok(None);
            }
        }

        // validate that Info/footer begins immediately after final content xorb.
        // end of for loop completes the content chunks, now should be able to deserialize an Info directly
        let cur_position = reader.stream_position()? as u32;
        let expected_position = cumulative_compressed_length;
        let expected_from_end_position =
            reader.seek(SeekFrom::End(0))? as u32 - cas.info_length - size_of::<u32>() as u32;
        if cur_position != expected_position || cur_position != expected_from_end_position {
            warn!("XORB Validation: Content bytes after known chunks in Info object.");
            return Ok(None);
        }

        // 4. combine hashes to get full xorb hash, compare to provided
        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &hash_chunks);
        let ret = db.finalize(staging);

        if *ret.hash() != *hash || *ret.hash() != cas.info.cashash {
            warn!("XORB Validation: Computed hash does not match provided hash or Info hash.");
            return Ok(None);
        }

        Ok(Some(cas))
    }

    /// Generate a hash for securing a chunk range.
    ///
    /// chunk_start_index, chunk_end_index: indices for chunks in CasObject.
    /// The indices should be [start, end) - meaning start is inclusive and end is exclusive.
    /// Ex. For specifying the 1st chunk: chunk_start_index: 0, chunk_end_index: 1
    ///
    /// key: additional key incorporated into generating hash.
    ///
    /// This hash ensures validity of the knowledge of chunks, since ranges are public,
    /// this ensures that only users that actually have access to chunks can claim them
    /// in a file reconstruction entry.
    pub fn generate_chunk_range_hash(
        &self,
        chunk_start_index: u32,
        chunk_end_index: u32,
    ) -> Result<DataHash, CasObjectError> {
        self.validate_cas_object_info()?;

        if chunk_end_index <= chunk_start_index || chunk_end_index > self.info.num_chunks {
            return Err(CasObjectError::InvalidArguments);
        }

        // Collect relevant hashes
        let range_hashes = self.info.chunk_hashes[chunk_start_index as usize..chunk_end_index as usize].as_ref();

        Ok(range_hash_from_chunks(range_hashes))
    }

    /// Return end offset of all physical chunk contents (byte index at the beginning of footer)
    pub fn get_contents_length(&self) -> Result<u32, CasObjectError> {
        self.validate_cas_object_info()?;
        match self.info.chunk_boundary_offsets.last() {
            Some(c) => Ok(*c),
            None => Err(CasObjectError::FormatError(anyhow!("Cannot retrieve content length"))),
        }
    }

    /// Get range of content bytes uncompressed from Xorb.
    ///
    /// start and end are byte indices into the physical layout of a xorb.
    ///
    /// The start and end parameters are required to align with chunk boundaries.
    fn get_range<R: Read + Seek>(
        &self,
        reader: &mut R,
        byte_start: u32,
        byte_end: u32,
    ) -> Result<Vec<u8>, CasObjectError> {
        if byte_end < byte_start {
            return Err(CasObjectError::InvalidRange);
        }

        self.validate_cas_object_info()?;

        // make sure the end of the range is within the bounds of the xorb
        let end = min(byte_end, self.get_contents_length()?);

        // read chunk bytes
        let mut chunk_data = vec![0u8; (end - byte_start) as usize];
        reader.seek(SeekFrom::Start(byte_start as u64))?;
        reader.read_exact(&mut chunk_data)?;

        // build up result vector by processing these chunks
        let chunk_contents = self.get_chunk_contents(&chunk_data)?;
        Ok(chunk_contents)
    }

    /// Get all the content bytes from a Xorb
    pub fn get_all_bytes<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<u8>, CasObjectError> {
        self.validate_cas_object_info()?;
        self.get_range(reader, 0, self.get_contents_length()?)
    }

    /// Convenient function to get content bytes by chunk range, mainly for internal testing
    pub fn get_bytes_by_chunk_range<R: Read + Seek>(
        &self,
        reader: &mut R,
        chunk_index_start: u32,
        chunk_index_end: u32,
    ) -> Result<Vec<u8>, CasObjectError> {
        let (byte_start, byte_end) = self.get_byte_offset(chunk_index_start, chunk_index_end)?;

        self.get_range(reader, byte_start, byte_end)
    }

    /// Assumes chunk_data is 1+ complete chunks. Processes them sequentially and returns them as Vec<u8>.
    fn get_chunk_contents(&self, chunk_data: &[u8]) -> Result<Vec<u8>, CasObjectError> {
        // walk chunk_data, deserialize into Chunks, and then get_bytes() from each of them.
        let mut reader = Cursor::new(chunk_data);
        let mut res = Vec::<u8>::new();

        while reader.has_remaining() {
            let (data, _, _) = deserialize_chunk(&mut reader)?;
            res.extend_from_slice(&data);
        }
        Ok(res)
    }

    /// Helper function to translate a range of chunk indices to physical byte offset range.
    pub fn get_byte_offset(&self, chunk_index_start: u32, chunk_index_end: u32) -> Result<(u32, u32), CasObjectError> {
        self.validate_cas_object_info()?;
        if chunk_index_end <= chunk_index_start || chunk_index_end > self.info.num_chunks {
            return Err(CasObjectError::InvalidArguments);
        }

        let byte_offset_start = match chunk_index_start {
            0 => 0,
            _ => self.info.chunk_boundary_offsets[chunk_index_start as usize - 1],
        };
        let byte_offset_end = self.info.chunk_boundary_offsets[chunk_index_end as usize - 1];

        Ok((byte_offset_start, byte_offset_end))
    }

    /// given a valid chunk_index, returns the uncompressed chunk length for the chunk
    /// at the given index, chunk_index must be less than the number of chunks in the xorb
    pub fn uncompressed_chunk_length(&self, chunk_index: u32) -> Result<u32, CasObjectError> {
        self.validate_cas_object_info()?;
        let chunk_index = chunk_index as usize;
        if chunk_index >= self.info.unpacked_chunk_offsets.len() {
            return Err(CasObjectError::InvalidArguments);
        }
        let cumulative_sum = self.info.unpacked_chunk_offsets[chunk_index];
        let before = match chunk_index {
            0 => 0,
            _ => self.info.unpacked_chunk_offsets[chunk_index - 1],
        };
        Ok(cumulative_sum - before)
    }

    /// given a valid start and end, returns the uncompressed end-exclusive range length
    /// meaning the length of the chunk range [chunk_index_start, chunk_index_end)
    /// the following conditions must be valid:
    ///     chunk_index_start <= chunk_index_end &&
    ///     chunk_index_end <= num_chunks &&
    ///     chunk_index_start < num_chunks
    pub fn uncompressed_range_length(
        &self,
        chunk_index_start: u32,
        chunk_index_end: u32,
    ) -> Result<u32, CasObjectError> {
        self.validate_cas_object_info()?;
        if chunk_index_start > chunk_index_end
            || chunk_index_end > self.info.num_chunks
            || chunk_index_start >= self.info.num_chunks
        {
            return Err(CasObjectError::InvalidArguments);
        }

        // this check is important if chunk_index_end is 0
        if chunk_index_start == chunk_index_end {
            return Ok(0);
        }

        let before_start = match chunk_index_start {
            0 => 0,
            _ => self.info.unpacked_chunk_offsets[chunk_index_start as usize - 1],
        };
        let incl_end = self.info.unpacked_chunk_offsets[chunk_index_end as usize - 1];
        Ok(incl_end - before_start)
    }

    /// Helper method to verify that info object is complete
    fn validate_cas_object_info(&self) -> Result<(), CasObjectError> {
        if self.info.num_chunks == 0 {
            return Err(CasObjectError::FormatError(anyhow!("Invalid CasObjectInfo, no chunks in CasObject.")));
        }

        if self.info.num_chunks != self.info.chunk_boundary_offsets.len() as u32
            || self.info.num_chunks != self.info.chunk_hashes.len() as u32
            || (self.info.boundaries_version == CAS_OBJECT_FORMAT_BOUNDARIES_VERSION
                && self.info.num_chunks != self.info.unpacked_chunk_offsets.len() as u32)
        {
            return Err(CasObjectError::FormatError(anyhow!(
                "Invalid CasObjectInfo, num chunks not matching boundaries or hashes."
            )));
        }

        if self.info.cashash == MerkleHash::default() {
            return Err(CasObjectError::FormatError(anyhow!("Invalid CasObjectInfo, Missing cashash.")));
        }

        Ok(())
    }
}

/// A convenience struct that holds the raw information for the upload path.
pub struct SerializedCasObject {
    /// The serialized data and header; can be uploaded as a blob.
    pub serialized_data: Vec<u8>,

    /// The xorb hash
    pub hash: MerkleHash,

    /// Number of bytes before compression; to be used for progress reporting.  
    pub raw_num_bytes: u64,

    /// For logging
    pub num_chunks: usize,

    pub footer_start: Option<u64>,
}

impl SerializedCasObject {
    /// Builds the xorb from raw xorb data.
    pub fn from_xorb(
        xorb: RawXorbData,
        compression_scheme: Option<CompressionScheme>,
        serialize_footer: bool,
    ) -> Result<Self, CasObjectError> {
        let mut cas_object_info = CasObjectInfoV1::default();

        let hash = xorb.hash();
        cas_object_info.cashash = hash;
        let raw_num_bytes = xorb.num_bytes() as u64;
        let num_chunks = xorb.data.len();

        let chunks_and_boundaries = xorb.cas_info.chunks_and_boundaries();

        cas_object_info.num_chunks = chunks_and_boundaries.len() as u32;
        cas_object_info.chunk_boundary_offsets = Vec::with_capacity(cas_object_info.num_chunks as usize);
        cas_object_info.chunk_hashes = chunks_and_boundaries.iter().map(|(hash, _)| *hash).collect();
        cas_object_info.unpacked_chunk_offsets = chunks_and_boundaries
            .iter()
            .map(|(_, unpacked_chunk_boundary)| *unpacked_chunk_boundary)
            .collect();

        // Make an upper estimate of the size to account for to avoid copies and allocations. We assume compression
        // doesn't help at all, and add just a bit of buffer on it (lz4 with no compression adds just a few
        // bytes).
        let size_upper_bound = xorb.num_bytes()
            + size_of::<CasObjectInfoV1>()
            + (32 + 2 * size_of::<u32>() + size_of::<MerkleHash>() + size_of::<CASChunkHeader>()) * xorb.data.len();

        let mut serialized_data = Vec::with_capacity(size_upper_bound);

        // Set the periodic retesting interval
        let retest_interval = if *CAS_OBJECT_COMPRESSION_SCHEME_RETEST_INTERVAL > 0 {
            *CAS_OBJECT_COMPRESSION_SCHEME_RETEST_INTERVAL
        } else {
            num_chunks
        };

        if compression_scheme.is_none() && num_chunks != 0 {
            debug_assert!(xorb.file_boundaries.is_sorted());
            debug_assert_ge!(xorb.file_boundaries.len(), 0);
            debug_assert_lt!(*xorb.file_boundaries.last().unwrap(), xorb.data.len());

            for (f_idx, &start_idx) in xorb.file_boundaries.iter().enumerate() {
                let end_idx = *xorb.file_boundaries.get(f_idx + 1).unwrap_or(&num_chunks);

                let mut s_idx = start_idx;
                while s_idx < end_idx {
                    let n_idx = (s_idx + retest_interval).min(end_idx);

                    // Choose the compression scheme for this block.
                    let compression_scheme = CompressionScheme::choose_from_data(&xorb.data[s_idx]);

                    for chunk in &xorb.data[s_idx..n_idx] {
                        // now serialize chunk directly to writer (since chunks come first!)
                        serialize_chunk(chunk, &mut serialized_data, Some(compression_scheme))?;
                        cas_object_info.chunk_boundary_offsets.push(serialized_data.len() as u32);
                    }
                    s_idx = n_idx;
                }
            }
        } else {
            for chunk in xorb.data {
                // now serialize chunk directly to writer (since chunks come first!)
                serialize_chunk(&chunk, &mut serialized_data, compression_scheme)?;
                cas_object_info.chunk_boundary_offsets.push(serialized_data.len() as u32);
            }
        }

        // Fill in the boundary offsets
        cas_object_info.fill_in_boundary_offsets();

        let mut footer_start = None;
        if serialize_footer {
            // Serialize the CasObject footer
            footer_start = Some(serialized_data.len() as u64);
            CasObject::serialize_given_info(&mut serialized_data, cas_object_info)?;
        }

        Ok(Self {
            serialized_data,
            hash,
            raw_num_bytes,
            num_chunks,
            footer_start,
        })
    }
}

pub mod test_utils {
    use merkledb::prelude::MerkleDBHighLevelMethodsV1;
    use merkledb::{ChunkInfo, MerkleMemDB};
    use rand::Rng;

    use super::*;
    use crate::cas_chunk_format::serialize_chunk;

    /// The reference version of the code serializing components of a raw cas object to
    /// a stream.  Replaced by SerializedCasObject::from_xorb; used here for correctness testing.
    /// returns the serialized CasObject, the number of bytes written, and the footer start offset.
    pub fn serialize_xorb_to_stream_reference<W: Write + Seek>(
        writer: &mut W,
        hash: &MerkleHash,
        data: &[u8],
        chunk_and_boundaries: &[(MerkleHash, u32)],
        compression_scheme: Option<CompressionScheme>,
    ) -> Result<(CasObject, usize, u64), CasObjectError> {
        let mut cas = CasObject::default();
        cas.info.cashash = *hash;
        cas.info.num_chunks = chunk_and_boundaries.len() as u32;
        cas.info.chunk_boundary_offsets = Vec::with_capacity(cas.info.num_chunks as usize);
        cas.info.chunk_hashes = chunk_and_boundaries.iter().map(|(hash, _)| *hash).collect();
        cas.info.unpacked_chunk_offsets = chunk_and_boundaries
            .iter()
            .map(|(_, unpacked_chunk_boundary)| *unpacked_chunk_boundary)
            .collect();

        let mut total_written_bytes: usize = 0;

        let mut raw_start_idx = 0;
        for boundary in chunk_and_boundaries {
            let chunk_boundary: u32 = boundary.1;

            let chunk_raw_bytes = &data[raw_start_idx as usize..chunk_boundary as usize];

            // now serialize chunk directly to writer (since chunks come first!)
            let chunk_written_bytes = serialize_chunk(chunk_raw_bytes, writer, compression_scheme)?;
            total_written_bytes += chunk_written_bytes;
            cas.info.chunk_boundary_offsets.push(total_written_bytes as u32);

            // update indexes and onto next chunk
            raw_start_idx = chunk_boundary;
        }

        cas.info.fill_in_boundary_offsets();

        let footer_start = writer.stream_position()?;

        // now that footer is ready, write out to writer.
        let info_length = cas.info.serialize(writer)?;
        cas.info_length = info_length as u32;
        total_written_bytes += info_length;

        writer.write_all(&cas.info_length.to_le_bytes())?;
        total_written_bytes += size_of::<u32>();

        Ok((cas, total_written_bytes, footer_start))
    }

    /// Serialize into Cas Object from uncompressed data and chunk boundaries.
    /// Assumes correctness from caller: it's the receiver's responsibility to validate a cas object.
    pub fn serialized_cas_object_from_components(
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
        compression: Option<CompressionScheme>,
    ) -> Result<SerializedCasObject, CasObjectError> {
        let mut writer = Cursor::new(Vec::new());

        let (_, _, footer_start) =
            serialize_xorb_to_stream_reference(&mut writer, hash, &data, &chunk_and_boundaries, compression)?;

        Ok(SerializedCasObject {
            serialized_data: writer.into_inner(),
            hash: *hash,
            raw_num_bytes: data.len() as u64,
            num_chunks: chunk_and_boundaries.len(),
            footer_start: Some(footer_start),
        })
    }

    pub fn verify_serialized_cas_object(
        xorb: &RawXorbData,
        compression_scheme: Option<CompressionScheme>,
        cas_object: &SerializedCasObject,
    ) {
        let xorb_hash = xorb.hash();
        let num_chunks = xorb.data.len();

        let chunks_and_boundaries = xorb.cas_info.chunks_and_boundaries();

        // For testing, check the current serialization method against the older version, byte for byte.
        let mut verification_data = Vec::new();

        // This is the older path;
        let xorb_data = raw_xorb_to_vec(xorb);

        let mut writer = Cursor::new(&mut verification_data);

        let (c, nbytes_trans, footer_start) = serialize_xorb_to_stream_reference(
            &mut writer,
            &xorb_hash,
            &xorb_data,
            &chunks_and_boundaries,
            compression_scheme,
        )
        .unwrap();

        assert_eq!(
            footer_start,
            cas_object.serialized_data.len() as u64 - size_of::<u32>() as u64 - c.info_length as u64
        );

        assert_eq!(verification_data.len(), nbytes_trans);

        assert_eq!(cas_object.serialized_data, verification_data);

        assert_eq!(cas_object.raw_num_bytes, xorb.num_bytes() as u64);

        assert_eq!(cas_object.num_chunks, num_chunks);
    }

    pub fn gen_random_bytes(size: u32) -> Vec<u8> {
        let mut rng = rand::rng();
        let mut data = vec![0u8; size as usize];
        rng.fill(&mut data[..]);
        data
    }

    #[derive(Debug, Clone, Copy)]
    pub enum ChunkSize {
        Random(u32, u32),
        Fixed(u32),
    }

    impl std::fmt::Display for ChunkSize {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ChunkSize::Random(a, b) => write!(f, "[{a}, {b}]"),
                ChunkSize::Fixed(a) => write!(f, "{a}"),
            }
        }
    }

    pub fn build_raw_xorb(num_chunks: u32, chunk_size: ChunkSize) -> RawXorbData {
        let mut chunks: Vec<deduplication::Chunk> = Vec::with_capacity(num_chunks as usize);

        for _idx in 0..num_chunks {
            let chunk_size: u32 = match chunk_size {
                ChunkSize::Random(a, b) => {
                    let mut rng = rand::rng();
                    rng.random_range(a..=b)
                },
                ChunkSize::Fixed(size) => size,
            };
            let bytes = gen_random_bytes(chunk_size);

            let chunk_hash = merklehash::compute_data_hash(&bytes);

            chunks.push(deduplication::Chunk {
                hash: chunk_hash,
                data: bytes.into(),
            });
        }

        RawXorbData::from_chunks(&chunks, vec![0])

        // Now, validate that this is actually the same
    }

    pub fn build_and_verify_cas_object(
        xorb: RawXorbData,
        compression_scheme: Option<CompressionScheme>,
    ) -> SerializedCasObject {
        let cas_object = SerializedCasObject::from_xorb(xorb.clone(), compression_scheme, true).unwrap();

        verify_serialized_cas_object(&xorb, compression_scheme, &cas_object);

        cas_object
    }

    /// Utility test method for creating a cas object
    /// Returns (CasObject, chunks serialized, raw data, raw data chunk hash and boundaries)
    #[allow(clippy::type_complexity)]
    pub fn build_cas_object(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
    ) -> (CasObject, Vec<u8>, Vec<u8>, Vec<(MerkleHash, u32)>) {
        let mut c = CasObject::default();

        let mut chunk_hashes = vec![];
        let mut chunk_boundary_offsets = vec![];
        let mut writer = Cursor::new(vec![]);

        let mut total_bytes = 0;
        let mut chunks = vec![];
        let mut data_contents_raw = vec![];
        let mut raw_chunk_boundaries = vec![];

        for _idx in 0..num_chunks {
            let chunk_size: u32 = match chunk_size {
                ChunkSize::Random(a, b) => {
                    let mut rng = rand::rng();
                    rng.random_range(a..=b)
                },
                ChunkSize::Fixed(size) => size,
            };

            let bytes = gen_random_bytes(chunk_size);

            let chunk_hash = merklehash::compute_data_hash(&bytes);
            chunks.push(ChunkInfo {
                hash: chunk_hash,
                length: bytes.len(),
            });

            data_contents_raw.extend_from_slice(&bytes);

            // build chunk, create ChunkInfo and keep going

            let bytes_written = serialize_chunk(&bytes, &mut writer, Some(compression_scheme)).unwrap();

            total_bytes += bytes_written as u32;

            raw_chunk_boundaries.push((chunk_hash, data_contents_raw.len() as u32));
            chunk_hashes.push(chunk_hash);
            chunk_boundary_offsets.push(total_bytes);
        }

        c.info.num_chunks = chunk_boundary_offsets.len() as u32;
        c.info.chunk_boundary_offsets = chunk_boundary_offsets;
        c.info.unpacked_chunk_offsets = raw_chunk_boundaries
            .iter()
            .map(|(_chunk_hash, unpacked_chunk_boundary)| *unpacked_chunk_boundary)
            .collect();
        c.info.chunk_hashes = chunk_hashes;

        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &chunks);
        let ret = db.finalize(staging);

        c.info.cashash = *ret.hash();

        c.info.fill_in_boundary_offsets();

        c.info_length = c.info.serialized_length() as u32;

        (c, writer.get_ref().to_vec(), data_contents_raw, raw_chunk_boundaries)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use futures::TryStreamExt;
    use mdb_shard::chunk_verification::VERIFICATION_KEY;

    use super::test_utils::*;
    use super::*;

    #[test]
    fn test_default_cas_object() {
        let cas = CasObject::default();

        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let len = cas.info.serialize(&mut writer).unwrap();

        assert_eq!(cas.info_length, len as u32);
        assert_eq!(cas.info_length, CAS_OBJECT_INFO_DEFAULT_LENGTH);
    }

    #[test]
    fn test_uncompressed_cas_object() {
        // Arrange
        let (c, _cas_data, _raw_data, _raw_chunk_boundaries) =
            build_cas_object(3, ChunkSize::Fixed(100), CompressionScheme::None);
        // Act & Assert
        assert_eq!(c.info.num_chunks, 3);
        assert_eq!(c.info.chunk_boundary_offsets.len(), c.info.num_chunks as usize);

        let second_chunk_boundary = *c.info.chunk_boundary_offsets.get(1).unwrap();
        assert_eq!(second_chunk_boundary, 216); // 8-byte header, 3 chunks, so 2nd chunk boundary is at byte 216

        let third_chunk_boundary = *c.info.chunk_boundary_offsets.get(2).unwrap();
        assert_eq!(third_chunk_boundary, 324); // 8-byte header, 3 chunks, so 3rd chunk boundary is at byte 324

        let byte_offset_range = c.get_byte_offset(0, 1).unwrap();
        assert_eq!(byte_offset_range, (0, 108));

        let byte_offset_range = c.get_byte_offset(1, 3).unwrap();
        assert_eq!(byte_offset_range, (108, 324));
    }

    #[test]
    fn test_generate_range_hash_full_range() {
        // Arrange
        let (c, _cas_data, _raw_data, _raw_chunk_boundaries) =
            build_cas_object(3, ChunkSize::Fixed(100), CompressionScheme::None);

        let hashes: Vec<u8> = c.info.chunk_hashes.iter().flat_map(|hash| hash.as_bytes().to_vec()).collect();

        let expected_hash = blake3::keyed_hash(&VERIFICATION_KEY, hashes.as_slice());

        // Act & Assert
        let range_hash = c.generate_chunk_range_hash(0, 3).unwrap();
        assert_eq!(range_hash, DataHash::from(expected_hash.as_bytes()));
    }

    #[test]
    fn test_generate_range_hash_partial() {
        // Arrange
        let (c, _cas_data, _raw_data, _raw_chunk_boundaries) =
            build_cas_object(5, ChunkSize::Fixed(100), CompressionScheme::None);

        let hashes: Vec<u8> = c.info.chunk_hashes.as_slice()[1..=3]
            .to_vec()
            .iter()
            .flat_map(|hash| hash.as_bytes().to_vec())
            .collect();
        let expected_hash = blake3::keyed_hash(&VERIFICATION_KEY, hashes.as_slice());

        // Act & Assert
        let range_hash = c.generate_chunk_range_hash(1, 4).unwrap();
        assert_eq!(range_hash, DataHash::from(expected_hash.as_bytes()));

        let hashes: Vec<u8> = c.info.chunk_hashes.as_slice()[0..1]
            .to_vec()
            .iter()
            .flat_map(|hash| hash.as_bytes().to_vec())
            .collect();
        let expected_hash = blake3::keyed_hash(&VERIFICATION_KEY, hashes.as_slice());

        let range_hash = c.generate_chunk_range_hash(0, 1).unwrap();
        assert_eq!(range_hash, DataHash::from(expected_hash.as_bytes()));
    }

    #[test]
    fn test_generate_range_hash_invalid_range() {
        // Arrange
        let (c, _cas_data, _raw_data, _raw_chunk_boundaries) =
            build_cas_object(5, ChunkSize::Fixed(100), CompressionScheme::None);

        // Act & Assert
        assert_eq!(c.generate_chunk_range_hash(1, 6), Err(CasObjectError::InvalidArguments));
        assert_eq!(c.generate_chunk_range_hash(100, 10), Err(CasObjectError::InvalidArguments));
        assert_eq!(c.generate_chunk_range_hash(0, 0), Err(CasObjectError::InvalidArguments));
    }

    #[test]
    fn test_validate_cas_object_info() {
        // Arrange & Act & Assert
        let (c, _cas_data, _raw_data, _raw_chunk_boundaries) =
            build_cas_object(5, ChunkSize::Fixed(100), CompressionScheme::None);
        let result = c.validate_cas_object_info();
        assert!(result.is_ok());

        // no chunks
        let c = CasObject::default();
        let result = c.validate_cas_object_info();
        assert_eq!(result, Err(CasObjectError::FormatError(anyhow!("Invalid CasObjectInfo, no chunks in CasObject."))));

        // num_chunks doesn't match chunk_boundaries.len()
        let mut c = CasObject::default();
        c.info.num_chunks = 1;
        let result = c.validate_cas_object_info();
        assert_eq!(
            result,
            Err(CasObjectError::FormatError(anyhow!(
                "Invalid CasObjectInfo, num chunks not matching boundaries or hashes."
            )))
        );

        // no hash
        let (mut c, _cas_data, _raw_data, _raw_chunk_boundaries) =
            build_cas_object(1, ChunkSize::Fixed(100), CompressionScheme::None);
        c.info.cashash = MerkleHash::default();
        let result = c.validate_cas_object_info();
        assert_eq!(result, Err(CasObjectError::FormatError(anyhow!("Invalid CasObjectInfo, Missing cashash."))));
    }

    #[test]
    fn test_compress_decompress() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(55, ChunkSize::Fixed(53212), CompressionScheme::LZ4);

        // Act & Assert
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        assert!(serialize_xorb_to_stream_reference(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        let c = res.unwrap();

        let c_bytes = c.get_all_bytes(&mut reader).unwrap();

        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        assert!(serialize_xorb_to_stream_reference(
            &mut writer,
            &c.info.cashash,
            &c_bytes,
            &raw_chunk_boundaries,
            Some(CompressionScheme::None)
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        let c2 = res.unwrap();

        assert_eq!(c2.info.cashash, c.info.cashash);
        assert_eq!(c.get_all_bytes(&mut writer), c.get_all_bytes(&mut reader));
        assert!(CasObject::validate_cas_object(&mut reader, &c2.info.cashash).unwrap().is_some());
        assert!(CasObject::validate_cas_object(&mut writer, &c.info.cashash).unwrap().is_some());
    }

    #[test]
    fn test_hash_generation_compression() {
        // Arrange
        let (c, cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(55, ChunkSize::Fixed(53212), CompressionScheme::LZ4);
        // Act & Assert
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        let serialized_all_bytes = c.get_all_bytes(&mut buf).unwrap();

        assert_eq!(raw_data, serialized_all_bytes);
        assert_eq!(cas_data.len() as u32, c.get_contents_length().unwrap());
    }

    #[test]
    fn test_basic_serialization_mem() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(3, ChunkSize::Fixed(100), CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::None)
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap().is_some());
    }

    #[test]
    fn test_serialization_deserialization_mem_medium() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(32, ChunkSize::Fixed(16384), CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::None)
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap().is_some());

        let mut reader = buf.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        let bytes_read = c2.get_all_bytes(&mut reader).unwrap();
        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
        assert_eq!(raw_data, bytes_read);
    }

    #[test]
    fn test_serialization_deserialization_mem_large_random() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(32, ChunkSize::Random(512, 65536), CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::None)
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap().is_some());

        let mut reader = buf.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
        assert_eq!(raw_data, c2.get_all_bytes(&mut reader).unwrap());
    }

    #[test]
    fn test_serialization_deserialization_file_large_random() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(256, ChunkSize::Random(512, 65536), CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::None)
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap().is_some());

        let mut reader = buf.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
        assert_eq!(raw_data, c2.get_all_bytes(&mut reader).unwrap());
    }

    #[test]
    fn test_basic_mem_lz4() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(1, ChunkSize::Fixed(8), CompressionScheme::LZ4);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        let bytes_read = c2.get_all_bytes(&mut reader).unwrap();
        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
        assert_eq!(raw_data, bytes_read);
    }

    #[test]
    fn test_serialization_deserialization_mem_medium_lz4() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(32, ChunkSize::Fixed(16384), CompressionScheme::LZ4);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap().is_some());

        let mut reader = buf.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        let bytes_read = c2.get_all_bytes(&mut reader).unwrap();
        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
        assert_eq!(raw_data, bytes_read);
    }

    #[test]
    fn test_serialization_deserialization_mem_large_random_lz4() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(32, ChunkSize::Random(512, 65536), CompressionScheme::LZ4);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap().is_some());

        let mut reader = buf.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
        assert_eq!(raw_data, c2.get_all_bytes(&mut reader).unwrap());
    }

    #[test]
    fn test_serialization_deserialization_file_large_random_lz4() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(256, ChunkSize::Random(512, 65536), CompressionScheme::LZ4);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
        assert_eq!(raw_data, c2.get_all_bytes(&mut reader).unwrap());
    }

    #[tokio::test]
    async fn test_serialization_async_deserialization() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(64, ChunkSize::Random(512, 2048), CompressionScheme::LZ4);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        let xorb_bytes = buf.into_inner();
        // length - 4 byte for the info_length - info_length + ident + version (already read ident + version)
        let start_pos = xorb_bytes.len() - size_of::<u32>() - c.info_length as usize
            + size_of::<CasObjectIdent>()
            + size_of::<u8>();

        let chunks = xorb_bytes[start_pos..].chunks(10).map(Ok).collect::<Vec<_>>();
        let mut xorb_footer_async_reader = futures::stream::iter(chunks).into_async_read();
        let cas_object_result =
            CasObject::deserialize_async(&mut xorb_footer_async_reader, CAS_OBJECT_FORMAT_VERSION).await;
        assert!(cas_object_result.is_ok(), "{cas_object_result:?}");
        let cas_object = cas_object_result.unwrap();
        assert_eq!(c, cas_object);
    }

    #[test]
    fn test_jump_pointer_in_metadata() {
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(4, ChunkSize::Random(512, 2048), CompressionScheme::LZ4);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4)
        )
        .is_ok());

        let xorb_bytes = buf.into_inner();

        // Retrieve the jump pointers.
        const JUMP_POINTER_BUFFER_AND_INFO_LENGTH_SIZE: usize =
            size_of::<u32>() + size_of::<u32>() + size_of::<[u8; 16]>() + size_of::<u32>();

        let jump_pointer_buffer_and_info_length_bytes =
            &xorb_bytes[xorb_bytes.len() - JUMP_POINTER_BUFFER_AND_INFO_LENGTH_SIZE..];
        let mut reader = Cursor::new(jump_pointer_buffer_and_info_length_bytes);
        let hash_section_offset_from_info_end = read_u32(&mut reader).unwrap();
        let boundary_section_offset_from_info_end = read_u32(&mut reader).unwrap();

        // Now verify the hashes section
        let hash_section =
            &xorb_bytes[xorb_bytes.len() - size_of::<u32>() - hash_section_offset_from_info_end as usize..];
        let mut reader = Cursor::new(hash_section);

        let mut ident_hash_section = [0u8; 7];
        read_bytes(&mut reader, &mut ident_hash_section).unwrap();
        assert_eq!(ident_hash_section, c.info.ident_hash_section);
        let hashes_version = read_u8(&mut reader).unwrap();
        assert_eq!(hashes_version, c.info.hashes_version);
        let num_chunks = read_u32(&mut reader).unwrap();
        let mut chunk_hashes = vec![];
        for _ in 0..num_chunks {
            chunk_hashes.push(read_hash(&mut reader).unwrap());
        }
        assert_eq!(chunk_hashes, c.info.chunk_hashes);

        // Now verify the boundaries section
        let boundary_section =
            &xorb_bytes[xorb_bytes.len() - size_of::<u32>() - boundary_section_offset_from_info_end as usize..];
        let mut reader = Cursor::new(boundary_section);

        let mut ident_boundary_section = [0u8; 7];
        read_bytes(&mut reader, &mut ident_boundary_section).unwrap();
        assert_eq!(ident_boundary_section, c.info.ident_boundary_section);
        let boundaries_version = read_u8(&mut reader).unwrap();
        assert_eq!(boundaries_version, c.info.boundaries_version);
        let num_chunks = read_u32(&mut reader).unwrap();
        let mut chunk_boundary_offsets = vec![0u32; num_chunks as usize];
        read_u32s(&mut reader, &mut chunk_boundary_offsets).unwrap();
        assert_eq!(chunk_boundary_offsets, c.info.chunk_boundary_offsets);
        let mut unpacked_chunk_offsets = vec![0u32; num_chunks as usize];
        read_u32s(&mut reader, &mut unpacked_chunk_offsets).unwrap();
        assert_eq!(unpacked_chunk_offsets, c.info.unpacked_chunk_offsets);
    }

    #[test]
    fn test_deserialize_from_v0_xorb() {
        // build a v1 xorb
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(4, ChunkSize::Random(512, 2048), CompressionScheme::LZ4);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        assert!(serialize_xorb_to_stream_reference(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(CompressionScheme::LZ4),
        )
        .is_ok());

        // Switch V1 footer to V0
        let mut cas_info_v0 = CasObjectInfoV0::default();
        cas_info_v0.cashash = c.info.cashash;
        cas_info_v0.num_chunks = c.info.num_chunks;
        cas_info_v0.chunk_boundary_offsets = c.info.chunk_boundary_offsets.clone();
        cas_info_v0.chunk_hashes = c.info.chunk_hashes.clone();

        let mut buf = buf.into_inner();
        let serialized_chunks_length = c.get_contents_length().unwrap();
        buf.resize(serialized_chunks_length as usize, 0);

        let mut buf = Cursor::new(buf);
        buf.seek(SeekFrom::End(0)).unwrap();
        #[allow(deprecated)]
        let info_length = cas_info_v0.serialize(&mut buf).unwrap() as u32;
        write_u32(&mut buf, info_length).unwrap();

        let xorb_bytes = buf.into_inner();
        let mut reader = Cursor::new(xorb_bytes);
        let ret = CasObject::deserialize(&mut reader).unwrap();
        assert_eq!(ret.info.cashash, cas_info_v0.cashash);
        assert_eq!(ret.info.num_chunks, cas_info_v0.num_chunks);
        assert_eq!(ret.info.chunk_boundary_offsets, cas_info_v0.chunk_boundary_offsets);
        assert_eq!(ret.info.chunk_hashes, cas_info_v0.chunk_hashes);
        assert_eq!(ret.info._buffer, cas_info_v0._buffer);
    }

    #[test]
    fn test_uncompressed_chunk_length() {
        const NUM_CHUNKS: u32 = 8;
        let (c, _, _, _) = build_cas_object(NUM_CHUNKS, ChunkSize::Random(512, 2048), CompressionScheme::LZ4);

        let mut cumulative_sum = 0;
        for i in 0..NUM_CHUNKS {
            let chunk_length_result = c.uncompressed_chunk_length(i);
            assert!(chunk_length_result.is_ok());
            let chunk_length = chunk_length_result.unwrap();
            assert_eq!(chunk_length, c.info.unpacked_chunk_offsets[i as usize] - cumulative_sum);
            cumulative_sum += chunk_length;
        }
    }

    #[test]
    fn test_uncompressed_chunk_length_invalid() {
        const NUM_CHUNKS: u32 = 8;
        let (c, _, _, _) = build_cas_object(NUM_CHUNKS, ChunkSize::Random(512, 2048), CompressionScheme::LZ4);

        assert!(c.uncompressed_chunk_length(NUM_CHUNKS).is_err());
        assert!(c.uncompressed_chunk_length(NUM_CHUNKS + 1).is_err());
    }

    #[test]
    fn test_uncompressed_range_length() {
        const NUM_CHUNKS: u32 = 4;
        const CHUNK_SIZE: u32 = 512;
        let (c, _, _, _) = build_cas_object(NUM_CHUNKS, ChunkSize::Fixed(CHUNK_SIZE), CompressionScheme::LZ4);

        for start in 0..(NUM_CHUNKS - 1) {
            for end in start..NUM_CHUNKS {
                let uncompressed_range_length_result = c.uncompressed_range_length(start, end);
                assert!(uncompressed_range_length_result.is_ok());
                let length = uncompressed_range_length_result.unwrap();
                assert_eq!(length, CHUNK_SIZE * (end - start));
            }
        }
    }

    #[test]
    fn test_uncompressed_range_length_invalid() {
        const NUM_CHUNKS: u32 = 4;
        const CHUNK_SIZE: u32 = 512;
        let (c, _, _, _) = build_cas_object(NUM_CHUNKS, ChunkSize::Fixed(CHUNK_SIZE), CompressionScheme::LZ4);

        assert!(c.uncompressed_range_length(1, 0).is_err());
        assert!(c.uncompressed_range_length(0, NUM_CHUNKS + 1).is_err());
        assert!(c.uncompressed_range_length(NUM_CHUNKS, NUM_CHUNKS + 1).is_err());
        assert!(c.uncompressed_range_length(NUM_CHUNKS + 2, NUM_CHUNKS + 1).is_err());
    }

    #[test]
    fn test_deserialize_only_boundaries_section() {
        const CHUNK_SIZE: u32 = 100;
        const COMPRESSION_SCHEME: CompressionScheme = CompressionScheme::None;

        for num_chunks in [1, 10, 100, 1000] {
            let (c, _, raw_data, raw_chunk_boundaries) =
                build_cas_object(num_chunks, ChunkSize::Fixed(CHUNK_SIZE), COMPRESSION_SCHEME);

            // Act & Assert
            let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
            assert!(serialize_xorb_to_stream_reference(
                &mut writer,
                &c.info.cashash,
                &raw_data,
                &raw_chunk_boundaries,
                Some(COMPRESSION_SCHEME)
            )
            .is_ok());
            let original = c.info;

            writer.seek(SeekFrom::Start(0)).unwrap();

            let result = CasObjectInfoV1::deserialize_only_boundaries_section(&mut writer);
            assert!(result.is_ok());
            let (boundaries_footer, _num_read) = result.unwrap();

            assert_eq!(boundaries_footer.version, original.version);
            assert_eq!(boundaries_footer.ident_boundary_section, original.ident_boundary_section);
            assert_eq!(boundaries_footer.boundaries_version, original.boundaries_version);
            assert_eq!(boundaries_footer.num_chunks, num_chunks);
            assert_eq!(boundaries_footer.num_chunks, original.num_chunks);
            assert_eq!(boundaries_footer.boundary_section_offset_from_end, original.boundary_section_offset_from_end);
            assert_eq!(boundaries_footer.hashes_section_offset_from_end, original.hashes_section_offset_from_end);
            assert_eq!(boundaries_footer.chunk_boundary_offsets, original.chunk_boundary_offsets);
            assert_eq!(boundaries_footer.unpacked_chunk_offsets, original.unpacked_chunk_offsets);

            // check that the hashes are not filled in
            assert!(!boundaries_footer.has_chunk_hashes());
            assert!(original.has_chunk_hashes());
        }
    }

    #[test]
    fn test_serialized_length() {
        const CHUNK_SIZE: u32 = 4096;
        const COMPRESSION_SCHEME: CompressionScheme = CompressionScheme::LZ4;

        for num_chunks in [1, 10, 100, 1000] {
            let (c, _, _, _) = build_cas_object(num_chunks, ChunkSize::Fixed(CHUNK_SIZE), COMPRESSION_SCHEME);

            // serialize the info, match that returned length matches the serialized_length() function
            // output and the actual result length of the serialized data

            let mut buf = Vec::new();
            let len = c.info.serialize(&mut buf).unwrap();
            assert_eq!(len, c.info.serialized_length());
            // c.info_length should have been set to c.info.serialized_length() as u32, this is tautology
            assert_eq!(len, c.info_length as usize);
            assert_eq!(len, buf.len())
        }
    }
}
