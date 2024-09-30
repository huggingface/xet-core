use bytes::Buf;
use merkledb::{prelude::MerkleDBHighLevelMethodsV1, Chunk, MerkleMemDB};
use merklehash::{DataHash, MerkleHash};
use tracing::warn;
use std::{
    cmp::min,
    io::{Cursor, Error, Read, Seek, Write},
    mem::size_of,
};

use crate::{
    cas_chunk_format::{deserialize_chunk, serialize_chunk}, error::CasObjectError, CompressionScheme
};
use anyhow::anyhow;

const CAS_OBJECT_FORMAT_IDENT: [u8; 7] = [b'X', b'E', b'T', b'B', b'L', b'O', b'B'];
const CAS_OBJECT_FORMAT_VERSION: u8 = 0;
const CAS_OBJECT_INFO_DEFAULT_LENGTH: u32 = 60;

#[derive(Clone, PartialEq, Eq, Debug)]
/// Info struct for [CasObject]. This is stored at the end of the XORB
///
/// See details here: https://www.notion.so/huggingface2/Introduction-To-XetHub-Storage-Architecture-And-The-Integration-Path-54c3d14c682c4e41beab2364f273fc35?pvs=4#4ffa9b930a6942bd87f054714865375d
pub struct CasObjectInfo {
    /// CAS identifier: "XETBLOB"
    pub ident: [u8; 7],

    /// Format version, expected to be 0 right now.
    pub version: u8,

    /// 256-bits, 16-bytes, The CAS Hash of this Xorb.
    pub cashash: MerkleHash,

    /// Total number of chunks in the Xorb. Length of chunk_byte_offset & chunk_hashes vectors.
    pub num_chunks: u32,

    /// Byte offset marking the boundary of each chunk. Length of vector is num_chunks.
    /// 
    /// This vector only contains boundaries, so assumes the first chunk starts at offset 0.
    /// The final entry in vector is the total length of the chunks.
    /// See example below.
    /// chunk[n] offset = chunk_boundary_offsets[n-1]
    /// ```
    /// // ex.             chunks: [  0,   1,   2,   3 ]
    /// // chunk_boundary_offsets: [ 100, 200, 300, 400]
    /// ```
    pub chunk_boundary_offsets: Vec<u32>,

    /// Merklehash for each chunk stored in the Xorb. Length of vector is num_chunks.
    pub chunk_hashes: Vec<MerkleHash>,

    /// Unused 16-byte buffer to allow for future extensibility.
    _buffer: [u8; 16],
}

impl Default for CasObjectInfo {
    fn default() -> Self {
        CasObjectInfo {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            cashash: DataHash::default(),
            num_chunks: 0,
            chunk_boundary_offsets: Vec::new(),
            chunk_hashes: Vec::new(),
            _buffer: Default::default(),
        }
    }
}

impl CasObjectInfo {
    /// Serialize CasObjectInfo to provided Writer.
    ///
    /// Assumes caller has set position of Writer to appropriate location for serialization.
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, CasObjectError> {
        let mut total_bytes_written = 0;

        // Helper function to write data and update the byte count
        let mut write_bytes = |data: &[u8]| -> Result<(), Error> {
            writer.write_all(data)?;
            total_bytes_written += data.len();
            Ok(())
        };

        // Write fixed-size fields, in order: ident, version, cashash, num_chunks
        write_bytes(&self.ident)?;
        write_bytes(&[self.version])?;
        write_bytes(self.cashash.as_bytes())?;
        write_bytes(&self.num_chunks.to_le_bytes())?;

        // write variable field: chunk offsets & hashes
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

    /// Construct CasObjectInfo object from Reader + Seek.
    ///
    /// Expects metadata struct is found at end of Reader, written out in struct order.
    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<(Self, u32), CasObjectError> {
        let mut total_bytes_read: u32 = 0;

        // Go to end of Reader and get length, then jump back to it, and read sequentially
        // read last 4 bytes to get length
        reader.seek(std::io::SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut info_length = [0u8; 4];
        reader.read_exact(&mut info_length)?;
        let info_length = u32::from_le_bytes(info_length);

        // now seek back that many bytes + size of length (u32) and read sequentially.
        reader.seek(std::io::SeekFrom::End(
            -(size_of::<u32>() as i64 + info_length as i64),
        ))?;

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

        if version[0] != CAS_OBJECT_FORMAT_VERSION {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Invalid Format Version"
            )));
        }

        let mut buf = [0u8; 32];
        read_bytes(&mut buf)?;
        let cashash = DataHash::from(&buf);

        let mut num_chunks = [0u8; 4];
        read_bytes(&mut num_chunks)?;
        let num_chunks = u32::from_le_bytes(num_chunks);

        let mut chunk_boundary_offsets = Vec::with_capacity(num_chunks as usize);
        for _ in 0..num_chunks {
            let mut offset = [0u8; 4];
            read_bytes(&mut offset)?;
            chunk_boundary_offsets.push(u32::from_le_bytes(offset));
        }
        let mut chunk_hashes = Vec::with_capacity(num_chunks as usize); // dummy chunk
        for _ in 0..num_chunks {
            let mut hash = [0u8; 32];
            read_bytes(&mut hash)?;
            chunk_hashes.push(DataHash::from(&hash));
        }

        let mut _buffer = [0u8; 16];
        read_bytes(&mut _buffer)?;

        // validate that info_length matches what we read off of header
        if total_bytes_read != info_length {
            return Err(CasObjectError::FormatError(anyhow!(
                "Xorb Info Format Error"
            )));
        }

        Ok((
            CasObjectInfo {
                ident,
                version: version[0],
                cashash,
                num_chunks,
                chunk_boundary_offsets,
                chunk_hashes,
                _buffer,
            },
            info_length,
        ))
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
/// XORB: 16MB data block for storing chunks.
///
/// Has header, and a set of functions that interact directly with XORB.
pub struct CasObject {
    /// CasObjectInfo block see [CasObjectInfo] for details.
    pub info: CasObjectInfo,

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
        reader.seek(std::io::SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut info_length = [0u8; 4];
        reader.read_exact(&mut info_length)?;
        let info_length = u32::from_le_bytes(info_length);
        Ok(info_length)
    }

    /// Deserialize the CasObjectInfo struct, the metadata for this Xorb.
    ///
    /// This allows the CasObject to be partially constructed, allowing for range reads inside the CasObject.
    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CasObjectError> {
        let (info, info_length) = CasObjectInfo::deserialize(reader)?;
        Ok(Self { info, info_length })
    }
    
    /// Return end value of all chunk contents (byte index prior to header)
    pub fn get_contents_length(&self) -> Result<u32, CasObjectError> {
        match self.info.chunk_boundary_offsets.last() {
            Some(c) => Ok(*c),
            None => Err(CasObjectError::FormatError(anyhow!(
                "Cannot retrieve content length"
            ))),
        }
    }

    /// Get range of content bytes from Xorb.
    /// 
    /// The start and end parameters are required to align with chunk boundaries.
    pub fn get_range<R: Read + Seek>(
        &self,
        reader: &mut R,
        start: u32,
        end: u32,
    ) -> Result<Vec<u8>, CasObjectError> {
        if end < start {
            return Err(CasObjectError::InvalidRange);
        }

        // make sure the end of the range is within the bounds of the xorb
        let end = min(end, self.get_contents_length()?);

        // read chunk bytes
        let mut chunk_data = vec![0u8; (end - start) as usize];
        reader.seek(std::io::SeekFrom::Start(start as u64))?;
        reader.read_exact(&mut chunk_data)?;

        // build up result vector by processing these chunks
        let chunk_contents = self.get_chunk_contents(&chunk_data)?;
        Ok(chunk_contents)
    }

    /// Assumes chunk_data is 1+ complete chunks. Processes them sequentially and returns them as Vec<u8>.
    fn get_chunk_contents(&self, chunk_data: &[u8]) -> Result<Vec<u8>, CasObjectError> {
        // walk chunk_data, deserialize into Chunks, and then get_bytes() from each of them.
        let mut reader = Cursor::new(chunk_data);
        let mut res = Vec::<u8>::new();

        while reader.has_remaining() {
            let (data, _) = deserialize_chunk(&mut reader)?;
            res.extend_from_slice(&data);
        }
        Ok(res)
    }

    /// Get all the content bytes from a Xorb
    pub fn get_all_bytes<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<u8>, CasObjectError> {
        if self.info == Default::default() {
            return Err(CasObjectError::InternalError(anyhow!(
                "Incomplete CasObject, no CasObjectInfo footer."
            )));
        }

        self.get_range(reader, 0, self.get_contents_length()?)
    }

    /// Helper function to translate CasObjectInfo.chunk_byte_offsets to just return chunk boundaries.
    ///
    /// The final chunk boundary returned is required to be the length of the contents, which is recorded in the dummy chunk.
    fn get_chunk_boundaries(&self) -> Vec<u32> {
        self.info.chunk_boundary_offsets.to_vec()
    }

    /// Get all the content bytes from a Xorb, and return the chunk boundaries
    pub fn get_detailed_bytes<R: Read + Seek>(
        &self,
        reader: &mut R,
    ) -> Result<(Vec<u32>, Vec<u8>), CasObjectError> {
        if self.info == Default::default() {
            return Err(CasObjectError::InternalError(anyhow!(
                "Incomplete CasObject, no header"
            )));
        }

        let data = self.get_all_bytes(reader)?;
        let chunk_boundaries = self.get_chunk_boundaries();

        Ok((chunk_boundaries, data))
    }

    /// Used by LocalClient for generating Cas Object from chunk_boundaries while uploading or downloading blocks.
    pub fn serialize<W: Write + Seek>(
        writer: &mut W,
        hash: &MerkleHash,
        data: &[u8],
        chunk_boundaries: &Vec<u32>,
        compression_scheme: CompressionScheme,
    ) -> Result<(Self, usize), CasObjectError> {

        // validate hash against contents
        if !Self::validate_root_hash(data, chunk_boundaries, hash) {
            return Err(CasObjectError::HashMismatch);
        }

        let mut cas = CasObject::default();
        cas.info.cashash.copy_from_slice(hash.as_slice());
        cas.info.num_chunks = chunk_boundaries.len() as u32;
        cas.info.chunk_boundary_offsets = Vec::with_capacity(cas.info.num_chunks as usize);
        cas.info.chunk_hashes = Vec::with_capacity(cas.info.num_chunks as usize);

        let mut total_written_bytes: usize = 0;

        let mut raw_start_idx = 0;
        for boundary in chunk_boundaries {
            let chunk_boundary: u32 = *boundary;

            let mut chunk_raw_bytes = Vec::<u8>::new();
            chunk_raw_bytes
                .extend_from_slice(&data[raw_start_idx as usize..chunk_boundary as usize]);

            // generate chunk hash and store it
            let chunk_hash = merklehash::compute_data_hash(&chunk_raw_bytes);
            cas.info.chunk_hashes.push(chunk_hash);

            // now serialize chunk directly to writer (since chunks come first!)
            let chunk_written_bytes =
                serialize_chunk(&chunk_raw_bytes, writer, compression_scheme)?;
            total_written_bytes += chunk_written_bytes;
            cas.info.chunk_boundary_offsets.push(total_written_bytes as u32);

            // update indexes and onto next chunk
            raw_start_idx = chunk_boundary;
        }

        // now that header is ready, write out to writer.
        let info_length = cas.info.serialize(writer)?;
        cas.info_length = info_length as u32;
        total_written_bytes += info_length;

        writer.write_all(&cas.info_length.to_le_bytes())?;
        total_written_bytes += size_of::<u32>();

        Ok((cas, total_written_bytes))
    }

    fn validate_root_hash(data: &[u8], chunk_boundaries: &[u32], hash: &MerkleHash) -> bool {
        // at least 1 chunk, and last entry in chunk boundary must match the length
        if chunk_boundaries.is_empty()
            || chunk_boundaries[chunk_boundaries.len() - 1] as usize != data.len()
        {
            return false;
        }

        let mut chunks: Vec<Chunk> = Vec::new();
        let mut left_edge: usize = 0;
        for i in chunk_boundaries {
            let right_edge = *i as usize;
            let hash = merklehash::compute_data_hash(&data[left_edge..right_edge]);
            let length = right_edge - left_edge;
            chunks.push(Chunk { hash, length });
            left_edge = right_edge;
        }

        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &chunks);
        let ret = db.finalize(staging);
        *ret.hash() == *hash
    }

    /// Validate CasObject.
    /// Verifies each chunk is valid and correctly represented in CasObjectInfo, along with 
    /// recomputing the hash and validating it matches CasObjectInfo.
    /// 
    /// Returns Ok(true) if recomputed hash matches what is passed in.
    pub fn validate_cas_object<R: Read + Seek>(reader: &mut R, hash: &MerkleHash) -> Result<bool, CasObjectError> {

        // 1. deserialize to get Info
        let cas = CasObject::deserialize(reader)?;

        // 2. walk chunks from Info
        let mut hash_chunks: Vec<Chunk> = Vec::new();
        let mut cumulative_compressed_length: u32 = 0;

        let mut start_offset = 0;
        // Validate each chunk: iterate chunks, deserialize chunk, compare stored hash with 
        // computed hash, store chunk hashes for cashash validation
        for idx in 0..cas.info.num_chunks {

            // deserialize each chunk 
            reader.seek(std::io::SeekFrom::Start(start_offset as u64))?;
            let (data, compressed_chunk_length) = deserialize_chunk(reader)?;
            let chunk_uncompressed_length = data.len();

            let chunk_hash = merklehash::compute_data_hash(&data);
            hash_chunks.push(Chunk {hash: chunk_hash, length: chunk_uncompressed_length});
            
            cumulative_compressed_length += compressed_chunk_length as u32;
            
            // verify chunk hash
            if *cas.info.chunk_hashes.get(idx as usize).unwrap() != chunk_hash {
                warn!("XORB Validation: Chunk hash does not match Info object.");
                return Ok(false);
            }

            let boundary = *cas.info.chunk_boundary_offsets.get(idx as usize).unwrap();

            // verify that cas.chunk[n].len + 1 == cas.chunk_boundary_offsets[n]
            if (start_offset + compressed_chunk_length as u32) != boundary {
                warn!("XORB Validation: Chunk boundary byte index does not match Info object.");
                return Ok(false);
            }

            // set start offset of next chunk as the boundary of the current chunk
            start_offset = boundary;
        }
        
        // validate that Info/footer begins immediately after final content xorb.
        // end of for loop completes the content chunks, now should be able to deserialize an Info directly
        let cur_position = reader.stream_position()? as u32;
        let expected_position = cumulative_compressed_length;
        let expected_from_end_position = reader.seek(std::io::SeekFrom::End(0))? as u32 - cas.info_length - size_of::<u32>() as u32;
        if cur_position != expected_position || cur_position != expected_from_end_position {
            warn!("XORB Validation: Content bytes after known chunks in Info object.");
            return Ok(false);
        }

        // 4. combine hashes to get full xorb hash, compare to provided
        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &hash_chunks);
        let ret = db.finalize(staging);

        if *ret.hash() != *hash || *ret.hash() != cas.info.cashash {
            warn!("XORB Validation: Computed hash does not match provided hash or Info hash.");
            return Ok(false);
        }

        Ok(true)

    }

}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cas_chunk_format::serialize_chunk;
    use merkledb::{prelude::MerkleDBHighLevelMethodsV1, Chunk, MerkleMemDB};
    use rand::Rng;
    use std::io::Cursor;

    #[test]
    fn test_default_header_initialization() {
        // Create an instance using the Default trait
        let default_instance = CasObjectInfo::default();

        // Expected default values
        let expected_default = CasObjectInfo {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            cashash: DataHash::default(),
            num_chunks: 0,
            chunk_boundary_offsets: Vec::new(),
            chunk_hashes: Vec::new(),
            _buffer: [0; 16],
        };

        // Assert that the default instance matches the expected values
        assert_eq!(default_instance, expected_default);
    }

    #[test]
    fn test_default_cas_object() {
        let cas = CasObject::default();

        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let len = cas.info.serialize(&mut writer).unwrap();

        assert_eq!(cas.info_length, len as u32);
        assert_eq!(cas.info_length, CAS_OBJECT_INFO_DEFAULT_LENGTH);
    }

    #[test]
    fn test_chunk_boundaries_chunk_size_info() {
        // Arrange
        let (c, _cas_data, _raw_data, _raw_chunk_boundaries) = build_cas_object(3, 100, false, CompressionScheme::None);
        // Act & Assert
        assert_eq!(c.get_chunk_boundaries().len(), 3);
        assert_eq!(c.get_chunk_boundaries(), [108, 216, 324]);
        assert_eq!(c.info.num_chunks, 3);
        assert_eq!(c.info.chunk_boundary_offsets.len(), c.info.num_chunks as usize);

        let second_chunk_boundary = *c.info.chunk_boundary_offsets.get(1).unwrap();
        let third_chunk_boundary = *c.info.chunk_boundary_offsets.get(2).unwrap();
        assert_eq!(second_chunk_boundary, 216);  // 8-byte header, 3 chunks, so 2nd chunk boundary is at byte 216
        assert_eq!(third_chunk_boundary, 324); // 8-byte header, 3 chunks, so 3rd chunk boundary is at byte 324
    }

    fn gen_random_bytes(uncompressed_chunk_size: u32) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; uncompressed_chunk_size as usize];
        rng.fill(&mut data[..]);
        data
    }

    /// Utility test method for creating a cas object
    /// Returns (CasObject, CasObjectInfo serialized, raw data, raw data chunk boundaries)
    fn build_cas_object(
        num_chunks: u32,
        uncompressed_chunk_size: u32,
        use_random_chunk_size: bool,
        compression_scheme: CompressionScheme,
    ) -> (CasObject, Vec<u8>, Vec<u8>, Vec<u32>) {
        let mut c = CasObject::default();

        let mut chunk_boundary_offsets = Vec::<u32>::new();
        let mut chunk_hashes = Vec::<DataHash>::new();
        let mut writer = Cursor::new(Vec::<u8>::new());

        let mut total_bytes = 0;
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut data_contents_raw = Vec::<u8>::new();
        let mut raw_chunk_boundaries = Vec::<u32>::new();

        for _idx in 0..num_chunks {
            let chunk_size: u32 = if use_random_chunk_size {
                let mut rng = rand::thread_rng();
                rng.gen_range(512..=uncompressed_chunk_size)
            } else {
                uncompressed_chunk_size
            };

            let bytes = gen_random_bytes(chunk_size);

            let chunk_hash = merklehash::compute_data_hash(&bytes);
            chunks.push(Chunk { hash: chunk_hash, length: bytes.len() });

            data_contents_raw.extend_from_slice(&bytes);

            // build chunk, create ChunkInfo and keep going

            let bytes_written = serialize_chunk(
                &bytes,
                &mut writer,
                compression_scheme,
            )
            .unwrap();

            total_bytes += bytes_written as u32;

            raw_chunk_boundaries.push(data_contents_raw.len() as u32);
            chunk_boundary_offsets.push(total_bytes);
            chunk_hashes.push(chunk_hash);
        }

        c.info.num_chunks = chunk_boundary_offsets.len() as u32;
        c.info.chunk_boundary_offsets = chunk_boundary_offsets;
        c.info.chunk_hashes = chunk_hashes;

        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &chunks);
        let ret = db.finalize(staging);

        c.info.cashash = *ret.hash();

        // now serialize info to end Xorb length
        let mut buf = Cursor::new(Vec::new());
        let len = c.info.serialize(&mut buf).unwrap();
        c.info_length = len as u32;

        (c, writer.get_ref().to_vec(), data_contents_raw, raw_chunk_boundaries)
    }

    #[test]
    fn test_compress_decompress() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(55, 53212, false, CompressionScheme::LZ4); 

        // Act & Assert
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::LZ4
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        let c = res.unwrap();

        let c_bytes = c.get_all_bytes(&mut reader).unwrap();

        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &c_bytes,
            &raw_chunk_boundaries,
            CompressionScheme::None
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        let c2 = res.unwrap();

        assert_eq!(c2.info.cashash, c.info.cashash);
        assert_eq!(c.get_all_bytes(&mut writer), c.get_all_bytes(&mut reader));
        assert!(CasObject::validate_cas_object(&mut reader, &c2.info.cashash).is_ok());
        assert!(CasObject::validate_cas_object(&mut writer, &c.info.cashash).is_ok());
    }

    #[test]
    fn test_hash_generation_compression() {
        // Arrange
        let (c, cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(55, 53212, false, CompressionScheme::LZ4);
        // Act & Assert
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::LZ4
        )
        .is_ok());

        let serialized_all_bytes = c.get_all_bytes(&mut buf).unwrap();

        assert_eq!(raw_data, serialized_all_bytes);
        assert_eq!(cas_data.len() as u32, c.get_contents_length().unwrap());
    }

    #[test]
    fn test_basic_serialization_mem() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(3, 100, false, CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::None
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap());
    }

    #[test]
    fn test_serialization_deserialization_mem_medium() {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(32, 16384, false, CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::None
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap());

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
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(32, 65536, true, CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::None
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap());

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
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(256, 65536, true, CompressionScheme::None);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::None
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap());

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
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(1, 8, false, CompressionScheme::LZ4);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::LZ4
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
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(32, 16384, false, CompressionScheme::LZ4);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::LZ4
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap());

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
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(32, 65536, true, CompressionScheme::LZ4);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::LZ4
        )
        .is_ok());

        assert!(CasObject::validate_cas_object(&mut buf, &c.info.cashash).unwrap());

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
        let (c, _cas_data, raw_data, raw_chunk_boundaries) = build_cas_object(256, 65536, true, CompressionScheme::LZ4);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            CompressionScheme::LZ4
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
}
