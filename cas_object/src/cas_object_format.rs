use bytes::Buf;
use merklehash::{DataHash, MerkleHash};
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
    pub cashash: DataHash,

    /// Total number of chunks in the file. Length of chunk_size_info.
    pub num_chunks: u32,

    /// Chunk metadata (start of chunk, length of chunk), length of vector matches num_chunks.
    /// This vector is expected to be in order (ex. `chunk[0].start_byte_index == 0`).
    /// If uncompressed chunk, then: `chunk[n].start_byte_index == chunk[n-1].uncompressed_cumulative_len`.
    /// And the final entry in this vector is a dummy entry to know the final chunk ending byte range.
    ///
    /// ```
    /// // ex.       chunks:  [ 0 - 99 | 100 - 199 | 200 - 299 ]
    /// // chunk_size_info : < (0,100), (100, 200), (200, 300), (300, 300) > <-- notice extra entry.
    /// ```
    pub chunk_size_info: Vec<CasChunkInfo>,

    /// Unused 16-byte buffer to allow for future extensibility.
    _buffer: [u8; 16],
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CasChunkInfo {
    /// Starting index of chunk.
    ///
    /// Ex. `chunk[5]` would start at start_byte_index
    /// from the beginning of the XORB.
    ///
    /// This does include chunk header, to allow for fast range lookups.
    pub start_byte_index: u32,

    /// Cumulative length of chunk.
    ///
    /// Does not include chunk header length, only uncompressed contents.
    pub cumulative_uncompressed_len: u32,
}

impl Default for CasObjectInfo {
    fn default() -> Self {
        CasObjectInfo {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            cashash: DataHash::default(),
            num_chunks: 0,
            chunk_size_info: Vec::new(),
            _buffer: Default::default(),
        }
    }
}

impl CasObjectInfo {
    /// Serialize CasObjectMetadata to provided Writer.
    ///
    /// Assumes caller has set position of Writer to appropriate location for metadata serialization.
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

        // write variable field: chunk_size_metadata
        for chunk in &self.chunk_size_info {
            let chunk_bytes = chunk.as_bytes();
            write_bytes(&chunk_bytes)?;
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

        let mut chunk_size_info = Vec::with_capacity(num_chunks as usize);
        for _ in 0..num_chunks {
            let mut buf = [0u8; size_of::<CasChunkInfo>()];
            read_bytes(&mut buf)?;
            chunk_size_info.push(CasChunkInfo::from_bytes(buf)?);
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
                chunk_size_info,
                _buffer,
            },
            info_length,
        ))
    }
}

impl CasChunkInfo {
    pub fn as_bytes(&self) -> [u8; size_of::<Self>()] {
        let mut serialized_bytes = [0u8; size_of::<Self>()]; // 8 bytes, 2 u32
        serialized_bytes[..4].copy_from_slice(&self.start_byte_index.to_le_bytes());
        serialized_bytes[4..].copy_from_slice(&self.cumulative_uncompressed_len.to_le_bytes());
        serialized_bytes
    }

    pub fn from_bytes(buf: [u8; 8]) -> Result<Self, CasObjectError> {
        Ok(Self {
            start_byte_index: u32::from_le_bytes(buf[..4].try_into().unwrap()),
            cumulative_uncompressed_len: u32::from_le_bytes(buf[4..].try_into().unwrap()),
        })
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

/// Helper struct to capture 3-part tuple needed to
/// correctly support range reads across compressed chunks in a Xorb.
///
/// See docs for [CasObject::get_range_boundaries] for example usage.
pub struct RangeBoundaryHelper {
    /// Index for range start in compressed chunks.
    /// Guaranteed to be start of a [CASChunkHeader].
    pub compressed_range_start: u32,

    /// Index for range end in compressed chunk.
    /// Guaranteed to be end of chunk.
    pub compressed_range_end: u32,

    /// Offset into uncompressed chunk. This is necessary for
    /// range requests that do not align with chunk boundary.
    pub uncompressed_offset: u32,
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

    /// Deserialize the header only.
    ///
    /// This allows the CasObject to be partially constructed, allowing for range reads inside the CasObject.
    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CasObjectError> {
        let (info, info_length) = CasObjectInfo::deserialize(reader)?;
        Ok(Self { info, info_length })
    }

    /// Translate desired range into actual byte range from within Xorb.
    ///
    /// This function will return a [RangeBoundaryHelper] struct to be able to read
    /// a range from the Xorb. This function translates uncompressed ranges into their corresponding
    /// Xorb chunk start byte index and Xorb chunk end byte index, along with an offset into that chunk.
    /// See example below.
    ///
    /// Ex. If user requests range bytes 150-250 from a Xorb, and assume the following layout:
    /// ```
    /// //               chunk: [   0  |    1    |    2    |    3    ]
    /// // uncompressed chunks: [ 0-99 | 100-199 | 200-299 | 300-399 ]
    /// //   compressed chunks: [ 0-49 |   50-99 | 100-149 | 150-199 ]
    /// ```
    /// This function needs to return starting index for chunk 1, with an offset of 50 bytes, and the end
    /// index of chunk 2 in order to satisfy the range 150-250.
    /// ```
    /// // let ranges = cas.get_range_boundaries(150, 250)?;
    /// // ranges.compressed_range_start = 50
    /// // ranges.compressed_range_end = 150
    /// // ranges.uncompressed_offset = 50
    /// ```
    /// See [CasObject::get_range] for how these ranges are used.
    pub fn get_range_boundaries(
        &self,
        start: u32,
        end: u32,
    ) -> Result<RangeBoundaryHelper, CasObjectError> {
        if end < start {
            return Err(CasObjectError::InvalidArguments);
        }

        if end > self.get_contents_length()? {
            return Err(CasObjectError::InvalidArguments);
        }

        let chunk_size_info = &self.info.chunk_size_info;

        let mut compressed_range_start = u32::MAX;
        let mut compressed_range_end = u32::MAX;
        let mut uncompressed_offset = u32::MAX;

        // Enumerate all the chunks in order in the Xorb, but ignore the final one since that is a dummy chunk used to
        // get the final byte index of the final content chunk. This allows the (idx + 1) to always be correct.
        for (idx, c) in chunk_size_info[..chunk_size_info.len() - 1]
            .iter()
            .enumerate()
        {
            // Starting chunk is identified, store the start_byte_index of this chunk.
            // compute the offset into the chunk if necessary by subtracting start range from end of
            // previous chunk len (idx - 1).
            if c.cumulative_uncompressed_len >= start && compressed_range_start == u32::MAX {
                compressed_range_start = c.start_byte_index;
                uncompressed_offset = if idx == 0 {
                    start
                } else {
                    start
                        - chunk_size_info
                            .get(idx - 1)
                            .unwrap()
                            .cumulative_uncompressed_len
                }
            }

            // Once we find the 1st chunk (in-order) that meets the range query, we find the start_byte_index
            // of the next chunk and capture that as compressed_range_end. This uses the dummy chunk entry
            // to get the end of the final content chunk.
            if c.cumulative_uncompressed_len >= end && compressed_range_end == u32::MAX {
                compressed_range_end = chunk_size_info.get(idx + 1).unwrap().start_byte_index;
                break;
            }
        }

        Ok(RangeBoundaryHelper {
            compressed_range_start,
            compressed_range_end,
            uncompressed_offset,
        })
    }

    /// Return end value of all chunk contents (byte index prior to header)
    pub fn get_contents_length(&self) -> Result<u32, CasObjectError> {
        match self.info.chunk_size_info.last() {
            Some(c) => Ok(c.cumulative_uncompressed_len),
            None => Err(CasObjectError::FormatError(anyhow!(
                "Cannot retrieve content length"
            ))),
        }
    }

    /// Get range of content bytes from Xorb
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

        // create return data bytes
        // let mut data = vec![0u8; (end - start) as usize];

        // translate range into chunk bytes to read from xorb directly
        let boundary = self.get_range_boundaries(start, end)?;
        let chunk_start = boundary.compressed_range_start;
        let chunk_end = boundary.compressed_range_end;
        let offset = boundary.uncompressed_offset as usize;

        // read chunk bytes
        let mut chunk_data = vec![0u8; (chunk_end - chunk_start) as usize];
        reader.seek(std::io::SeekFrom::Start(chunk_start as u64))?;
        reader.read_exact(&mut chunk_data)?;

        // build up result vector by processing these chunks
        let chunk_contents = self.get_chunk_contents(&chunk_data)?;
        let len = (end - start) as usize;

        Ok(chunk_contents[offset..offset + len].to_vec())
    }

    /// Assumes chunk_data is 1+ complete chunks. Processes them sequentially and returns them as Vec<u8>.
    fn get_chunk_contents(&self, chunk_data: &[u8]) -> Result<Vec<u8>, CasObjectError> {
        // walk chunk_data, deserialize into Chunks, and then get_bytes() from each of them.
        let mut reader = Cursor::new(chunk_data);
        let mut res = Vec::<u8>::new();

        while reader.has_remaining() {
            let data = deserialize_chunk(&mut reader)?;
            res.extend_from_slice(&data);
        }
        Ok(res)
    }

    /// Get all the content bytes from a Xorb
    pub fn get_all_bytes<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<u8>, CasObjectError> {
        if self.info == Default::default() {
            return Err(CasObjectError::InternalError(anyhow!(
                "Incomplete CasObject, no header"
            )));
        }

        self.get_range(reader, 0, self.get_contents_length()?)
    }

    /// Helper function to translate CasObjectInfo.chunk_size_info to just return chunk_boundaries.
    ///
    /// This isolates the weirdness about iterating through chunk_size_info and ignoring the final dummy entry.
    fn get_chunk_boundaries(&self) -> Vec<u32> {
        self.info.chunk_size_info.clone()[..self.info.chunk_size_info.len() - 1]
            .iter()
            .map(|c| c.cumulative_uncompressed_len)
            .collect()
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
        let mut cas = CasObject::default();
        cas.info.cashash.copy_from_slice(hash.as_slice());
        cas.info.num_chunks = chunk_boundaries.len() as u32 + 1; // extra entry for dummy, see [chunk_size_info] for details.
        cas.info.chunk_size_info = Vec::with_capacity(cas.info.num_chunks as usize);

        let mut total_written_bytes: usize = 0;

        let mut raw_start_idx = 0;
        let mut start_idx: u32 = 0;
        let mut cumulative_chunk_length: u32 = 0;
        for boundary in chunk_boundaries {
            let chunk_boundary: u32 = *boundary;

            let mut chunk_raw_bytes = Vec::<u8>::new();
            chunk_raw_bytes
                .extend_from_slice(&data[raw_start_idx as usize..chunk_boundary as usize]);
            let chunk_size = chunk_boundary - raw_start_idx;

            // now serialize chunk directly to writer (since chunks come first!)
            // TODO: add compression scheme to this call
            let chunk_written_bytes =
                serialize_chunk(&chunk_raw_bytes, writer, compression_scheme)?;
            total_written_bytes += chunk_written_bytes;

            let chunk_meta = CasChunkInfo {
                start_byte_index: start_idx,
                cumulative_uncompressed_len: cumulative_chunk_length + chunk_size,
            };
            cas.info.chunk_size_info.push(chunk_meta);

            start_idx += chunk_written_bytes as u32;
            raw_start_idx = chunk_boundary;
            cumulative_chunk_length += chunk_size;
        }

        // dummy chunk_info to help with range reads. See [chunk_size_info] for details.
        let chunk_meta = CasChunkInfo {
            start_byte_index: start_idx,
            cumulative_uncompressed_len: cumulative_chunk_length,
        };
        cas.info.chunk_size_info.push(chunk_meta);

        // now that header is ready, write out to writer.
        let info_length = cas.info.serialize(writer)?;
        cas.info_length = info_length as u32;
        total_written_bytes += info_length;

        writer.write_all(&cas.info_length.to_le_bytes())?;
        total_written_bytes += size_of::<u32>();

        Ok((cas, total_written_bytes))
    }
}

#[cfg(test)]
mod tests {

    use crate::cas_chunk_format::serialize_chunk;

    use super::*;
    use merklehash::compute_data_hash;
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
            chunk_size_info: Vec::new(),
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
        let (c, _cas_data, _raw_data) = build_cas_object(3, 100, false, false);
        // Act & Assert
        assert_eq!(c.get_chunk_boundaries().len(), 3);
        assert_eq!(c.get_chunk_boundaries(), [100, 200, 300]);
        assert_eq!(c.info.num_chunks, 4);
        assert_eq!(c.info.chunk_size_info.len(), c.info.num_chunks as usize);

        let last_chunk_info = c.info.chunk_size_info[2].clone();
        let dummy_chunk_info = c.info.chunk_size_info[3].clone();
        assert_eq!(dummy_chunk_info.cumulative_uncompressed_len, 300);
        assert_eq!(dummy_chunk_info.start_byte_index, 324); // 8-byte header, 3 chunks, so 4th chunk should start at byte 324
        assert_eq!(last_chunk_info.cumulative_uncompressed_len, 300);
    }

    fn gen_random_bytes(uncompressed_chunk_size: u32) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; uncompressed_chunk_size as usize];
        rng.fill(&mut data[..]);
        data
    }

    fn build_cas_object(
        num_chunks: u32,
        uncompressed_chunk_size: u32,
        use_random_chunk_size: bool,
        use_lz4_compression: bool
    ) -> (CasObject, Vec<u8>, Vec<u8>) {
        let mut c = CasObject::default();

        let mut chunk_size_info = Vec::<CasChunkInfo>::new();
        let mut writer = Cursor::new(Vec::<u8>::new());

        let mut total_bytes = 0;
        let mut uncompressed_bytes: u32 = 0;

        let mut data_contents_raw =
            Vec::<u8>::with_capacity(num_chunks as usize * uncompressed_chunk_size as usize);

        for _idx in 0..num_chunks {
            let chunk_size: u32 = if use_random_chunk_size {
                let mut rng = rand::thread_rng();
                rng.gen_range(512..=uncompressed_chunk_size)
            } else {
                uncompressed_chunk_size
            };

            let bytes = gen_random_bytes(chunk_size);
            let len: u32 = bytes.len() as u32;

            data_contents_raw.extend_from_slice(&bytes);

            // build chunk, create ChunkInfo and keep going

            let compression_scheme = match use_lz4_compression {
                true => CompressionScheme::LZ4,
                false => CompressionScheme::None
            };

            let bytes_written = serialize_chunk(
                &bytes,
                &mut writer,
                compression_scheme,
            )
            .unwrap();

            let chunk_info = CasChunkInfo {
                start_byte_index: total_bytes,
                cumulative_uncompressed_len: uncompressed_bytes + len,
            };

            chunk_size_info.push(chunk_info);
            total_bytes += bytes_written as u32;
            uncompressed_bytes += len;
        }

        let chunk_info = CasChunkInfo {
            start_byte_index: total_bytes,
            cumulative_uncompressed_len: uncompressed_bytes,
        };
        chunk_size_info.push(chunk_info);

        c.info.num_chunks = chunk_size_info.len() as u32;

        c.info.cashash = compute_data_hash(&writer.get_ref());
        c.info.chunk_size_info = chunk_size_info;

        // now serialize info to end Xorb length
        let len = c.info.serialize(&mut writer).unwrap();
        c.info_length = len as u32;

        writer.write_all(&c.info_length.to_le_bytes()).unwrap();

        (c, writer.get_ref().to_vec(), data_contents_raw)
    }

    #[test]
    fn test_basic_serialization_mem() {
        // Arrange
        let (c, _cas_data, raw_data) = build_cas_object(3, 100, false, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
            CompressionScheme::None
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        let c2 = res.unwrap();
        assert_eq!(c, c2);
        assert_eq!(c.info.cashash, c2.info.cashash);
        assert_eq!(c.info.num_chunks, c2.info.num_chunks);
    }

    #[test]
    fn test_serialization_deserialization_mem_medium() {
        // Arrange
        let (c, _cas_data, raw_data) = build_cas_object(32, 16384, false, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
            CompressionScheme::None
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
    fn test_serialization_deserialization_mem_large_random() {
        // Arrange
        let (c, _cas_data, raw_data) = build_cas_object(32, 65536, true, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
            CompressionScheme::None
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

    #[test]
    fn test_serialization_deserialization_file_large_random() {
        // Arrange
        let (c, _cas_data, raw_data) = build_cas_object(256, 65536, true, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
            CompressionScheme::None
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

    #[test]
    fn test_basic_mem_lz4() {
        // Arrange
        let (c, _cas_data, raw_data) = build_cas_object(1, 8, false, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
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
        let (c, _cas_data, raw_data) = build_cas_object(32, 16384, false, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
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
    fn test_serialization_deserialization_mem_large_random_lz4() {
        // Arrange
        let (c, _cas_data, raw_data) = build_cas_object(32, 65536, true, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
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

    #[test]
    fn test_serialization_deserialization_file_large_random_lz4() {
        // Arrange
        let (c, _cas_data, raw_data) = build_cas_object(256, 65536, true, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.info.cashash,
            &raw_data,
            &c.get_chunk_boundaries(),
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
