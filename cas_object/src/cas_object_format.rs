use bincode::serialize;
use merklehash::{DataHash, MerkleHash};
use std::{
    cmp::min,
    io::{Error, Read, Seek, Write},
    mem::size_of,
};

use crate::error::CasObjectError;
use anyhow::anyhow;

const CAS_OBJECT_FORMAT_IDENT: [u8; 7] = [b'X', b'E', b'T', b'B', b'L', b'O', b'B'];
const CAS_OBJECT_FORMAT_VERSION: u8 = 0;
const CAS_OBJECT_HEADER_DEFAULT_LENGTH: u32 = 50;

// TODO: #[repr(C, packed)] - this requires alignment stuff to get corrected for Vec & DataHash.
#[derive(Clone, PartialEq, Eq, Debug)]
/// Header struct for [CasObject]
///
/// See details here: https://www.notion.so/huggingface2/Introduction-To-XetHub-Storage-Architecture-And-The-Integration-Path-54c3d14c682c4e41beab2364f273fc35?pvs=4#4ffa9b930a6942bd87f054714865375d
pub struct CasObjectMetadata {
    /// CAS identifier: "XETBLOB"
    pub ident: [u8; 7],

    /// Format version, expected to be 0 right now.
    pub version: u8,

    /// 256-bits, 16-bytes, The CAS Hash of this Xorb.
    pub cashash: DataHash,

    /// Total number of chunks in the file. Length of chunk_size_metadata.
    pub num_chunks: u32,

    /// Chunk metadata (start of chunk, length of chunk), length of vector matches num_chunks.
    pub chunk_size_metadata: Vec<ChunkMetadata>,

    /// Unused 16-byte buffer to allow for future extensibility.
    _buffer: [u8; 16],

    /// Length of entire metadata block.
    ///
    /// This is required to be at the end of the CasObject, so readers can read the
    /// final 4 bytes and know the full length of the metadata block.
    pub metadata_length: u32,
}

#[repr(C, packed)]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChunkMetadata {
    /// Starting index of chunk.
    ///
    /// Ex. chunk[5] would start at start_byte_index
    /// from the beginning of the XORB.
    ///
    /// This does include chunk header, to allow for fast range lookups.
    pub start_byte_index: u32,

    /// Cumulative length of chunk.
    ///
    /// Does not include chunk header length, only uncompressed contents.
    pub cumulative_uncompressed_len: u32,
}

impl Default for CasObjectMetadata {
    fn default() -> Self {
        CasObjectMetadata {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            cashash: DataHash::default(),
            num_chunks: 0,
            chunk_size_metadata: Vec::new(),
            _buffer: Default::default(),
            metadata_length: 0,
        }
    }
}

impl CasObjectMetadata {
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
        for chunk in &self.chunk_size_metadata {
            let chunk_bytes = chunk.as_bytes()?;
            write_bytes(&chunk_bytes)?;
        }

        // write closing metadata
        write_bytes(&self._buffer);
        write_bytes(&self.metadata_length.to_le_bytes());

        Ok(total_bytes_written)
    }

    /// Construct CasObjectMetadata object from Reader + Seek.
    ///
    /// Expects metadata struct is found at end of Reader, written out in struct order.
    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CasObjectError> {
        let mut total_bytes_read: usize = 0;

        // Go to end of Reader and get length, then jump back to it, and read sequentially
        // read last 4 bytes to get length
        reader.seek(std::io::SeekFrom::End(
            -1 * std::mem::size_of::<u32>() as i64,
        ))?;

        let mut metadata_length = [0u8; 4];
        reader.read_exact(&mut metadata_length)?;
        total_bytes_read += metadata_length.len();
        let metadata_length = u32::from_le_bytes(metadata_length);

        // now seek back that many bytes and read sequentially.
        reader.seek(std::io::SeekFrom::End(-1 * metadata_length as i64))?;

        // Helper function to read data and update the byte count
        let mut read_bytes = |data: &mut [u8]| -> Result<(), CasObjectError> {
            reader.read_exact(data)?;
            total_bytes_read += data.len();
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

        let mut chunk_size_metadata = Vec::with_capacity(num_chunks as usize);
        for _ in 0..num_chunks {
            let mut buf = [0u8; 8];
            read_bytes(&mut buf)?;
            chunk_size_metadata.push(ChunkMetadata::from_bytes(buf)?);
        }

        let mut _buffer = [0u8; 16];
        read_bytes(&mut _buffer)?;

        Ok(CasObjectMetadata {
            ident,
            version: version[0],
            cashash,
            num_chunks,
            chunk_size_metadata,
            _buffer,
            metadata_length,
        })
    }
}

impl ChunkMetadata {
    pub fn as_bytes(&self) -> Result<Vec<u8>, CasObjectError> {
        let mut serialized_bytes = Vec::with_capacity(2 * std::mem::size_of::<u32>()); // 8 bytes, 2 u32
        serialized_bytes.extend_from_slice(&self.start_byte_index.to_le_bytes());
        serialized_bytes.extend_from_slice(&self.cumulative_uncompressed_len.to_le_bytes());
        Ok(serialized_bytes)
    }

    pub fn from_bytes(buf: [u8; 8]) -> Result<Self, CasObjectError> {
        Ok(Self {
            start_byte_index: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            cumulative_uncompressed_len: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        })
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
/// XORB: 16MB data block for storing chunks.
///
/// Has header, and a set of functions that interact directly with XORB.
pub struct CasObject {
    /// Header CAS object, see CasObjectHeader struct.
    pub meta: CasObjectMetadata,
}

impl Default for CasObject {
    fn default() -> Self {
        Self {
            meta: Default::default(),
        }
    }
}

impl CasObject {
    /// Deserialize the header only.
    ///
    /// This allows the CasObject to be partially constructed, allowing for range reads inside the CasObject.
    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CasObjectError> {
        let meta = CasObjectMetadata::deserialize(reader)?;
        Ok(Self { meta })
    }

    /// Translate range into actual byte range from within Xorb.
    /// This function will return a tuple that is the start idx for a chunk and the end idx of a chunk,
    /// the assumption is the caller can fetch this byte range from Xorb directly and then use
    /// chunk operations to translate bytes into uncompressed bytes.
    pub fn get_range_boundaries(&self, start: u32, end: u32) -> Result<(u32, u32), CasObjectError> {
        if end < start {
            return Err(CasObjectError::InvalidArguments);
        }

        /*
        - cumulative_uncompressed_len does not include chunk header lengths
        - start_byte_index does include chunk header length.
        */

        let chunk_size_metadata = self.meta.chunk_size_metadata.as_slice();

        let start_i = chunk_size_metadata
            .binary_search_by_key(&start, |c| c.cumulative_uncompressed_len)
            .map_err(|_| CasObjectError::InvalidRange)?;

        let end_i = chunk_size_metadata
            .binary_search_by_key(&end, |c| c.cumulative_uncompressed_len)
            .map_err(|_| CasObjectError::InvalidRange)?;

        let cas_start_index_inclusive = chunk_size_metadata[start_i].start_byte_index;
        let cas_end_index_inclusive = chunk_size_metadata[end_i + 1].start_byte_index - 1;

        Ok((cas_start_index_inclusive, cas_end_index_inclusive))
    }

    /// Return end value of all chunk contents (byte index prior to header)
    pub fn get_contents_length(&self) -> Result<u32, CasObjectError> {
        match self.meta.chunk_size_metadata.last() {
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
        let (chunk_start, chunk_end) = self.get_range_boundaries(start, end)?;

        // read chunk bytes
        let mut chunk_data = vec![0u8; (chunk_end - chunk_start) as usize];
        reader.seek(std::io::SeekFrom::Start(chunk_start as u64))?;
        reader.read_exact(&mut chunk_data)?;

        // build up result vector by processing these chunks
        self.get_chunk_contents(&chunk_data)
    }

    /// Assumes chunk_data is 1+ complete chunks. Processes them sequentially and returns them as Vec<u8>.
    fn get_chunk_contents(&self, _chunk_data: &[u8]) -> Result<Vec<u8>, CasObjectError> {
        // TODO: walk chunk_data, deserialize into Chunks, and then get_bytes() from each of them.
        // RAJAT ADD IMPL HERE
        Ok(Vec::new())
    }

    /// Get all the content bytes from a Xorb
    pub fn get_all_bytes<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<u8>, CasObjectError> {
        // seek to header_length (from start).
        // if uncompressed, just read rest of uncompressed length and return.
        // if compressed, then walk compressed chunk vector and decompress and return.
        if self.meta == Default::default() {
            return Err(CasObjectError::InternalError(anyhow!(
                "Incomplete CasObject, no header"
            )));
        }

        self.get_range(reader, 0, self.get_contents_length()?)
    }

    /// Get all the content bytes from a Xorb, and return the chunk boundaries
    pub fn get_detailed_bytes<R: Read + Seek>(
        &self,
        reader: &mut R,
    ) -> Result<(Vec<u32>, Vec<u8>), CasObjectError> {
        // seek to header_length (from start).
        // if uncompressed, just read rest of uncompressed length and return.
        // if compressed, then walk compressed chunk vector and decompress and return.
        if self.meta == Default::default() {
            return Err(CasObjectError::InternalError(anyhow!(
                "Incomplete CasObject, no header"
            )));
        }

        let data = self.get_all_bytes(reader)?;
        let chunk_boundaries = self.meta.chunk_size_metadata.clone().into_iter().map(|c| c.cumulative_uncompressed_len).collect();

        Ok((chunk_boundaries, data))
    }

    /// Used by LocalClient for generating Cas Object from chunk_boundaries while uploading or downloading blocks.
    /// Can only generate Uncompressed CasObjects.
    pub fn serialize<W: Write + Seek>(
        writer: &mut W,
        hash: &MerkleHash,
        data: &[u8],
        chunk_boundaries: &Vec<u32>,
    ) -> Result<(Self, usize), CasObjectError> {
        let mut cas = CasObject::default();
        cas.meta.cashash.copy_from_slice(hash.as_slice());
        cas.meta.num_chunks = chunk_boundaries.len() as u32;
        cas.meta.chunk_size_metadata = Vec::with_capacity(chunk_boundaries.len());

        let mut total_written_bytes: usize = 0;
        let mut written_bytes = Vec::<u8>::new();

        let mut start_idx = 0;
        for boundary in chunk_boundaries {
            let chunk_boundary: u32 = *boundary;

            // for uncompressed just take data as is, for compressed compress chunk and then write those out.

            // TODO: add support for compression here

            written_bytes.extend_from_slice(&data[start_idx as usize..chunk_boundary as usize]);

            let chunk_size = chunk_boundary - start_idx;

            /*
            let chunk_meta = ChunkMetadata {}
            cas.meta.chunk_uncompressed_len.push(chunk_size);

            // TODO: always take chunk size because compressed chunks will reduce the bytes needed for this chunk
            cas.meta
                .chunk_compressed_cumulative
                .push(start_idx + chunk_size);

            start_idx = chunk_boundary;
            */
        }

        // now that header is ready, write out to writer, and then write the bytes.
        total_written_bytes += cas.meta.serialize(writer)?;

        // now write out the bytes
        writer.write_all(&written_bytes)?;
        total_written_bytes += written_bytes.len();

        Ok((cas, total_written_bytes))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use merklehash::compute_data_hash;
    use rand::Rng;
    use std::io::Cursor;

    /*
    #[test]
    fn test_default_header_initialization() {
        // Create an instance using the Default trait
        let default_instance = CasObjectMetadata::default();

        // Expected default values
        let expected_default = CasObjectMetadata {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            compression_method: CAS_OBJECT_COMPRESSION_UNCOMPRESSED,
            cashash: DataHash::default(),
            total_uncompressed_length: 0,
            num_chunks: 0,
            chunk_uncompressed_len: Vec::new(),
            chunk_compressed_cumulative: Vec::new(),
        };

        // Assert that the default instance matches the expected values
        assert_eq!(default_instance, expected_default);
    }

    #[test]
    fn test_default_cas_object() {
        let cas = CasObject::default();

        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let len = cas.meta.serialize(&mut writer).unwrap();

        assert_eq!(cas.header_length, len as u32);
        assert_eq!(cas.header_length, CAS_OBJECT_HEADER_DEFAULT_LENGTH);
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
    ) -> (CasObject, Vec<u8>) {
        let mut c = CasObject::default();
        let mut data = Vec::<u8>::new();

        c.meta.num_chunks = num_chunks;

        let mut total_bytes = 0;
        for _idx in 0..num_chunks {
            let chunk_size: u32 = if use_random_chunk_size {
                let mut rng = rand::thread_rng();
                rng.gen_range(1024..=uncompressed_chunk_size)
            } else {
                uncompressed_chunk_size
            };

            let bytes = gen_random_bytes(chunk_size);
            let len: u32 = bytes.len() as u32;
            c.meta.chunk_uncompressed_len.push(len);
            data.extend(bytes);
            total_bytes += len;
            c.meta.chunk_compressed_cumulative.push(total_bytes);
        }

        c.meta.cashash = compute_data_hash(&data);
        c.meta.total_uncompressed_length = total_bytes;

        // get header length
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let len = c.meta.serialize(&mut writer).unwrap();
        c.header_length = len as u32;

        (c, data)
    }

    #[test]
    fn test_basic_serialization_mem() {
        // Arrange
        let (c, data) = build_cas_object(3, 100, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.meta.cashash,
            &data,
            &c.meta.chunk_compressed_cumulative
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        let c2 = res.unwrap();
        assert_eq!(c, c2);
        assert_eq!(c.meta.cashash, c2.meta.cashash);
    }

    #[test]
    fn test_serialization_deserialization_mem_medium() {
        // Arrange
        let (c, data) = build_cas_object(32, 16384, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.meta.cashash,
            &data,
            &c.meta.chunk_compressed_cumulative
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        let bytes_read = c2.get_all_bytes(&mut reader).unwrap();
        assert_eq!(c.meta.num_chunks, c2.meta.num_chunks);
        assert_eq!(data, bytes_read);
    }

    #[test]
    fn test_serialization_deserialization_mem_large_random() {
        // Arrange
        let (c, data) = build_cas_object(32, 65536, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.meta.cashash,
            &data,
            &c.meta.chunk_compressed_cumulative
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        assert_eq!(c.meta.num_chunks, c2.meta.num_chunks);
        assert_eq!(data, c2.get_all_bytes(&mut reader).unwrap());
    }

    #[test]
    fn test_serialization_deserialization_file_large_random() {
        // Arrange
        let (c, data) = build_cas_object(256, 65536, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut writer,
            &c.meta.cashash,
            &data,
            &c.meta.chunk_compressed_cumulative
        )
        .is_ok());

        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());

        let c2 = res.unwrap();
        assert_eq!(c, c2);

        assert_eq!(c.meta.num_chunks, c2.meta.num_chunks);
        assert_eq!(data, c2.get_all_bytes(&mut reader).unwrap());
    }
    */
}
