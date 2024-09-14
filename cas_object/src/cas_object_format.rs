
use std::{cmp::min, io::{Error, Read, Seek, Write}};
use merklehash::{DataHash, MerkleHash};

use crate::error::CasObjectError;
use anyhow::anyhow;

const CAS_OBJECT_FORMAT_IDENT: [u8; 8] = [ b'X', b'C', b'A', b'S', b'B', b'L', b'O', b'B'];
const CAS_OBJECT_FORMAT_VERSION: u8 = 0;
const CAS_OBJECT_HEADER_DEFAULT_LENGTH: u32 = 50;
const CAS_OBJECT_COMPRESSION_UNCOMPRESSED: u8 = 0;
const CAS_OBJECT_COMPRESSION_LZ4: u8 = 1;

#[derive(Clone, PartialEq, Eq, Debug)]
/// Header struct for [CasObject]
/// 
/// See details here: https://www.notion.so/huggingface2/Introduction-To-XetHub-Storage-Architecture-And-The-Integration-Path-54c3d14c682c4e41beab2364f273fc35?pvs=4#4ffa9b930a6942bd87f054714865375d
pub struct CasObjectHeader {
    /// CAS identifier: "XCASBLOB"
    pub ident: [u8; 8],

    /// Format version, expected to be 0 right now.
    pub version: u8,
    
    /// Compression scheme for CAS block, 0 for uncompressed, 1 for LZ4
    pub compression_method: u8,
    
    /// 256-bytes, The CAS Hash of this Xorb.
    pub cashash: DataHash,

    /// Uncompressed length of entire CAS block, sum of chunk_uncompressed_len below
    pub total_uncompressed_length: u32,

    /// Total number of chunks in the file. Length of vectors: chunk_uncompressed_len and chunk_compressed_cumulative
    pub num_chunks: u32,

    /// Num uncompressed bytes in each chunk. Ex. chunk_uncompressed_len[5] is the uncompressed length of chunk 5.
    pub chunk_uncompressed_len: Vec<u32>,

    /// Cumulative compressed chunk size, to allow seeking to chunk.
    /// ex. vec [100, 600] means chunk0 is 100 bytes and chunk1 is 500 bytes.
    /// To seek to chunk 5, go to chunk_compressed_cumulative[5] bytes in block.
    pub chunk_compressed_cumulative: Vec<u32>,

    // [Chunk 0] ...
    // [Chunk 1] ...
    // [Chunk 2] ...
}

impl Default for CasObjectHeader {
    fn default() -> Self {
        CasObjectHeader {
            ident: CAS_OBJECT_FORMAT_IDENT,
            version: CAS_OBJECT_FORMAT_VERSION,
            compression_method: CAS_OBJECT_COMPRESSION_UNCOMPRESSED,
            cashash: DataHash::default(),
            total_uncompressed_length: 0,
            num_chunks: 0,
            chunk_uncompressed_len: Vec::new(),
            chunk_compressed_cumulative: Vec::new(),
        }
    }
}

impl CasObjectHeader {

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, Error> {
        let mut total_bytes_written = 0;

        // Helper function to write data and update the byte count
        let mut write_bytes = |data: &[u8]| -> Result<(), Error> {
            writer.write_all(data)?;
            total_bytes_written += data.len();
            Ok(())
        };

        // Write fixed-size fields
        write_bytes(&self.ident)?;
        write_bytes(&[self.version])?;
        write_bytes(&[self.compression_method])?;

        // Write cashash
        write_bytes(self.cashash.as_bytes())?;

        // Write total_uncompressed_length
        write_bytes(&self.total_uncompressed_length.to_le_bytes())?;

        // Write num_chunks
        write_bytes(&self.num_chunks.to_le_bytes())?;

        // Write chunk_uncompressed_len vector
        for &len in &self.chunk_uncompressed_len {
            write_bytes(&len.to_le_bytes())?;
        }

        // Write chunk_compressed_cumulative vector
        for &len in &self.chunk_compressed_cumulative {
            write_bytes(&len.to_le_bytes())?;
        }

        Ok(total_bytes_written)
    }

    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<(Self, usize), CasObjectError> {

        let mut total_bytes_read: usize = 0;

        // Helper function to read data and update the byte count
        let mut read_bytes = |data: &mut [u8]| -> Result<(), CasObjectError> {
            reader.read_exact(data)?;
            total_bytes_read += data.len();
            Ok(())
        };

        let mut ident = [0u8; 8];
        read_bytes(&mut ident)?;

        if ident != CAS_OBJECT_FORMAT_IDENT {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Ident")));
        }

        let mut version = [0u8; 1];
        read_bytes(&mut version)?;

        if version[0] != CAS_OBJECT_FORMAT_VERSION {
            return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version")));
        }

        let mut compression_method = [0u8; 1];
        read_bytes(&mut compression_method)?;

        let mut buf = [0u8; 32];
        read_bytes(&mut buf)?;
        let cashash = DataHash::from(&buf);
        
        let mut total_uncompressed_length = [0u8; 4];
        read_bytes(&mut total_uncompressed_length)?;
        let total_uncompressed_length = u32::from_le_bytes(total_uncompressed_length);

        let mut num_chunks = [0u8; 4];
        read_bytes(&mut num_chunks)?;
        let num_chunks = u32::from_le_bytes(num_chunks);

        let mut chunk_uncompressed_len = Vec::with_capacity(num_chunks as usize);
        for _ in 0..num_chunks {
            let mut buf = [0u8; 4];
            read_bytes(&mut buf)?;
            chunk_uncompressed_len.push(u32::from_le_bytes(buf));
        }

        let mut chunk_compressed_cumulative = Vec::with_capacity(num_chunks as usize);
        for _ in 0..num_chunks {
            let mut buf = [0u8; 4];
            read_bytes(&mut buf)?;
            chunk_compressed_cumulative.push(u32::from_le_bytes(buf));
        }

        Ok((CasObjectHeader {
            ident,
            version: version[0],
            compression_method: compression_method[0],
            cashash,
            total_uncompressed_length,
            num_chunks,
            chunk_uncompressed_len,
            chunk_compressed_cumulative,
        }, total_bytes_read))
    }
}



#[derive(Clone, PartialEq, Eq, Debug)]
/// XORB: 16MB data block for storing chunks.
/// 
/// Has header, and a set of functions that interact directly with XORB.
pub struct CasObject {
    /// Header CAS object, see CasObjectHeader struct.
    pub header: CasObjectHeader,
    
    /// Number of bytes in completed header, used for seeking to chunks.
    header_length: u32,
}

impl Default for CasObject {
    fn default() -> Self {
        Self { header: Default::default(), header_length: CAS_OBJECT_HEADER_DEFAULT_LENGTH }
    }
}

impl CasObject {

    /// Deserialize the header only.
    /// 
    /// This allows the CasObject to be partially constructed, allowing for range reads inside the CasObject.
    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CasObjectError> {
        let (header, header_len) = CasObjectHeader::deserialize(reader)?;
        let header_length = header_len as u32;
        Ok(Self { header, header_length })
    }

    /// Get range of content bytes from Xorb
    pub fn get_range<R: Read + Seek>(&self, reader: &mut R, start: u32, end: u32) -> Result<Vec<u8>, CasObjectError> {
        
        if end < start {
            return Err(CasObjectError::InvalidRange);
        }

        // make sure the end of the range is within the bounds of the xorb
        let end = min(end, self.header.total_uncompressed_length);
        
        let mut data = vec![0u8; (end - start) as usize];

        reader.seek(std::io::SeekFrom::Start(self.header_length as u64 + start as u64))?;
        reader.read_exact(&mut data)?;
        
        match self.header.compression_method {
            CAS_OBJECT_COMPRESSION_UNCOMPRESSED => Ok(data),
            CAS_OBJECT_COMPRESSION_LZ4 => Err(CasObjectError::FormatError(anyhow!("LZ4 compression method is not supported."))),
            _  => Err(CasObjectError::FormatError(anyhow!("Unknown compression method"))),
        }
    }

    /// Get all the content bytes from a Xorb
    pub fn get_all_bytes<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<u8>, CasObjectError> {
        // seek to header_length (from start).
        // if uncompressed, just read rest of uncompressed length and return.
        // if compressed, then walk compressed chunk vector and decompress and return.
        if self.header == Default::default() || self.header_length == 0 {
            return Err(CasObjectError::InternalError(anyhow!("Incomplete CasObject, no header")));
        }

        match self.header.compression_method {

            CAS_OBJECT_COMPRESSION_UNCOMPRESSED => {
                let mut data = vec![0; self.header.total_uncompressed_length as usize];
                reader.seek(std::io::SeekFrom::Start(self.header_length as u64))?;
                reader.read_exact(&mut data)?;
                Ok(data)
            },

            CAS_OBJECT_COMPRESSION_LZ4 => Err(CasObjectError::FormatError(anyhow!("LZ4 compression method is not supported."))),
            _  =>Err(CasObjectError::FormatError(anyhow!("Unknown compression method"))),
        }
    }

    /// Get all the content bytes from a Xorb, and return the chunk boundaries
    pub fn get_detailed_bytes<R: Read + Seek>(&self, reader: &mut R) -> Result<(Vec<u32>, Vec<u8>), CasObjectError> {
        // seek to header_length (from start).
        // if uncompressed, just read rest of uncompressed length and return.
        // if compressed, then walk compressed chunk vector and decompress and return.
        if self.header == Default::default() || self.header_length == 0 {
            return Err(CasObjectError::InternalError(anyhow!("Incomplete CasObject, no header")));
        }

        match self.header.compression_method {

            CAS_OBJECT_COMPRESSION_UNCOMPRESSED => {
                let mut data = vec![0; self.header.total_uncompressed_length as usize];
                reader.seek(std::io::SeekFrom::Start(self.header_length as u64))?;
                reader.read_exact(&mut data)?;
                Ok((self.header.chunk_compressed_cumulative.clone(), data))
            },

            CAS_OBJECT_COMPRESSION_LZ4 => Err(CasObjectError::FormatError(anyhow!("LZ4 compression method is not supported."))),
            _  =>Err(CasObjectError::FormatError(anyhow!("Unknown compression method"))),
        }
    }

    /// Used by LocalClient for generating Cas Object from chunk_boundaries while uploading or downloading blocks.
    pub fn serialize<W: Write+Seek>(writer: &mut W, hash: &MerkleHash, data: &[u8], chunk_boundaries: &Vec<u32>) -> Result<(Self, usize), CasObjectError> {
        
        let mut cas = CasObject::default();
        cas.header.cashash.copy_from_slice(hash.as_slice());
        cas.header.total_uncompressed_length = data.len() as u32;
        cas.header.num_chunks = chunk_boundaries.len() as u32;
        cas.header.chunk_uncompressed_len = Vec::with_capacity(chunk_boundaries.len());
        cas.header.chunk_compressed_cumulative = Vec::with_capacity(chunk_boundaries.len());

        let mut total_written_bytes: usize = 0;
        let mut written_bytes = Vec::<u8>::new();

        let mut start_idx = 0;
        for boundary in chunk_boundaries {
            let chunk_boundary: u32 = *boundary;
            
            // for uncompressed just take data as is, for compressed compress chunk and then write those out.

            // TODO: add support for compression here

            written_bytes.extend_from_slice(&data[start_idx as usize .. chunk_boundary as usize]);
            
            let chunk_size = chunk_boundary - start_idx;
            cas.header.chunk_uncompressed_len.push(chunk_size);
            
            // TODO: always take chunk size because compressed chunks will reduce the bytes needed for this chunk
            cas.header.chunk_compressed_cumulative.push(start_idx + chunk_size);

            start_idx = chunk_boundary;
        }

        // now that header is ready, write out to writer, and then write the bytes.
        total_written_bytes += cas.header.serialize(writer)?;

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

    #[test]
    fn test_default_header_initialization() {
        // Create an instance using the Default trait
        let default_instance = CasObjectHeader::default();
        
        // Expected default values
        let expected_default = CasObjectHeader {
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
        let len = cas.header.serialize(&mut writer).unwrap();
        
        assert_eq!(cas.header_length, len as u32);
        assert_eq!(cas.header_length, CAS_OBJECT_HEADER_DEFAULT_LENGTH);
    }

    fn gen_random_bytes(uncompressed_chunk_size: u32) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; uncompressed_chunk_size as usize];
        rng.fill(&mut data[..]);
        data        
    }

    fn build_cas_object(num_chunks: u32, uncompressed_chunk_size: u32, use_random_chunk_size: bool) -> (CasObject, Vec<u8>) {

        let mut c = CasObject::default();
        let mut data = Vec::<u8>::new();

        c.header.num_chunks = num_chunks;

        let mut total_bytes = 0;
        for _idx in 0..num_chunks {

            let chunk_size: u32 = if use_random_chunk_size {
                let mut rng = rand::thread_rng();
                rng.gen_range(1024..=uncompressed_chunk_size)
            } else {
                uncompressed_chunk_size
            };

            let bytes = gen_random_bytes(chunk_size);
            let len : u32 = bytes.len() as u32;
            c.header.chunk_uncompressed_len.push(len);
            data.extend(bytes);
            total_bytes += len;
            c.header.chunk_compressed_cumulative.push(total_bytes);
        }

        c.header.cashash = compute_data_hash(&data);
        c.header.total_uncompressed_length = total_bytes;
        
        // get header length
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let len = c.header.serialize(&mut writer).unwrap();
        c.header_length = len as u32;

        (c, data)
    }

    #[test]
    fn test_basic_serialization_mem() {
        // Arrange
        let (c, data) = build_cas_object(3, 100, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(&mut writer, &c.header.cashash, &data, &c.header.chunk_compressed_cumulative).is_ok());
        
        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        let c2 = res.unwrap();
        assert_eq!(c, c2);
        assert_eq!(c.header.cashash, c2.header.cashash);
    }

    #[test]
    fn test_serialization_deserialization_mem_medium() {
        // Arrange
        let (c, data) = build_cas_object(32, 16384, false);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(&mut writer, &c.header.cashash, &data, &c.header.chunk_compressed_cumulative).is_ok());
        
        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        
        let c2 = res.unwrap();
        assert_eq!(c, c2);
        
        let bytes_read = c2.get_all_bytes(&mut reader).unwrap();
        assert_eq!(c.header.num_chunks, c2.header.num_chunks);
        assert_eq!(data, bytes_read);
    }

    #[test]
    fn test_serialization_deserialization_mem_large_random() {
        // Arrange
        let (c, data) = build_cas_object(32, 65536, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(&mut writer, &c.header.cashash, &data, &c.header.chunk_compressed_cumulative).is_ok());
        
        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        
        let c2 = res.unwrap();
        assert_eq!(c, c2);
        
        assert_eq!(c.header.num_chunks, c2.header.num_chunks);
        assert_eq!(data, c2.get_all_bytes(&mut reader).unwrap());
    }
    
    #[test]
    fn test_serialization_deserialization_file_large_random() {
        // Arrange
        let (c, data) = build_cas_object(256, 65536, true);
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(&mut writer, &c.header.cashash, &data, &c.header.chunk_compressed_cumulative).is_ok());
        
        let mut reader = writer.clone();
        reader.set_position(0);
        let res = CasObject::deserialize(&mut reader);
        assert!(res.is_ok());
        
        let c2 = res.unwrap();
        assert_eq!(c, c2);
        
        assert_eq!(c.header.num_chunks, c2.header.num_chunks);
        assert_eq!(data, c2.get_all_bytes(&mut reader).unwrap());
    }

}