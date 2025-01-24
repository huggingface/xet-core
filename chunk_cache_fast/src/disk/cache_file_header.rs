use std::io::{Read, Seek, Write};
use std::mem::size_of;

use utils::serialization_utils::{read_u32, write_u32, write_u32s};

use crate::error::ChunkCacheError;

/// Header for every cache file, it is simple to deserialize and serialize
/// All numbers are unsigned 32 bit little endian integers
///
/// format:
/// (chunk_byte_indices length n)
/// (
///     chunk_byte_indices[0]
///     chunk_byte_indices[1]
///     chunk_byte_indices[2]
///     ...
///     chunk_byte_indices[n - 1]
/// )
pub struct CacheFileHeader {
    pub chunk_byte_indices: Vec<u32>,
}

impl CacheFileHeader {
    pub fn new<T: Into<Vec<u32>>>(chunk_byte_indices: T) -> Self {
        let chunk_byte_indices = chunk_byte_indices.into();
        Self { chunk_byte_indices }
    }

    pub fn header_len(&self) -> usize {
        (self.chunk_byte_indices.len() + 1) * size_of::<u32>()
    }

    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, ChunkCacheError> {
        reader.seek(std::io::SeekFrom::Start(0))?;
        let chunk_byte_indices_len = read_u32(reader)?;
        let mut chunk_byte_indices: Vec<u32> = Vec::with_capacity(chunk_byte_indices_len as usize);
        for i in 0..chunk_byte_indices_len {
            let idx = read_u32(reader)?;
            if i == 0 && idx != 0 {
                return Err(ChunkCacheError::parse("first byte index isn't 0"));
            } else if !chunk_byte_indices.is_empty() && chunk_byte_indices.last().unwrap() >= &idx {
                return Err(ChunkCacheError::parse("chunk byte indices are not strictly increasing"));
            }
            chunk_byte_indices.push(idx);
        }

        Ok(Self::new(chunk_byte_indices))
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
        write_u32(writer, self.chunk_byte_indices.len() as u32)?;
        write_u32s(writer, &self.chunk_byte_indices)?;
        Ok(())
    }
}
