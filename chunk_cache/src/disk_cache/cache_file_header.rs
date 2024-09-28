use std::io::{Read, Seek, Write};

use crate::error::ChunkCacheError;

pub struct CacheFileHeader {
    pub chunk_byte_indicies: Vec<u32>,
    pub header_len: usize,
}

impl CacheFileHeader {
    pub fn new<T: Into<Vec<u32>>>(chunk_byte_indicies: T) -> Self {
        let chunk_byte_indicies = chunk_byte_indicies.into();
        let header_len = (chunk_byte_indicies.len() + 1) * size_of::<u32>();
        Self {
            chunk_byte_indicies,
            header_len,
        }
    }

    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, ChunkCacheError> {
        reader.seek(std::io::SeekFrom::Start(0))?;
        let chunk_byte_indicies_len = read_u32(reader)?;
        let mut chunk_byte_indicies = Vec::with_capacity(chunk_byte_indicies_len as usize);
        for _ in 0..chunk_byte_indicies_len {
            let idx = read_u32(reader)?;
            chunk_byte_indicies.push(idx);
        }
        Ok(Self {
            chunk_byte_indicies,
            header_len: (chunk_byte_indicies_len as usize + 1) * size_of::<u32>(),
        })
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut num_written = write_u32(writer, self.chunk_byte_indicies.len() as u32)?;
        for idx in &self.chunk_byte_indicies {
            num_written += write_u32(writer, *idx)?;
        }
        Ok(num_written)
    }
}

pub fn read_u32<R: Read>(reader: &mut R) -> Result<u32, std::io::Error> {
    let mut buf = [0u8; size_of::<u32>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u32::from_le_bytes(buf))
}

pub fn write_u32<W: Write>(writer: &mut W, v: u32) -> Result<usize, std::io::Error> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(size_of::<u32>())
}
