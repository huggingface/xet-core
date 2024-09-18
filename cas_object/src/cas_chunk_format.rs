use std::{
    io::{copy, Cursor, Read, Seek, Write},
    slice,
};

use anyhow::anyhow;
use bytes::{BufMut, Bytes, BytesMut};
use cas_types::compression_scheme::CompressionScheme;
use lz4_flex::block::uncompressed_size;

use crate::error::CasObjectError;

const CURRENT_VERSION: u8 = 0;

#[repr(C, packed)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct CASChunkHeader {
    pub version: u8,              // 1 byte
    compressed_length: [u8; 3],   // 3 bytes
    compression_scheme: u8,       // 1 byte
    uncompressed_length: [u8; 3], // 3 bytes
}

impl CASChunkHeader {
    pub fn new(
        compression_scheme: CompressionScheme,
        compressed_length: u32,
        uncompressed_length: u32,
    ) -> Self {
        let mut result = CASChunkHeader {
            version: CURRENT_VERSION,
            ..Default::default()
        };
        result.set_compression_scheme(compression_scheme);
        result.set_compressed_length(compressed_length);
        result.set_uncompressed_length(uncompressed_length);
        result
    }

    // Helper function to set compressed length from u32
    pub fn set_compressed_length(&mut self, length: u32) {
        copy_three_byte_num(&mut self.compressed_length, length);
    }

    // Helper function to get compressed length as u32
    pub fn get_compressed_length(&self) -> u32 {
        convert_three_byte_num(&self.compressed_length)
    }

    // Helper function to set uncompressed length from u32
    pub fn set_uncompressed_length(&mut self, length: u32) {
        copy_three_byte_num(&mut self.uncompressed_length, length);
    }

    // Helper function to get uncompressed length as u32
    pub fn get_uncompressed_length(&self) -> u32 {
        convert_three_byte_num(&self.uncompressed_length)
    }

    pub fn get_compression_scheme(&self) -> CompressionScheme {
        CompressionScheme::try_from(self.compression_scheme).unwrap_or_default()
    }

    pub fn set_compression_scheme(&mut self, compression_scheme: CompressionScheme) {
        self.compression_scheme = compression_scheme as u8;
    }
}

fn write_chunk_header<W: Write>(w: &mut W, chunk_header: &CASChunkHeader) -> std::io::Result<()> {
    w.write_all(&[chunk_header.version])?;
    w.write_all(&chunk_header.compressed_length)?;
    w.write_all(&[chunk_header.compression_scheme])?;
    w.write_all(&chunk_header.uncompressed_length)
}

#[inline]
fn copy_three_byte_num(buf: &mut [u8; 3], num: u32) {
    let bytes = num.to_le_bytes(); // Convert u32 to little-endian bytes
    buf.copy_from_slice(&bytes[0..3]);
}

#[inline]
fn convert_three_byte_num(buf: &[u8; 3]) -> u32 {
    let mut bytes = [0u8; 4]; // Create 4-byte array
    bytes[0..3].copy_from_slice(buf); // Copy 3 bytes
    u32::from_le_bytes(bytes) // Convert back to u32
}

pub fn serialize_chunk<W: Write + Seek>(
    chunk: &[u8],
    w: &mut W,
    compression_scheme: CompressionScheme,
) -> Result<usize, CasObjectError> {
    let uncompressed_len = chunk.len();

    let compressed = match compression_scheme {
        CompressionScheme::None => Vec::from(chunk),
        CompressionScheme::LZ4 => {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
            enc.write_all(chunk)
                .map_err(|e| CasObjectError::InternalError(anyhow!("{e}")))?;
            enc.finish()
                .map_err(|e| CasObjectError::InternalError(anyhow!("{e}")))?
        }
    };
    let compressed_len = compressed.len();
    let header = CASChunkHeader::new(
        compression_scheme,
        compressed_len as u32,
        uncompressed_len as u32,
    );

    write_chunk_header(w, &header).map_err(|e| CasObjectError::InternalError(anyhow!("{e}")))?;
    w.write_all(&compressed)
        .map_err(|e| CasObjectError::InternalError(anyhow!("{e}")))?;

    Ok(size_of::<CASChunkHeader>() + compressed_len)
}

pub fn deserialize_chunk_header<R: Read>(reader: &mut R) -> Result<CASChunkHeader, CasObjectError> {
    let mut result = CASChunkHeader::default();
    unsafe {
        let buf = slice::from_raw_parts_mut(
            &mut result as *mut _ as *mut u8,
            size_of::<CASChunkHeader>(),
        );
        reader.read_exact(buf)?;
    }

    Ok(result)
}

pub fn deserialize_chunk<R: Read>(
    reader: &mut R,
) -> Result<(CASChunkHeader, Vec<u8>), CasObjectError> {
    let header = deserialize_chunk_header(reader)?;
    let mut compressed_buf = Vec::with_capacity(header.get_compressed_length() as usize);
    let mut chunk = reader.take(header.get_compressed_length() as u64);
    chunk.read_to_end(&mut compressed_buf)?;

    let uncompressed_buf = match header.get_compression_scheme() {
        CompressionScheme::None => compressed_buf,
        CompressionScheme::LZ4 => {
            let mut uncompressed_buf = Vec::new();
            let mut dec = lz4_flex::frame::FrameDecoder::new(Cursor::new(compressed_buf));
            copy(&mut dec, &mut uncompressed_buf)?;
            uncompressed_buf
        }
    };

    Ok((header, uncompressed_buf))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use cas_types::compression_scheme::CompressionScheme;

    const COMP_LEN: u32 = 0x010203;
    const UNCOMP_LEN: u32 = 0x040506;

    fn assert_chunk_header_deserialize_match(header: &CASChunkHeader, buf: &[u8]) {
        assert_eq!(buf[0], header.version);
        assert_eq!(buf[4], header.compression_scheme);
        for i in 0..3 {
            assert_eq!(buf[1 + i], header.compressed_length[i]);
            assert_eq!(buf[5 + i], header.uncompressed_length[i]);
        }
    }

    #[test]
    fn test_basic_header_serialization() {
        let header = CASChunkHeader::new(CompressionScheme::None, COMP_LEN, UNCOMP_LEN);

        let mut buf = Vec::with_capacity(size_of::<CASChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        assert_chunk_header_deserialize_match(&header, &buf);

        let header = CASChunkHeader::new(CompressionScheme::LZ4, COMP_LEN, UNCOMP_LEN);

        let mut buf = Vec::with_capacity(size_of::<CASChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        assert_chunk_header_deserialize_match(&header, &buf);
    }

    #[test]
    fn test_basic_header_deserialization() {
        let header = CASChunkHeader::new(CompressionScheme::None, COMP_LEN, UNCOMP_LEN);

        let mut buf = Vec::with_capacity(size_of::<CASChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        let deserialized_header = deserialize_chunk_header(&mut Cursor::new(buf)).unwrap();
        assert_eq!(deserialized_header, header)
    }

    #[test]
    fn test_deserialize_chunk_uncompressed() {
        let data = &[1, 2, 3, 4];
        let header = CASChunkHeader::new(CompressionScheme::None, 4, 4);
        let mut buf = Vec::with_capacity(size_of::<CASChunkHeader>() + 4);
        println!("len buf: {}", buf.len());
        write_chunk_header(&mut buf, &header).unwrap();
        buf.extend_from_slice(data);

        let (deserialized_header, data_copy) = deserialize_chunk(&mut Cursor::new(buf)).unwrap();
        assert_eq!(&deserialized_header, &header);
        assert_eq!(data_copy.as_slice(), data);
    }
}
