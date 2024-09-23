use std::{
    io::{self, copy, Cursor, Read, Write},
    mem::size_of,
    slice,
};

use crate::error::CasObjectError;
use anyhow::anyhow;
use crate::CompressionScheme;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

pub const CAS_CHUNK_HEADER_LENGTH: u8 = 8;
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
    debug_assert!(num < 16_777_216); // verify that chunk is under 16MB
    let bytes = num.to_le_bytes(); // Convert u32 to little-endian bytes
    buf.copy_from_slice(&bytes[0..3]);
}

#[inline]
fn convert_three_byte_num(buf: &[u8; 3]) -> u32 {
    let mut bytes = [0u8; 4]; // Create 4-byte array
    bytes[0..3].copy_from_slice(buf); // Copy 3 bytes
    u32::from_le_bytes(bytes) // Convert back to u32
}

pub fn serialize_chunk<W: Write>(
    chunk: &[u8],
    w: &mut W,
    compression_scheme: CompressionScheme,
) -> Result<usize, CasObjectError> {
    let uncompressed_len = chunk.len();

    let compressed = match compression_scheme {
        CompressionScheme::None => Vec::from(chunk),
        CompressionScheme::LZ4 => {
            let mut enc = FrameEncoder::new(Vec::new());
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

pub fn deserialize_chunk<R: Read>(reader: &mut R) -> Result<Vec<u8>, CasObjectError> {
    let mut buf = Vec::new();
    let _ = deserialize_chunk_to_writer(reader, &mut buf)?;
    Ok(buf)
}

pub fn deserialize_chunk_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<usize, CasObjectError> {
    let header = deserialize_chunk_header(reader)?;
    let mut compressed_buf = vec![0u8; header.get_compressed_length() as usize];
    reader.read_exact(&mut compressed_buf)?;

    match header.get_compression_scheme() {
        CompressionScheme::None => writer.write_all(&compressed_buf)?,
        CompressionScheme::LZ4 => {
            let mut dec = FrameDecoder::new(Cursor::new(compressed_buf));
            copy(&mut dec, writer)?;
        }
    };

    Ok(header.get_uncompressed_length() as usize)
}

pub fn deserialize_chunks<R: Read>(reader: &mut R) -> Result<Vec<u8>, CasObjectError> {
    let mut buf = Vec::new();
    let _ = deserialize_chunks_to_writer(reader, &mut buf)?;
    Ok(buf)
}

pub fn deserialize_chunks_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<usize, CasObjectError> {
    let mut num_written = 0;

    loop {
        match deserialize_chunk_to_writer(reader, writer) {
            Ok(delta_written) => {
                num_written += delta_written;
            }
            Err(CasObjectError::InternalIOError(e)) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(CasObjectError::InternalIOError(e));
            }
            Err(e) => return Err(e),
        }
    }

    Ok(num_written)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use CompressionScheme;
    use rand::Rng;

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

        let data_copy = deserialize_chunk(&mut Cursor::new(buf)).unwrap();
        assert_eq!(data_copy.as_slice(), data);
    }

    fn gen_random_bytes(uncompressed_chunk_size: u32) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; uncompressed_chunk_size as usize];
        rng.fill(&mut data[..]);
        data
    }

    const CHUNK_SIZE: usize = 1000;

    fn get_chunks(num_chunks: u32, compression_scheme: CompressionScheme) -> Vec<u8> {
        let mut out = Vec::new();
        for _ in 0..num_chunks {
            let data = gen_random_bytes(CHUNK_SIZE as u32);
            serialize_chunk(&data, &mut out, compression_scheme).unwrap();
        }
        out
    }

    #[test]
    fn test_deserialize_multiple_chunks() {
        let cases = [
            (1, CompressionScheme::None),
            (3, CompressionScheme::None),
            (5, CompressionScheme::LZ4),
            (100, CompressionScheme::None),
            (100, CompressionScheme::LZ4),
        ];
        for (num_chunks, compression_scheme) in cases {
            let chunks = get_chunks(num_chunks, compression_scheme);
            let mut buf = Vec::new();
            let res = deserialize_chunks_to_writer(&mut Cursor::new(chunks), &mut buf);
            assert!(res.is_ok());
            assert_eq!(buf.len(), num_chunks as usize * CHUNK_SIZE);
        }
    }
}
