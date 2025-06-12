use std::io::{Read, Write};
use std::mem::size_of;

use anyhow::anyhow;
use merkledb::constants::MAXIMUM_CHUNK_SIZE;

use crate::cas_object_format::CAS_OBJECT_FORMAT_IDENT;
use crate::error::CasObjectError;
use crate::CompressionScheme;

pub mod deserialize_async;

pub const CAS_CHUNK_HEADER_LENGTH: usize = size_of::<CASChunkHeader>();
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
    pub fn new(compression_scheme: CompressionScheme, compressed_length: u32, uncompressed_length: u32) -> Self {
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

    pub fn get_compression_scheme(&self) -> Result<CompressionScheme, CasObjectError> {
        CompressionScheme::try_from(self.compression_scheme)
    }

    pub fn set_compression_scheme(&mut self, compression_scheme: CompressionScheme) {
        self.compression_scheme = compression_scheme as u8;
    }

    fn validate(&self) -> Result<(), CasObjectError> {
        let _ = self.get_compression_scheme()?;
        if self.version > CURRENT_VERSION {
            return Err(CasObjectError::FormatError(anyhow!(
                "chunk header version too high at {}, current version is {}",
                self.version,
                CURRENT_VERSION
            )));
        }
        if self.get_compressed_length() as usize > MAXIMUM_CHUNK_SIZE * 2 {
            return Err(CasObjectError::FormatError(anyhow!(
                "chunk header compressed length too large at {}, maximum: {MAXIMUM_CHUNK_SIZE}",
                self.get_compressed_length()
            )));
        }
        // the max chunk size is strictly enforced
        if self.get_uncompressed_length() as usize > MAXIMUM_CHUNK_SIZE {
            return Err(CasObjectError::FormatError(anyhow!(
                "chunk header uncompressed length too large at {}, maximum: {MAXIMUM_CHUNK_SIZE}",
                self.get_uncompressed_length()
            )));
        }
        Ok(())
    }
}

pub fn write_chunk_header<W: Write>(w: &mut W, chunk_header: &CASChunkHeader) -> std::io::Result<()> {
    w.write_all(&[chunk_header.version])?;
    w.write_all(&chunk_header.compressed_length)?;
    w.write_all(&[chunk_header.compression_scheme])?;
    w.write_all(&chunk_header.uncompressed_length)
}

#[inline]
fn copy_three_byte_num(buf: &mut [u8; 3], num: u32) {
    debug_assert!(num < 16_777_216); // verify that num is under 16MB (3 byte unsigned max + 1, 1 << 24)
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
    compression_scheme: Option<CompressionScheme>,
) -> Result<usize, CasObjectError> {
    let compression_scheme = compression_scheme.unwrap_or_else(|| CompressionScheme::choose_from_data(chunk));

    let compressed = compression_scheme.compress_from_slice(chunk)?;

    // set compression scheme and compressed data buffer to no compression if the compressed
    // length is longer than uncompressed
    let (compression_scheme, compressed) = if compressed.len() >= chunk.len() {
        (CompressionScheme::None, chunk.into())
    } else {
        (compression_scheme, compressed)
    };
    let header = CASChunkHeader::new(compression_scheme, compressed.len() as u32, chunk.len() as u32);
    write_chunk_header(w, &header)?;
    w.write_all(&compressed)?;

    Ok(size_of::<CASChunkHeader>() + compressed.len())
}

pub fn parse_chunk_header(chunk_header_bytes: [u8; CAS_CHUNK_HEADER_LENGTH]) -> Result<CASChunkHeader, CasObjectError> {
    if chunk_header_bytes[..CAS_OBJECT_FORMAT_IDENT.len()] == CAS_OBJECT_FORMAT_IDENT {
        return Err(CasObjectError::ChunkHeaderParseErrorFooterIdent);
    }
    let result: CASChunkHeader = unsafe { std::mem::transmute_copy(&chunk_header_bytes) };
    result.validate()?;
    Ok(result)
}

pub fn deserialize_chunk_header<R: Read>(reader: &mut R) -> Result<CASChunkHeader, CasObjectError> {
    let mut buf = [0u8; size_of::<CASChunkHeader>()];
    reader.read_exact(&mut buf)?;
    parse_chunk_header(buf)
}

pub fn deserialize_chunk<R: Read>(reader: &mut R) -> Result<(Vec<u8>, usize, u32), CasObjectError> {
    let mut buf = Vec::new();
    let (compressed_chunk_size, uncompressed_chunk_size) = deserialize_chunk_to_writer(reader, &mut buf)?;
    Ok((buf, compressed_chunk_size, uncompressed_chunk_size))
}

/// Returns the compressed chunk size along with the uncompressed chunk size as a tuple, (compressed, uncompressed)
pub fn deserialize_chunk_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, u32), CasObjectError> {
    let header = deserialize_chunk_header(reader)?;
    let mut compressed_data_reader = reader.take(header.get_compressed_length().into());

    let uncompressed_len = header
        .get_compression_scheme()?
        .decompress_from_reader(&mut compressed_data_reader, writer)?;

    if uncompressed_len != header.get_uncompressed_length() as u64 {
        return Err(CasObjectError::FormatError(anyhow!(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header"
        )));
    }

    Ok((header.get_compressed_length() as usize + CAS_CHUNK_HEADER_LENGTH, uncompressed_len as u32))
}

pub fn deserialize_chunks<R: Read>(reader: &mut R) -> Result<(Vec<u8>, Vec<u32>), CasObjectError> {
    let mut buf = Vec::new();
    let (_, chunk_byte_indices) = deserialize_chunks_to_writer(reader, &mut buf)?;
    Ok((buf, chunk_byte_indices))
}

pub fn deserialize_chunks_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, Vec<u32>), CasObjectError> {
    let mut num_compressed_written = 0;
    let mut num_uncompressed_written = 0;

    // chunk indices are expected to record the byte indices of uncompressed chunks
    // as they are read from the reader, so start with [0, len(uncompressed chunk 0..n), total length]
    let mut chunk_byte_indices = Vec::<u32>::new();
    chunk_byte_indices.push(num_uncompressed_written);

    loop {
        match deserialize_chunk_to_writer(reader, writer) {
            Ok((delta_written, uncompressed_chunk_len)) => {
                num_compressed_written += delta_written;
                num_uncompressed_written += uncompressed_chunk_len;
                chunk_byte_indices.push(num_uncompressed_written); // record end of current chunk
            },
            Err(CasObjectError::InternalIOError(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(CasObjectError::InternalIOError(e));
            },
            Err(e) => return Err(e),
        }
    }

    Ok((num_compressed_written, chunk_byte_indices))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::test_utils::{build_cas_object, ChunkSize};

    const COMPRESSED_LEN: u32 = 66051;
    const UNCOMPRESSED_LEN: u32 = 131072;

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
        let header = CASChunkHeader::new(CompressionScheme::None, COMPRESSED_LEN, UNCOMPRESSED_LEN);

        let mut buf = Vec::with_capacity(size_of::<CASChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        assert_chunk_header_deserialize_match(&header, &buf);

        let header = CASChunkHeader::new(CompressionScheme::LZ4, COMPRESSED_LEN, UNCOMPRESSED_LEN);

        let mut buf = Vec::with_capacity(size_of::<CASChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        assert_chunk_header_deserialize_match(&header, &buf);
    }

    #[test]
    fn test_basic_header_deserialization() {
        let header = CASChunkHeader::new(CompressionScheme::None, COMPRESSED_LEN, UNCOMPRESSED_LEN);

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
        write_chunk_header(&mut buf, &header).unwrap();
        buf.extend_from_slice(data);

        let (data_copy, _, _) = deserialize_chunk(&mut Cursor::new(buf)).unwrap();
        assert_eq!(data_copy.as_slice(), data);
    }

    const CASES: &[(u32, ChunkSize, CompressionScheme)] = &[
        (2, ChunkSize::Fixed(16), CompressionScheme::None),
        (10, ChunkSize::Fixed(16), CompressionScheme::None),
        (100, ChunkSize::Fixed(16), CompressionScheme::None),
        (1000, ChunkSize::Fixed(16), CompressionScheme::None),
        (2, ChunkSize::Fixed(16 << 10), CompressionScheme::None),
        (10, ChunkSize::Fixed(16 << 10), CompressionScheme::None),
        (100, ChunkSize::Fixed(16 << 10), CompressionScheme::LZ4),
        (1000, ChunkSize::Fixed(16 << 10), CompressionScheme::LZ4),
        (2, ChunkSize::Random(1024, 16 << 10), CompressionScheme::LZ4),
        (10, ChunkSize::Random(1024, 16 << 10), CompressionScheme::LZ4),
        (100, ChunkSize::Random(1024, 16 << 10), CompressionScheme::LZ4),
        (1000, ChunkSize::Random(1024, 16 << 10), CompressionScheme::LZ4),
        (1000, ChunkSize::Random(1024, 16 << 10), CompressionScheme::ByteGrouping4LZ4),
    ];

    #[test]
    fn test_deserialize_multiple_chunks() {
        for (num_chunks, chunk_size, compression_scheme) in CASES {
            let (c, cas_data, raw_data, raw_chunk_boundaries) =
                build_cas_object(*num_chunks, *chunk_size, *compression_scheme);
            let mut deserialized = Vec::new();
            let res = deserialize_chunks_to_writer(&mut Cursor::new(&cas_data), &mut deserialized);
            assert!(res.is_ok());
            assert_eq!(deserialized.len(), raw_data.len());
            assert_eq!(deserialized, raw_data);

            let (num_read, chunk_byte_indices) = res.unwrap();
            assert_eq!(num_read, cas_data.len());
            assert_eq!(chunk_byte_indices[0], 0);
            assert_eq!(
                chunk_byte_indices.iter().skip(1).map(|v| *v).collect::<Vec<_>>(),
                raw_chunk_boundaries.iter().map(|v| v.1).collect::<Vec<_>>()
            );
            assert_eq!(
                chunk_byte_indices.iter().skip(1).map(|v| *v).collect::<Vec<_>>(),
                c.info.unpacked_chunk_offsets,
            );

            let ChunkSize::Fixed(chunk_size) = chunk_size else {
                // not testing chunk length for random chunk sizes
                return;
            };
            // verify that chunk boundaries are correct
            for i in 0..chunk_byte_indices.len() - 1 {
                assert_eq!(chunk_byte_indices[i + 1] - chunk_byte_indices[i], *chunk_size);
            }
        }
    }
}
