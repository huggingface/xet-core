use std::io::{Read, Write};
use std::mem::size_of;

use super::CompressionScheme;
use super::constants::MAX_CHUNK_SIZE;
use super::xorb_object_format::XORB_OBJECT_FORMAT_IDENT;
use crate::error::CoreError;

pub mod deserialize_async;

pub const XORB_CHUNK_HEADER_LENGTH: usize = size_of::<XorbChunkHeader>();
const CURRENT_VERSION: u8 = 0;

#[repr(C, packed)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct XorbChunkHeader {
    pub version: u8,              // 1 byte
    compressed_length: [u8; 3],   // 3 bytes
    compression_scheme: u8,       // 1 byte
    uncompressed_length: [u8; 3], // 3 bytes
}

impl XorbChunkHeader {
    pub fn new(compression_scheme: CompressionScheme, compressed_length: u32, uncompressed_length: u32) -> Self {
        let mut result = XorbChunkHeader {
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

    pub fn get_compression_scheme(&self) -> Result<CompressionScheme, CoreError> {
        CompressionScheme::try_from(self.compression_scheme)
    }

    pub fn set_compression_scheme(&mut self, compression_scheme: CompressionScheme) {
        debug_assert_ne!(compression_scheme, CompressionScheme::Auto);
        self.compression_scheme = compression_scheme as u8;
    }

    fn validate(&self) -> Result<(), CoreError> {
        let _ = self.get_compression_scheme()?;
        if self.version > CURRENT_VERSION {
            return Err(CoreError::MalformedData(format!(
                "chunk header version too high at {}, current version is {}",
                self.version, CURRENT_VERSION
            )));
        }
        if self.get_compressed_length() as usize > *MAX_CHUNK_SIZE * 2 {
            return Err(CoreError::MalformedData(format!(
                "chunk header compressed length too large at {}, maximum: {}",
                self.get_compressed_length(),
                *MAX_CHUNK_SIZE
            )));
        }
        // the max chunk size is strictly enforced
        if self.get_uncompressed_length() as usize > *MAX_CHUNK_SIZE {
            return Err(CoreError::MalformedData(format!(
                "chunk header uncompressed length too large at {}, maximum: {}",
                self.get_uncompressed_length(),
                *MAX_CHUNK_SIZE
            )));
        }
        Ok(())
    }
}

pub fn write_chunk_header<W: Write>(w: &mut W, chunk_header: &XorbChunkHeader) -> std::io::Result<()> {
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
    compression_scheme: CompressionScheme,
) -> Result<usize, CoreError> {
    let compression_scheme = compression_scheme.resolve_for_data(chunk);
    debug_assert_ne!(compression_scheme, CompressionScheme::Auto);
    if compression_scheme == CompressionScheme::Auto {
        return Err(CoreError::MalformedData(
            "CompressionScheme::Auto cannot be serialized into xorb chunks".to_string(),
        ));
    }

    let compressed = compression_scheme.compress_from_slice(chunk)?;

    // set compression scheme and compressed data buffer to no compression if the compressed
    // length is longer than uncompressed
    let (compression_scheme, compressed) = if compressed.len() >= chunk.len() {
        (CompressionScheme::None, chunk.into())
    } else {
        (compression_scheme, compressed)
    };
    let header = XorbChunkHeader::new(compression_scheme, compressed.len() as u32, chunk.len() as u32);
    write_chunk_header(w, &header)?;
    w.write_all(&compressed)?;

    Ok(size_of::<XorbChunkHeader>() + compressed.len())
}

pub fn parse_chunk_header(chunk_header_bytes: [u8; XORB_CHUNK_HEADER_LENGTH]) -> Result<XorbChunkHeader, CoreError> {
    if chunk_header_bytes[..XORB_OBJECT_FORMAT_IDENT.len()] == XORB_OBJECT_FORMAT_IDENT {
        return Err(CoreError::ChunkHeaderParse);
    }
    let result: XorbChunkHeader = unsafe { std::mem::transmute_copy(&chunk_header_bytes) };
    result.validate()?;
    Ok(result)
}

pub fn deserialize_chunk_header<R: Read>(reader: &mut R) -> Result<XorbChunkHeader, CoreError> {
    let mut buf = [0u8; size_of::<XorbChunkHeader>()];
    reader.read_exact(&mut buf)?;
    parse_chunk_header(buf)
}

pub fn deserialize_chunk<R: Read>(reader: &mut R) -> Result<(Vec<u8>, usize, u32), CoreError> {
    let mut buf = Vec::new();
    let (compressed_chunk_size, uncompressed_chunk_size) = deserialize_chunk_to_writer(reader, &mut buf)?;
    Ok((buf, compressed_chunk_size, uncompressed_chunk_size))
}

/// Returns the compressed chunk size along with the uncompressed chunk size as a tuple, (compressed, uncompressed)
pub fn deserialize_chunk_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, u32), CoreError> {
    let header = deserialize_chunk_header(reader)?;
    deserialize_chunk_with_header_to_writer(reader, writer, header)
}

fn deserialize_chunk_with_header_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    header: XorbChunkHeader,
) -> Result<(usize, u32), CoreError> {
    let mut compressed_data_reader = reader.take(header.get_compressed_length().into());

    let uncompressed_len = header
        .get_compression_scheme()?
        .decompress_from_reader(&mut compressed_data_reader, writer)?;

    if uncompressed_len != header.get_uncompressed_length() as u64 {
        return Err(CoreError::MalformedData(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header".to_string(),
        ));
    }

    Ok((header.get_compressed_length() as usize + XORB_CHUNK_HEADER_LENGTH, uncompressed_len as u32))
}

pub fn deserialize_chunks<R: Read>(reader: &mut R) -> Result<(Vec<u8>, Vec<u32>), CoreError> {
    let mut buf = Vec::new();
    let (_, chunk_byte_indices) = deserialize_chunks_to_writer(reader, &mut buf)?;
    Ok((buf, chunk_byte_indices))
}

/// Appends a deserialized chunk segment to existing accumulated buffers.
///
/// `deserialize_chunks` returns `chunk_byte_indices` starting with a leading `0`.
/// When concatenating multiple segments, this function deduplicates that leading
/// zero for subsequent segments and rebases all indices to account for data already
/// accumulated.
pub fn append_chunk_segment(
    all_data: &mut Vec<u8>,
    all_chunk_indices: &mut Vec<u32>,
    segment_data: &[u8],
    segment_indices: &[u32],
) {
    let base_offset = all_data.len() as u32;
    if all_chunk_indices.is_empty() {
        all_chunk_indices.extend_from_slice(segment_indices);
    } else {
        all_chunk_indices.extend(segment_indices.iter().skip(1).map(|&o| o + base_offset));
    }
    all_data.extend_from_slice(segment_data);
}

/// Reads the next chunk header, returning `None` on clean EOF.
///
/// Uses a single `read()` call to detect EOF (returns 0), then completes
/// any partial header with `read_exact`. An `UnexpectedEof` from `read_exact`
/// means the stream was truncated mid-header.
fn try_read_chunk_header<R: Read>(reader: &mut R) -> Result<Option<XorbChunkHeader>, CoreError> {
    let mut header_buf = [0u8; XORB_CHUNK_HEADER_LENGTH];
    let n = match reader.read(&mut header_buf) {
        Ok(0) => return Ok(None),
        Ok(n) => n,
        Err(e) => return Err(CoreError::Io(e)),
    };
    if n < XORB_CHUNK_HEADER_LENGTH {
        reader.read_exact(&mut header_buf[n..])?;
    }
    parse_chunk_header(header_buf).map(Some)
}

pub fn deserialize_chunks_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, Vec<u32>), CoreError> {
    let mut num_compressed_written = 0;
    let mut num_uncompressed_written = 0;

    // chunk indices are expected to record the byte indices of uncompressed chunks
    // as they are read from the reader, so start with [0, len(uncompressed chunk 0..n), total length]
    let mut chunk_byte_indices = Vec::<u32>::new();
    chunk_byte_indices.push(num_uncompressed_written);

    while let Some(header) = try_read_chunk_header(reader)? {
        let (delta_written, uncompressed_chunk_len) = deserialize_chunk_with_header_to_writer(reader, writer, header)?;
        num_compressed_written += delta_written;
        num_uncompressed_written += uncompressed_chunk_len;
        chunk_byte_indices.push(num_uncompressed_written);
    }

    Ok((num_compressed_written, chunk_byte_indices))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::xorb_object::xorb_format_test_utils::{ChunkSize, build_xorb_object};

    const COMPRESSED_LEN: u32 = 66051;
    const UNCOMPRESSED_LEN: u32 = 131072;

    fn assert_chunk_header_deserialize_match(header: &XorbChunkHeader, buf: &[u8]) {
        assert_eq!(buf[0], header.version);
        assert_eq!(buf[4], header.compression_scheme);
        for i in 0..3 {
            assert_eq!(buf[1 + i], header.compressed_length[i]);
            assert_eq!(buf[5 + i], header.uncompressed_length[i]);
        }
    }

    #[test]
    fn test_basic_header_serialization() {
        let header = XorbChunkHeader::new(CompressionScheme::None, COMPRESSED_LEN, UNCOMPRESSED_LEN);

        let mut buf = Vec::with_capacity(size_of::<XorbChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        assert_chunk_header_deserialize_match(&header, &buf);

        let header = XorbChunkHeader::new(CompressionScheme::LZ4, COMPRESSED_LEN, UNCOMPRESSED_LEN);

        let mut buf = Vec::with_capacity(size_of::<XorbChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        assert_chunk_header_deserialize_match(&header, &buf);
    }

    #[test]
    fn test_basic_header_deserialization() {
        let header = XorbChunkHeader::new(CompressionScheme::None, COMPRESSED_LEN, UNCOMPRESSED_LEN);

        let mut buf = Vec::with_capacity(size_of::<XorbChunkHeader>());
        write_chunk_header(&mut buf, &header).unwrap();
        let deserialized_header = deserialize_chunk_header(&mut Cursor::new(buf)).unwrap();
        assert_eq!(deserialized_header, header)
    }

    #[test]
    fn test_deserialize_chunk_uncompressed() {
        let data = &[1, 2, 3, 4];
        let header = XorbChunkHeader::new(CompressionScheme::None, 4, 4);
        let mut buf = Vec::with_capacity(size_of::<XorbChunkHeader>() + 4);
        write_chunk_header(&mut buf, &header).unwrap();
        buf.extend_from_slice(data);

        let (data_copy, _, _) = deserialize_chunk(&mut Cursor::new(buf)).unwrap();
        assert_eq!(data_copy.as_slice(), data);
    }

    #[test]
    fn test_auto_scheme_never_serialized_in_chunk_header() {
        let chunk = vec![0u8; 4096];
        let mut serialized = Vec::new();
        let _ = serialize_chunk(&chunk, &mut serialized, CompressionScheme::Auto).unwrap();

        let header = deserialize_chunk_header(&mut Cursor::new(serialized)).unwrap();
        assert_ne!(header.get_compression_scheme().unwrap(), CompressionScheme::Auto);
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
            let (c, xorb_data, raw_data, raw_chunk_boundaries) =
                build_xorb_object(*num_chunks, *chunk_size, *compression_scheme);
            let mut deserialized = Vec::new();
            let res = deserialize_chunks_to_writer(&mut Cursor::new(xorb_data.as_slice()), &mut deserialized);
            assert!(res.is_ok());
            assert_eq!(deserialized.len(), raw_data.len());
            assert_eq!(deserialized, raw_data);

            let (num_read, chunk_byte_indices) = res.unwrap();
            assert_eq!(num_read, xorb_data.len());
            assert_eq!(chunk_byte_indices[0], 0);
            assert_eq!(
                chunk_byte_indices.iter().skip(1).map(|v| *v).collect::<Vec<u32>>(),
                raw_chunk_boundaries.iter().map(|v| v.1).collect::<Vec<u32>>()
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

    #[test]
    fn test_append_chunk_segment() {
        let mut all_data = Vec::new();
        let mut all_indices = Vec::<u32>::new();

        // First segment: simulates deserialize_chunks output [0, 10, 25]
        append_chunk_segment(&mut all_data, &mut all_indices, &[0u8; 25], &[0, 10, 25]);
        assert_eq!(all_data.len(), 25);
        assert_eq!(all_indices, vec![0, 10, 25]);

        // Second segment: [0, 8, 20] — leading 0 should be skipped, offsets rebased by 25
        append_chunk_segment(&mut all_data, &mut all_indices, &[1u8; 20], &[0, 8, 20]);
        assert_eq!(all_data.len(), 45);
        assert_eq!(all_indices, vec![0, 10, 25, 33, 45]);

        // Third segment: single chunk [0, 5] — leading 0 skipped, rebased by 45
        append_chunk_segment(&mut all_data, &mut all_indices, &[2u8; 5], &[0, 5]);
        assert_eq!(all_data.len(), 50);
        assert_eq!(all_indices, vec![0, 10, 25, 33, 45, 50]);
    }

    #[test]
    fn test_append_chunk_segment_single() {
        let mut all_data = Vec::new();
        let mut all_indices = Vec::<u32>::new();

        append_chunk_segment(&mut all_data, &mut all_indices, &[0u8; 10], &[0, 10]);
        assert_eq!(all_data.len(), 10);
        assert_eq!(all_indices, vec![0, 10]);
    }

    #[test]
    fn test_truncated_stream_returns_error() {
        let (_, xorb_data, _, _) = build_xorb_object(3, ChunkSize::Fixed(1024), CompressionScheme::None);

        // Truncate mid-header (e.g. 2 bytes into the second chunk's header)
        let first_chunk_end = XORB_CHUNK_HEADER_LENGTH + 1024;
        let mid_header = first_chunk_end + 2;
        let truncated = &xorb_data[..mid_header];
        let res = deserialize_chunks_to_writer(&mut Cursor::new(truncated), &mut Vec::new());
        assert!(res.is_err(), "truncation mid-header should error, not silently succeed");

        // Truncate mid-data (header present but compressed payload cut short)
        let mid_data = first_chunk_end + XORB_CHUNK_HEADER_LENGTH + 10;
        let truncated = &xorb_data[..mid_data];
        let res = deserialize_chunks_to_writer(&mut Cursor::new(truncated), &mut Vec::new());
        assert!(res.is_err(), "truncation mid-data should error, not silently succeed");
    }

    #[test]
    fn test_exact_eof_after_complete_chunk_succeeds() {
        let (_, xorb_data, raw_data, raw_chunk_boundaries) =
            build_xorb_object(3, ChunkSize::Fixed(1024), CompressionScheme::None);

        // Truncate exactly at the end of chunk 0. This should be treated as clean EOF.
        let first_chunk_end = XORB_CHUNK_HEADER_LENGTH + 1024;
        let truncated = &xorb_data[..first_chunk_end];

        let mut out = Vec::new();
        let (num_read, chunk_byte_indices) =
            deserialize_chunks_to_writer(&mut Cursor::new(truncated), &mut out).unwrap();

        assert_eq!(num_read, first_chunk_end);
        assert_eq!(chunk_byte_indices, vec![0, raw_chunk_boundaries[0].1]);
        assert_eq!(&out[..], &raw_data[..1024]);
    }
}
