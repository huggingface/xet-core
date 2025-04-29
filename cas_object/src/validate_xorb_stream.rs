use std::mem::{size_of, size_of_val};

use anyhow::anyhow;
use error_printer::ErrorPrinter;
use futures::{AsyncRead, AsyncReadExt};
use merkledb::prelude::MerkleDBHighLevelMethodsV1;
use merkledb::{Chunk, MerkleMemDB};
use merklehash::MerkleHash;

use crate::cas_object_format::CAS_OBJECT_FORMAT_IDENT;
use crate::error::{CasObjectError, Result, Validate};
use crate::{
    parse_chunk_header, CASChunkHeader, CasObject, CasObjectInfoV1, CAS_OBJECT_FORMAT_VERSION,
    CAS_OBJECT_FORMAT_VERSION_V0,
};

/// takes an async reader to the entire xorb data and validates that the xorb is correctly formatted
/// and returns the deserialized CasObject (metadata)
///
/// if either the hash stored in the metadata section of the validated xorb or the computed hash
/// do not match the provided hash, this function considers the provided xorb invalid.
///
/// returns Ok(None) on a validation error, returns Err() on a real error
/// returns Ok(<CasObject, None>) of no error occurred and the xorb is valid.
/// returns Ok(<CasObject, Some(0)>) if the xorb is missing a footer; in this case
///     a new footer is generated and stored in the returned CasObject, BUT with the
///     "info_length" field set to 0.
/// returns Ok(<CasObject, Some(go_back_bytes)>) if the xorb is v0; in this case
///     a new footer is generated and stored in the returned CasObject, BUT with the
///     "info_length" field set to 0; "go_back_bytes" equals the number of bytes read
///     from the header, i.e. the size of "ident" and "version".
pub async fn validate_cas_object_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
    hash: &MerkleHash,
) -> Result<Option<(CasObject, Option<usize>)>> {
    _validate_cas_object_from_async_read(reader, hash).await.ok_for_format_error()
}

// matches validate_cas_object_from_async_read but returns Err(CasObjectError::FormatError(...)) on
// an invalid xorb
async fn _validate_cas_object_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
    hash: &MerkleHash,
) -> Result<(CasObject, Option<usize>)> {
    let mut compressed_chunk_boundary_offsets: Vec<u32> = Vec::new();
    let mut chunk_hash_and_size: Vec<Chunk> = Vec::new();
    let (maybe_cas_object, go_back_bytes): (Option<CasObject>, Option<usize>) = loop {
        let mut buf8 = [0u8; 8];
        let mut bytes_read = 0;
        while bytes_read < size_of_val(&buf8) {
            let ret = reader.read(&mut buf8[bytes_read..]).await?;
            if ret == 0 {
                break;
            }
            bytes_read += ret;
        }

        if bytes_read == 0 {
            // no more bytes, meaning the client doesn't send the footer, we build a new footer.
            break (None, Some(0));
        }

        if bytes_read != size_of_val(&buf8) {
            // some bytes after valid chunk list, invalid xorb.
            return Err(CasObjectError::FormatError(anyhow!("invalid bytes after chunk list")))
                .log_error(format!("invalid bytes after chunk list: {hash}"));
        }

        if buf8[..CAS_OBJECT_FORMAT_IDENT.len()] == CAS_OBJECT_FORMAT_IDENT {
            let version = buf8[CAS_OBJECT_FORMAT_IDENT.len()..][0];
            if version > CAS_OBJECT_FORMAT_VERSION {
                return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version: {version}")))
                    .log_error(format!("invalid version for xorb: {hash}"));
            }

            if version == CAS_OBJECT_FORMAT_VERSION {
                // try to parse footer
                let cas_object = CasObject::deserialize_async(reader, version)
                    .await
                    .log_error("failed to deserialize footer")?;
                break (Some(cas_object), None);
            } else if version == CAS_OBJECT_FORMAT_VERSION_V0 {
                // we see an old xorb, we build a new footer.
                break (None, Some(size_of_val(&buf8)));
            }
        }

        // parse the chunk header, decompress the data, compute the hash
        let chunk_header = parse_chunk_header(buf8).log_error(format!("failed to parse chunk header {buf8:?}"))?;

        let chunk_compressed_len = chunk_header.get_compressed_length() as usize;
        // compressed length is validated above in chunk header parsing that it
        // will not exceed maximum chunk size * 2.
        let mut compressed_chunk_data = vec![0u8; chunk_compressed_len];
        reader.read_exact(&mut compressed_chunk_data).await.log_error(format!(
            "failed to read {} bytes chunk data at index {}",
            chunk_compressed_len,
            chunk_hash_and_size.len()
        ))?;

        let chunk_uncompressed_expected_len = chunk_header.get_uncompressed_length() as usize;
        let uncompressed_chunk_data = chunk_header
            .get_compression_scheme()?
            .decompress_from_slice(&compressed_chunk_data)
            .log_error(format!("failed to decompress chunk at index {}, xorb {hash}", chunk_hash_and_size.len()))?;

        if chunk_uncompressed_expected_len != uncompressed_chunk_data.len() {
            return Err(CasObjectError::FormatError(anyhow!(
                "chunk at index {} uncompressed len from header: {}, real uncompressed length: {} for xorb: {hash}",
                chunk_hash_and_size.len(),
                chunk_header.get_uncompressed_length(),
                uncompressed_chunk_data.len()
            )))
            .log_error("uncompressed chunk length mismatch");
        }

        let chunk_hash = merklehash::compute_data_hash(&uncompressed_chunk_data);
        chunk_hash_and_size.push(Chunk {
            hash: chunk_hash,
            length: uncompressed_chunk_data.len(),
        });

        // next offset is computed with: previous offset + length of chunk header + chunk compressed_length
        compressed_chunk_boundary_offsets.push(
            compressed_chunk_boundary_offsets.last().unwrap_or(&0)
                + size_of::<CASChunkHeader>() as u32
                + chunk_compressed_len as u32,
        );
    };

    // validating footer against chunks contents
    if let Some(cas_object) = &maybe_cas_object {
        let cas_object_info = &cas_object.info;

        if cas_object_info.cashash != *hash {
            return Err(CasObjectError::FormatError(anyhow!("xorb listed hash does not match provided hash")));
        }

        if cas_object_info.num_chunks as usize != chunk_hash_and_size.len() {
            return Err(CasObjectError::FormatError(anyhow!(
                "xorb metadata lists {} chunks, but {} were deserialized",
                cas_object_info.num_chunks,
                chunk_hash_and_size.len()
            )));
        }

        if cas_object_info.chunk_boundary_offsets != compressed_chunk_boundary_offsets {
            return Err(CasObjectError::FormatError(anyhow!("chunk boundary offsets do not match")));
        }

        if cas_object_info.chunk_hashes.len() != chunk_hash_and_size.len() {
            return Err(CasObjectError::FormatError(anyhow!(
                "xorb footer does not contain as many chunk as are in the xorb"
            )));
        }

        for (parsed, computed_chunk) in cas_object_info.chunk_hashes.iter().zip(chunk_hash_and_size.iter()) {
            if parsed != &computed_chunk.hash {
                return Err(CasObjectError::FormatError(anyhow!(
                    "found chunk hash in xorb footer that does not match the corresponding chunk's computed hash"
                )));
            }
        }

        let mut prefixsum = 0;
        for (parsed, computed_chunk) in cas_object_info.unpacked_chunk_offsets.iter().zip(chunk_hash_and_size.iter()) {
            prefixsum += computed_chunk.length as u32;
            if *parsed != prefixsum {
                return Err(CasObjectError::FormatError(anyhow!(
                    "found unpacked chunk offset in xorb footer that does not match the corresponding chunk's actual unpacked length"
                )));
            }
        }
    }

    // combine hashes to get full xorb hash, compare to provided
    let mut db = MerkleMemDB::default();
    let mut staging = db.start_insertion_staging();
    db.add_file(&mut staging, &chunk_hash_and_size);
    let ret = db.finalize(staging);
    if ret.hash() != hash {
        return Err(CasObjectError::FormatError(anyhow!("xorb computed hash does not match provided hash")));
    }

    let cas_object = maybe_cas_object
        .unwrap_or_else(|| create_cas_object_from_parts(hash, compressed_chunk_boundary_offsets, chunk_hash_and_size));

    Ok((cas_object, go_back_bytes))
}

fn create_cas_object_from_parts(
    hash: &MerkleHash,
    compressed_chunk_boundary_offsets: Vec<u32>,
    chunk_hash_and_size: Vec<Chunk>,
) -> CasObject {
    let mut unpacked_offset = 0;

    let mut cas_info = CasObjectInfoV1::default();
    cas_info.cashash = *hash;
    cas_info.chunk_hashes = chunk_hash_and_size.iter().map(|chunk| chunk.hash).collect();
    cas_info.chunk_boundary_offsets = compressed_chunk_boundary_offsets;
    cas_info.unpacked_chunk_offsets = chunk_hash_and_size
        .iter()
        .map(|chunk| {
            unpacked_offset += chunk.length;
            unpacked_offset as u32
        })
        .collect();
    cas_info.num_chunks = chunk_hash_and_size.len() as u32;
    cas_info.fill_in_boundary_offsets();

    CasObject {
        info: cas_info,
        info_length: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek};
    use std::mem::size_of;
    use std::u8;

    use futures::{AsyncRead, TryStreamExt};
    use rand::RngCore;
    use utils::serialization_utils::write_u32;

    use crate::test_utils::*;
    use crate::{
        validate_cas_object_from_async_read, CasObject, CasObjectInfoV0, CompressionScheme, CAS_OBJECT_FORMAT_VERSION,
        CAS_OBJECT_FORMAT_VERSION_V0,
    };

    const NO_FOOTER_XORB: u8 = u8::MAX;
    const INVALID_FOOTER_XORB: u8 = u8::MAX - 1;

    fn v1_xorb(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
    ) -> (CasObject, Vec<u8>, usize, usize) {
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(num_chunks, chunk_size, compression_scheme);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(compression_scheme),
        )
        .is_ok());

        let footer_length = c.info_length as usize + size_of::<u32>();
        (c, buf.into_inner(), _cas_data.len(), footer_length)
    }

    fn v0_xorb(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
    ) -> (CasObject, Vec<u8>, usize, usize) {
        let (c, cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(num_chunks, chunk_size, compression_scheme);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(compression_scheme),
        )
        .is_ok());

        // Switch V1 footer to V0
        let mut cas_info_v0 = CasObjectInfoV0::default();
        cas_info_v0.cashash = c.info.cashash;
        cas_info_v0.num_chunks = c.info.num_chunks;
        cas_info_v0.chunk_boundary_offsets = c.info.chunk_boundary_offsets.clone();
        cas_info_v0.chunk_hashes = c.info.chunk_hashes.clone();

        let mut buf = buf.into_inner();
        let serialized_chunks_length = c.get_contents_length().unwrap();
        buf.resize(serialized_chunks_length as usize, 0);

        let mut buf = Cursor::new(buf);
        buf.seek(std::io::SeekFrom::End(0)).unwrap();
        #[allow(deprecated)]
        let info_length = cas_info_v0.serialize(&mut buf).unwrap() as u32;
        write_u32(&mut buf, info_length).unwrap();

        let footer_length = info_length as usize + size_of::<u32>();
        (c, buf.into_inner(), cas_data.len(), footer_length)
    }

    fn no_footer_xorb(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
    ) -> (CasObject, Vec<u8>, usize, usize) {
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(num_chunks, chunk_size, compression_scheme);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(compression_scheme),
        )
        .is_ok());

        let mut buf = buf.into_inner();
        let serialized_chunks_length = c.get_contents_length().unwrap();
        buf.resize(serialized_chunks_length as usize, 0);

        (c, buf, serialized_chunks_length as usize, 0)
    }

    fn invalid_footer_xorb(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
    ) -> (CasObject, Vec<u8>, usize, usize) {
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(num_chunks, chunk_size, compression_scheme);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(compression_scheme),
        )
        .is_ok());

        let mut buf = buf.into_inner();
        let serialized_chunks_length = c.get_contents_length().unwrap();
        buf.resize(serialized_chunks_length as usize, 0);

        // Now add some maybe existing but less than 8 bytes
        let mut rng = rand::rng();
        let some_nonzero_size_less_than_8 = rng.next_u32() % 7 + 1;
        let mut some_bytes = vec![0u8; some_nonzero_size_less_than_8 as usize];
        rng.fill_bytes(&mut some_bytes);
        buf.extend_from_slice(&some_bytes);

        (c, buf, serialized_chunks_length as usize, some_bytes.len())
    }

    // Construct a randomly generate XORB
    // Return its metadata, serialized xorb stream, content size (number of bytes before footer)
    // and footer size.
    fn get_xorb(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
        split: usize,
        xorb_version: u8,
    ) -> (CasObject, impl AsyncRead + Unpin, usize, usize) {
        let (c, xorb_bytes, content_size, footer_size) = match xorb_version {
            CAS_OBJECT_FORMAT_VERSION_V0 => v0_xorb(num_chunks, chunk_size, compression_scheme),
            CAS_OBJECT_FORMAT_VERSION => v1_xorb(num_chunks, chunk_size, compression_scheme),
            NO_FOOTER_XORB => no_footer_xorb(num_chunks, chunk_size, compression_scheme),
            INVALID_FOOTER_XORB => invalid_footer_xorb(num_chunks, chunk_size, compression_scheme),
            _ => unimplemented!(),
        };

        let split = xorb_bytes
            .chunks(xorb_bytes.len() / split)
            .map(|c| Ok(c.to_vec()))
            .collect::<Vec<_>>();
        let async_reader = futures::stream::iter(split).into_async_read();
        (c, async_reader, content_size, footer_size)
    }

    #[tokio::test]
    async fn test_validate_xorb() {
        let cases = [
            (1, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::LZ4, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 10),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 100),
            (100, ChunkSize::Random(512, 2048), CompressionScheme::LZ4, 100),
            (1000, ChunkSize::Random(10 << 10, 20 << 10), CompressionScheme::LZ4, 2000), // chunk size 10KiB-20KiB
        ];

        for (i, (num_chunks, chunk_size, compression_scheme, split)) in cases.into_iter().enumerate() {
            let (cas_object, xorb_reader, content_length, footer_length) =
                get_xorb(num_chunks, chunk_size, compression_scheme, split, CAS_OBJECT_FORMAT_VERSION);
            let mut counting_xorb_reader = countio::Counter::new(xorb_reader);
            let validated_result =
                validate_cas_object_from_async_read(&mut counting_xorb_reader, &cas_object.info.cashash).await;
            assert!(validated_result.is_ok());
            let validated_option = validated_result.unwrap();
            assert!(validated_option.is_some());
            let (validated, go_back_bytes) = validated_option.unwrap();
            assert!(go_back_bytes.is_none());
            assert_eq!(validated, cas_object, "failed to match footers, iter {i}");
            assert_eq!(counting_xorb_reader.reader_bytes(), content_length + footer_length)
        }
    }

    #[tokio::test]
    async fn test_validate_v0_xorb() {
        let cases = [
            (1, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::LZ4, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 10),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 100),
            (100, ChunkSize::Random(512, 2048), CompressionScheme::LZ4, 100),
            (1000, ChunkSize::Random(10 << 10, 20 << 10), CompressionScheme::LZ4, 2000), // chunk size 10KiB-20KiB
        ];

        for (i, (num_chunks, chunk_size, compression_scheme, split)) in cases.into_iter().enumerate() {
            let (cas_object, xorb_reader, content_length, _footer_length) =
                get_xorb(num_chunks, chunk_size, compression_scheme, split, CAS_OBJECT_FORMAT_VERSION_V0);
            let mut counting_xorb_reader = countio::Counter::new(xorb_reader);
            let validated_result =
                validate_cas_object_from_async_read(&mut counting_xorb_reader, &cas_object.info.cashash).await;
            assert!(validated_result.is_ok());
            let validated_option = validated_result.unwrap();
            assert!(validated_option.is_some());
            let (validated, go_back_bytes) = validated_option.unwrap();
            assert_eq!(go_back_bytes, Some(8));
            assert_eq!(validated.info_length, 0);
            assert_eq!(validated.info, cas_object.info, "failed to match footers, iter {i}");
            // only read up to ident and version in CasObjectInfoV0
            assert_eq!(counting_xorb_reader.reader_bytes(), content_length + 8);
        }
    }

    #[tokio::test]
    async fn test_validate_xorb_without_footer() {
        let cases = [
            (1, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::LZ4, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 10),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 100),
            (100, ChunkSize::Random(512, 2048), CompressionScheme::LZ4, 100),
            (1000, ChunkSize::Random(10 << 10, 20 << 10), CompressionScheme::LZ4, 2000), // chunk size 10KiB-20KiB
        ];

        for (i, (num_chunks, chunk_size, compression_scheme, split)) in cases.into_iter().enumerate() {
            let (cas_object, xorb_reader, content_length, footer_length) =
                get_xorb(num_chunks, chunk_size, compression_scheme, split, NO_FOOTER_XORB);
            let mut counting_xorb_reader = countio::Counter::new(xorb_reader);
            let validated_result =
                validate_cas_object_from_async_read(&mut counting_xorb_reader, &cas_object.info.cashash).await;
            assert!(validated_result.is_ok());
            let validated_option = validated_result.unwrap();
            assert!(validated_option.is_some());
            let (validated, go_back_bytes) = validated_option.unwrap();
            assert_eq!(go_back_bytes, Some(0));
            assert_eq!(validated.info_length, 0);
            assert_eq!(validated.info, cas_object.info, "failed to match footers, iter {i}");
            // only read up to the last byte of chunk list
            assert_eq!(footer_length, 0);
            assert_eq!(counting_xorb_reader.reader_bytes(), content_length);
        }
    }

    #[tokio::test]
    async fn test_validate_xorb_with_invalid_footer() {
        let cases = [
            (1, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::LZ4, 1),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 10),
            (100, ChunkSize::Fixed(1000), CompressionScheme::None, 100),
            (100, ChunkSize::Random(512, 2048), CompressionScheme::LZ4, 100),
            (1000, ChunkSize::Random(10 << 10, 20 << 10), CompressionScheme::LZ4, 2000), // chunk size 10KiB-20KiB
        ];

        for (_i, (num_chunks, chunk_size, compression_scheme, split)) in cases.into_iter().enumerate() {
            let (cas_object, xorb_reader, _content_length, _footer_length) =
                get_xorb(num_chunks, chunk_size, compression_scheme, split, INVALID_FOOTER_XORB);
            let mut counting_xorb_reader = countio::Counter::new(xorb_reader);
            let validated_result =
                validate_cas_object_from_async_read(&mut counting_xorb_reader, &cas_object.info.cashash).await;
            assert!(validated_result.is_ok());
            let validated_option = validated_result.unwrap();
            assert!(validated_option.is_none());
        }
    }
}
