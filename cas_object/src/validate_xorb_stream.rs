use std::mem::size_of;

use anyhow::anyhow;
use error_printer::ErrorPrinter;
use futures::{AsyncRead, AsyncReadExt};
use merkledb::prelude::MerkleDBHighLevelMethodsV1;
use merkledb::{Chunk, MerkleMemDB};
use merklehash::MerkleHash;

use crate::cas_object_format::CAS_OBJECT_FORMAT_IDENT;
use crate::error::{CasObjectError, Result, Validate};
use crate::{parse_chunk_header, CASChunkHeader, CasObject};

/// takes an async reader to the entire xorb data and validates that the xorb is correctly formatted
/// and returns the deserialized CasObject (metadata)
///
/// if either the hash stored in the metadata section of the validated xorb or the computed hash
/// do not match the provided hash, this function considers the provided xorb invalid.
///
/// returns Ok(None) on a validation error, returns Err() on a real error
/// returns Ok(<CasObject>) of no error occurred and the xorb is valid.
pub async fn validate_cas_object_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
    hash: &MerkleHash,
) -> Result<Option<CasObject>> {
    _validate_cas_object_from_async_read(reader, hash).await.ok_for_format_error()
}

// matches validate_cas_object_from_async_read but returns Err(CasObjectError::FormatError(...)) on
// an invalid xorb
async fn _validate_cas_object_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
    hash: &MerkleHash,
) -> Result<CasObject> {
    let mut chunk_boundary_offsets: Vec<u32> = Vec::new();
    let mut hash_chunks: Vec<Chunk> = Vec::new();
    let cas_object: CasObject = loop {
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8).await?;
        if buf8[..CAS_OBJECT_FORMAT_IDENT.len()] == CAS_OBJECT_FORMAT_IDENT {
            let version = buf8[CAS_OBJECT_FORMAT_IDENT.len()..][0];
            if version != crate::cas_object_format::CAS_OBJECT_FORMAT_VERSION {
                return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version: {version}")))
                    .log_error(format!("invalid version for xorb: {hash}"));
            }
            // try to parse footer
            let cas_object = CasObject::deserialize_async(reader, version)
                .await
                .log_error("failed to deserialize footer")?;
            break cas_object;
        }

        // parse the chunk header, decompress the data, compute the hash
        let chunk_header = parse_chunk_header(buf8).log_error(format!("failed to parse chunk header {buf8:?}"))?;

        let chunk_compressed_len = chunk_header.get_compressed_length() as usize;
        let mut compressed_chunk_data = vec![0u8; chunk_compressed_len];
        reader.read_exact(&mut compressed_chunk_data).await.log_error(format!(
            "failed to read {} bytes chunk data at index {}",
            chunk_compressed_len,
            hash_chunks.len()
        ))?;

        let chunk_uncompressed_expected_len = chunk_header.get_uncompressed_length() as usize;
        let uncompressed_chunk_data = chunk_header
            .get_compression_scheme()?
            .decompress_from_slice(&compressed_chunk_data)
            .log_error(format!("failed to decompress chunk at index {}, xorb {hash}", hash_chunks.len()))?;

        if chunk_uncompressed_expected_len != uncompressed_chunk_data.len() {
            return Err(CasObjectError::FormatError(anyhow!(
                "chunk at index {} uncompressed len from header: {}, real uncompressed length: {} for xorb: {hash}",
                hash_chunks.len(),
                chunk_header.get_uncompressed_length(),
                uncompressed_chunk_data.len()
            )))
            .log_error("uncompressed chunk length mismatch");
        }

        let chunk_hash = merklehash::compute_data_hash(&uncompressed_chunk_data);
        hash_chunks.push(Chunk {
            hash: chunk_hash,
            length: uncompressed_chunk_data.len(),
        });

        // next offset is computed with: previous offset + length of chunk header + chunk compressed_length
        chunk_boundary_offsets.push(
            chunk_boundary_offsets.last().unwrap_or(&0)
                + size_of::<CASChunkHeader>() as u32
                + chunk_compressed_len as u32,
        );
    };

    // validating footer against chunks contents
    let cas_object_info = &cas_object.info;

    if cas_object_info.cashash != *hash {
        return Err(CasObjectError::FormatError(anyhow!("xorb listed hash does not match provided hash")));
    }

    if cas_object_info.num_chunks as usize != hash_chunks.len() {
        return Err(CasObjectError::FormatError(anyhow!(
            "xorb metadata lists {} chunks, but {} were deserialized",
            cas_object_info.num_chunks,
            hash_chunks.len()
        )));
    }

    if chunk_boundary_offsets != cas_object_info.chunk_boundary_offsets {
        return Err(CasObjectError::FormatError(anyhow!("chunk boundary offsets do not match")));
    }

    if cas_object_info.chunk_hashes.len() != hash_chunks.len() {
        return Err(CasObjectError::FormatError(anyhow!(
            "xorb footer does not contain as many chunk as are in the xorb"
        )));
    }

    for (parsed, computed_chunk) in cas_object_info.chunk_hashes.iter().zip(hash_chunks.iter()) {
        if parsed != &computed_chunk.hash {
            return Err(CasObjectError::FormatError(anyhow!(
                "found chunk hash in xorb footer that does not match the corresponding chunk's computed hash"
            )));
        }
    }

    // 4. combine hashes to get full xorb hash, compare to provided
    let mut db = MerkleMemDB::default();
    let mut staging = db.start_insertion_staging();
    db.add_file(&mut staging, &hash_chunks);
    let ret = db.finalize(staging);
    if ret.hash() != hash {
        return Err(CasObjectError::FormatError(anyhow!("xorb computed hash does not match provided hash")));
    }

    Ok(cas_object)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use futures::{AsyncRead, TryStreamExt};

    use crate::test_utils::*;
    use crate::{validate_cas_object_from_async_read, CasObject, CompressionScheme};

    fn get_xorb(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
        split: usize,
    ) -> (CasObject, impl AsyncRead + Unpin) {
        // Arrange
        let (c, _cas_data, raw_data, raw_chunk_boundaries) =
            build_cas_object(num_chunks, chunk_size, compression_scheme);
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Act & Assert
        assert!(CasObject::serialize(
            &mut buf,
            &c.info.cashash,
            &raw_data,
            &raw_chunk_boundaries,
            Some(compression_scheme),
        )
        .is_ok());

        let xorb_bytes = buf.into_inner();
        let split = xorb_bytes
            .chunks(xorb_bytes.len() / split)
            .map(|c| Ok(c.to_vec()))
            .collect::<Vec<_>>();
        let async_reader = futures::stream::iter(split).into_async_read();
        (c, async_reader)
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
            let (cas_object, mut xorb_reader) = get_xorb(num_chunks, chunk_size, compression_scheme, split);
            let validated_result =
                validate_cas_object_from_async_read(&mut xorb_reader, &cas_object.info.cashash).await;
            assert!(validated_result.is_ok());
            let validated_option = validated_result.unwrap();
            assert!(validated_option.is_some());
            let validated = validated_option.unwrap();
            assert_eq!(validated, cas_object, "failed to match footers, iter {i}");
        }
    }
}
