use std::mem::size_of;

use anyhow::anyhow;
use error_printer::ErrorPrinter;
use futures::{AsyncRead, AsyncReadExt};
use merkledb::prelude::MerkleDBHighLevelMethodsV1;
use merkledb::{Chunk, MerkleMemDB};
use merklehash::MerkleHash;

use crate::cas_chunk_format::decompress_chunk_to_writer;
use crate::cas_object_format::CAS_OBJECT_FORMAT_IDENT;
use crate::error::{CasObjectError, Result, Validate};
use crate::{parse_chunk_header, CASChunkHeader, CasObjectInfo};

// returns Ok(false) on a validation error, returns Err() on a real error
// returns Ok(true) if no error occurred and the xorb is valid.
pub async fn validate_cas_object_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
    hash: &MerkleHash,
) -> Result<bool> {
    _validate_cas_object_from_async_read(reader, hash)
        .await
        .ok_for_format_error()
        .map(|o| o.is_some())
}

async fn _validate_cas_object_from_async_read<R: AsyncRead + Unpin>(reader: &mut R, hash: &MerkleHash) -> Result<()> {
    let mut chunk_boundary_offsets: Vec<u32> = Vec::new();
    let mut hash_chunks: Vec<Chunk> = Vec::new();
    let cas_object_info: CasObjectInfo = loop {
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8).await?;
        if buf8[..CAS_OBJECT_FORMAT_IDENT.len()] == CAS_OBJECT_FORMAT_IDENT {
            let version = buf8[CAS_OBJECT_FORMAT_IDENT.len()..][0];
            if version != crate::cas_object_format::CAS_OBJECT_FORMAT_VERSION {
                return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version")));
            }
            // try to parse footer
            let (cas_object_info, _) = CasObjectInfo::deserialize_async(reader, version).await?;
            break cas_object_info;
        }

        // parse the chunk header, decompress the data, compute the hash
        let chunk_header = parse_chunk_header(buf8).log_error(format!("failed to parse chunk header {buf8:?}"))?;

        let chunk_compressed_len = chunk_header.get_compressed_length() as usize;
        let mut compressed_chunk_data = vec![0u8; chunk_compressed_len];
        reader.read_exact(&mut compressed_chunk_data).await?;

        let chunk_uncompressed_expected_len = chunk_header.get_uncompressed_length() as usize;
        let mut uncompressed_chunk_data = Vec::with_capacity(chunk_uncompressed_expected_len);
        decompress_chunk_to_writer(chunk_header, &mut compressed_chunk_data, &mut uncompressed_chunk_data)?;

        if chunk_uncompressed_expected_len != uncompressed_chunk_data.len() {
            return Err(CasObjectError::FormatError(anyhow!(
                "chunk at index {} uncompressed len from header: {}, real uncompressed length: {} for xorb: {hash}",
                hash_chunks.len(),
                chunk_header.get_uncompressed_length(),
                uncompressed_chunk_data.len()
            )));
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

    Ok(())
}
