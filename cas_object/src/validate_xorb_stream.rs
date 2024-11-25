use anyhow::anyhow;
use merkledb::prelude::MerkleDBHighLevelMethodsV1;
use merkledb::{Chunk, MerkleMemDB};
use merklehash::MerkleHash;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::cas_chunk_format::decompress_chunk_to_writer;
use crate::cas_object_format::CAS_OBJECT_FORMAT_IDENT;
use crate::error::{CasObjectError, Result, Validate};
use crate::{parse_chunk_header, CasObjectInfo};

// returns Ok(None) on a validation error, returns Err() on a real error
pub async fn validate_cas_object_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
    hash: &MerkleHash,
) -> Result<Option<(CasObjectInfo, Vec<Vec<u8>>, Vec<u32>)>> {
    _validate_cas_object_from_async_read(reader, hash).await.ok_for_format_error()
}

async fn _validate_cas_object_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
    hash: &MerkleHash,
) -> Result<(CasObjectInfo, Vec<Vec<u8>>, Vec<u32>)> {
    let mut chunks = Vec::new();
    let mut indices = vec![0];
    let mut hash_chunks: Vec<Chunk> = Vec::new();
    let cas_object_info: CasObjectInfo = loop {
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8).await?;
        if buf8[..CAS_OBJECT_FORMAT_IDENT.len()] == CAS_OBJECT_FORMAT_IDENT {
            if buf8[0] != crate::cas_object_format::CAS_OBJECT_FORMAT_VERSION {
                return Err(CasObjectError::FormatError(anyhow!("Xorb Invalid Format Version")));
            }
            // try to parse footer
            let (cas_object_info, _) = CasObjectInfo::deserialize_async(reader).await?;
            break cas_object_info;
        }

        // parse the chunk header, decompress the data, compute the hash
        let chunk_header = parse_chunk_header(buf8)?;
        let mut compressed_chunk_data = Vec::with_capacity(chunk_header.get_compressed_length() as usize);
        reader.read_exact(&mut compressed_chunk_data).await?;
        let mut uncompressed_chunk_data = Vec::with_capacity(chunk_header.get_uncompressed_length() as usize);
        decompress_chunk_to_writer(chunk_header, &mut compressed_chunk_data, &mut uncompressed_chunk_data)?;
        let chunk_hash = merklehash::compute_data_hash(&uncompressed_chunk_data);
        hash_chunks.push(Chunk {
            hash: chunk_hash,
            length: uncompressed_chunk_data.len(),
        });

        indices.push(compressed_chunk_data.len() as u32);
        chunks.push(uncompressed_chunk_data);
    };
    if cas_object_info.cashash != *hash {
        return Err(CasObjectError::FormatError(anyhow!("xorb listed hash does not match provided hash")));
    }

    // 4. combine hashes to get full xorb hash, compare to provided
    let mut db = MerkleMemDB::default();
    let mut staging = db.start_insertion_staging();
    db.add_file(&mut staging, &hash_chunks);
    let ret = db.finalize(staging);
    if ret.hash() != hash {
        return Err(CasObjectError::FormatError(anyhow!("xorb computed hash does not match provided hash")));
    }
    Ok((cas_object_info, chunks, indices))
}
