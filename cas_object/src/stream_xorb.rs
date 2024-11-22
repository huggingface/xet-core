use anyhow::anyhow;
use tokio::io::{AsyncRead, AsyncReadExt};
use crate::{CasObject, CompressionScheme};
use crate::cas_chunk_format::decompress_chunk_to_writer;
use crate::error::CasObjectError;

pub async fn validate_cas_object_from_async_read(reader: &mut impl AsyncRead + Unpin) -> Result<(CasObject, Vec<u8>, Vec<u32>), CasObjectError> {
    let mut chunks = Vec::new();
    let mut indices = vec![0];
    loop {
        let chunk_header = crate::async_deserialize::deserialize_chunk_header(reader).await?;
        // TODO determine if 'B' is error or special case
        let compression_scheme = chunk_header.get_compression_scheme()?;
        if compression_scheme == CompressionScheme::InvalidB {
            // try to deserialize the footer
            todo!();
        }
        let mut compressed_chunk_data = vec![0u8; chunk_header.get_compressed_length() as usize];
        reader.read_exact(&mut compressed_chunk_data).await?;
        let mut uncompressed_chunk_data = vec![0u8; compressed_chunk_data.len()];
        decompress_chunk_to_writer(chunk_header, &mut compressed_chunk_data, &mut uncompressed_chunk_data)?;

        indices.push(compressed_chunk_data.len() as u32);
        chunks.push(uncompressed_chunk_data);
    }
}

