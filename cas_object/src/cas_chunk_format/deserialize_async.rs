use std::io::Write;
use std::mem::size_of;

use anyhow::anyhow;
use futures::io::{AsyncRead, AsyncReadExt};
use futures::{Stream, TryStreamExt};

use crate::error::CasObjectError;
use crate::{parse_chunk_header, CASChunkHeader, CAS_CHUNK_HEADER_LENGTH};

pub async fn deserialize_chunk_header<R: AsyncRead + Unpin>(reader: &mut R) -> Result<CASChunkHeader, CasObjectError> {
    let mut buf = [0u8; size_of::<CASChunkHeader>()];
    reader.read_exact(&mut buf).await?;
    parse_chunk_header(buf)
}

/// Returns the compressed chunk size along with the uncompressed chunk size as a tuple, (compressed, uncompressed)
pub async fn deserialize_chunk_to_writer<R: AsyncRead + Unpin, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, u32), CasObjectError> {
    let header = deserialize_chunk_header(reader).await?;
    let mut compressed_data = vec![0u8; header.get_compressed_length() as usize];
    reader.read_exact(&mut compressed_data).await?;

    let uncompressed_data = header.get_compression_scheme()?.decompress_from_slice(&compressed_data)?;
    let uncompressed_len = uncompressed_data.len();

    if uncompressed_len != header.get_uncompressed_length() as usize {
        return Err(CasObjectError::FormatError(anyhow!(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header"
        )));
    }

    writer.write_all(&uncompressed_data)?;

    Ok((header.get_compressed_length() as usize + CAS_CHUNK_HEADER_LENGTH, uncompressed_len as u32))
}

/// deserialize 1 chunk returning a Vec<u8>, the compressed length and the uncompressed length of the chunk
pub async fn deserialize_chunk<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(Vec<u8>, usize, u32), CasObjectError> {
    let mut buf = Vec::new();
    let (compressed_len, uncompressed_len) = deserialize_chunk_to_writer(reader, &mut buf).await?;
    Ok((buf, compressed_len, uncompressed_len))
}

pub async fn deserialize_chunks_to_writer_from_async_read<R: AsyncRead + Unpin, W: Write>(
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
        match deserialize_chunk_to_writer(reader, writer).await {
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

pub async fn deserialize_chunks_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(Vec<u8>, Vec<u32>), CasObjectError> {
    let mut buf = Vec::new();
    let (_, chunk_byte_indices) = deserialize_chunks_to_writer_from_async_read(reader, &mut buf).await?;
    Ok((buf, chunk_byte_indices))
}

pub async fn deserialize_chunks_to_writer_from_stream<B, E, S, W>(
    stream: S,
    writer: &mut W,
) -> Result<(usize, Vec<u32>), CasObjectError>
where
    B: AsRef<[u8]>,
    E: Into<std::io::Error>,
    S: Stream<Item = Result<B, E>> + Unpin,
    W: Write,
{
    let mut stream_reader = stream.map_err(|e| e.into()).into_async_read();
    deserialize_chunks_to_writer_from_async_read(&mut stream_reader, writer).await
}

pub async fn deserialize_chunks_from_stream<B, E, S>(stream: S) -> Result<(Vec<u8>, Vec<u32>), CasObjectError>
where
    B: AsRef<[u8]>,
    E: Into<std::io::Error>,
    S: Stream<Item = Result<B, E>> + Unpin,
{
    let mut buf = Vec::new();
    let (_, chunk_byte_indices) = deserialize_chunks_to_writer_from_stream(stream, &mut buf).await?;
    Ok((buf, chunk_byte_indices))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::Stream;
    use rand::{rng, Rng};

    use crate::deserialize_async::deserialize_chunks_to_writer_from_stream;
    use crate::{serialize_chunk, CompressionScheme};

    fn gen_random_bytes(rng: &mut impl Rng, uncompressed_chunk_size: u32) -> Vec<u8> {
        let mut data = vec![0u8; uncompressed_chunk_size as usize];
        rng.fill(&mut data[..]);
        data
    }

    const CHUNK_SIZE: usize = 1000;

    fn get_chunks(rng: &mut impl Rng, num_chunks: u32, compression_scheme: CompressionScheme) -> Vec<u8> {
        let mut out = Vec::new();
        for _ in 0..num_chunks {
            let data = gen_random_bytes(rng, CHUNK_SIZE as u32);
            serialize_chunk(&data, &mut out, Some(compression_scheme)).unwrap();
        }
        out
    }

    fn get_stream(
        rng: &mut impl Rng,
        num_chunks: u32,
        compression_scheme: CompressionScheme,
    ) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Unpin {
        let data = get_chunks(rng, num_chunks, compression_scheme);
        let it = data
            .chunks(data.len() / (2 + rng.random::<u64>() % 8) as usize)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>();
        futures::stream::iter(it)
    }

    #[tokio::test]
    async fn test_deserialize_multiple_chunks() {
        let cases = [
            (1, CompressionScheme::None),
            (3, CompressionScheme::None),
            (5, CompressionScheme::LZ4),
            (100, CompressionScheme::None),
            (100, CompressionScheme::LZ4),
            (1000, CompressionScheme::LZ4),
        ];
        let rng = &mut rng();
        for (num_chunks, compression_scheme) in cases {
            let stream = get_stream(rng, num_chunks, compression_scheme);
            let mut buf = Vec::new();
            let res = deserialize_chunks_to_writer_from_stream(stream, &mut buf).await;
            assert!(res.is_ok());
            assert_eq!(buf.len(), num_chunks as usize * CHUNK_SIZE);

            // verify that chunk boundaries are correct
            let (data, chunk_byte_indices) = res.unwrap();
            assert!(data > 0);
            assert_eq!(chunk_byte_indices.len(), num_chunks as usize + 1);
            for i in 0..chunk_byte_indices.len() - 1 {
                assert_eq!(chunk_byte_indices[i + 1] - chunk_byte_indices[i], CHUNK_SIZE as u32);
            }
        }
    }
}
