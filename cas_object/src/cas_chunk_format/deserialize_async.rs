use std::io::{Read, Write};
use std::mem::size_of;
use std::slice;

use anyhow::anyhow;
use bytes::{Buf, Bytes};
use futures::Stream;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc::Sender;
use tokio::task::{spawn_blocking, JoinHandle};
use tokio_util::io::{StreamReader, SyncIoBridge};

use crate::error::CasObjectError;
use crate::{CASChunkHeader, CAS_CHUNK_HEADER_LENGTH};

pub async fn deserialize_chunk_header<R: AsyncRead + Unpin>(reader: &mut R) -> Result<CASChunkHeader, CasObjectError> {
    let mut result = CASChunkHeader::default();
    unsafe {
        let buf = slice::from_raw_parts_mut(&mut result as *mut _ as *mut u8, size_of::<CASChunkHeader>());
        reader.read_exact(buf).await?;
    }
    result.validate()?;
    Ok(result)
}

/// Returns the compressed chunk size along with the uncompressed chunk size as a tuple, (compressed, uncompressed)
pub async fn deserialize_chunk_to_writer<R: AsyncRead + Unpin, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, u32), CasObjectError> {
    let header = deserialize_chunk_header(reader).await?;
    let mut compressed_buf = vec![0u8; header.get_compressed_length() as usize];
    reader.read_exact(&mut compressed_buf).await?;

    let uncompressed_len = super::decompress_chunk_to_writer(header, &mut compressed_buf, writer)?;

    if uncompressed_len != header.get_uncompressed_length() {
        return Err(CasObjectError::FormatError(anyhow!(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header"
        )));
    }

    Ok((header.get_compressed_length() as usize + CAS_CHUNK_HEADER_LENGTH, uncompressed_len))
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
    chunk_byte_indices.push(num_compressed_written as u32);

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

pub fn deserialize_chunk_header_stream<R: Read>(reader: &mut R) -> Result<CASChunkHeader, CasObjectError> {
    let mut result = CASChunkHeader::default();
    unsafe {
        let buf = slice::from_raw_parts_mut(&mut result as *mut _ as *mut u8, size_of::<CASChunkHeader>());
        reader.read_exact(buf)?;
    }
    result.validate()?;
    Ok(result)
}

/// Returns the compressed chunk size along with the uncompressed chunk size as a tuple, (compressed, uncompressed)
pub fn deserialize_chunk_to_writer_stream<R: Read>(reader: &mut R) -> Result<Bytes, CasObjectError> {
    let header = deserialize_chunk_header_stream(reader)?;
    let mut compressed_buf = vec![0u8; header.get_compressed_length() as usize];
    reader.read_exact(&mut compressed_buf)?;
    let mut uncompressed_buf = vec![0u8; header.get_uncompressed_length() as usize];

    let uncompressed_len = super::decompress_chunk_to_writer(header, &mut compressed_buf, &mut uncompressed_buf)?;

    if uncompressed_len != header.get_uncompressed_length() {
        return Err(CasObjectError::FormatError(anyhow!(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header"
        )));
    }

    Ok(Bytes::from(uncompressed_buf))
}

/// Deserialize chunks one by one and write them to the writer, one message contains one full chunk
/// Returns the uncompressed chunks size
pub fn deserialize_chunks_to_writer_from_async_read_stream<R: AsyncRead + Unpin + Send + 'static>(
    reader: R,
    writer: Sender<Result<Bytes, CasObjectError>>,
    start_offset: Option<u64>,
    end_offset: Option<u64>,
) -> JoinHandle<()> {
    let mut reader = SyncIoBridge::new(reader);
    let mut start_offset = start_offset.unwrap_or(0);
    let mut end_offset = end_offset.unwrap_or(std::u64::MAX);

    // The deserialization of the chunks is done synchronously and thus needs to happen on a blocking thread
    // Moreover we expect to return from this function right away to be able to read the other end of the stream
    spawn_blocking(move || {
        let mut uncompressed_len = 0;
        loop {
            match deserialize_chunk_to_writer_stream(&mut reader) {
                Ok(uncompressed_bytes) => {
                    let uncompressed_bytes = if start_offset >= uncompressed_bytes.len() as u64 {
                        // Skip this chunk, it's entirely before the start offset
                        start_offset -= uncompressed_bytes.len() as u64;
                        continue;
                    } else if start_offset > 0 {
                        // Skip the part of the chunk before the start offset
                        let offset = start_offset as usize;
                        start_offset = 0;
                        uncompressed_bytes.slice(offset..)
                    } else if end_offset < uncompressed_len + uncompressed_bytes.len() as u64 {
                        // Skip the part of the chunk after the end offset
                        let offset = (end_offset - uncompressed_len) as usize;
                        end_offset = 0;
                        uncompressed_bytes.slice(..offset)
                    } else {
                        uncompressed_bytes
                    };

                    uncompressed_len += uncompressed_bytes.len() as u64;
                    if writer.blocking_send(Ok(uncompressed_bytes)).is_err() {
                        // Other end of the channel is closed, we can return
                        break;
                    }
                    if end_offset == 0 {
                        break;
                    }
                },
                Err(CasObjectError::InternalIOError(e)) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    let _ = writer.blocking_send(Err(CasObjectError::InternalIOError(e)));
                    break;
                },
                Err(e) => {
                    let _ = writer.blocking_send(Err(e));
                    break;
                },
            }
        }
    })
}

pub async fn deserialize_chunks_to_writer_from_stream<B, E, S, W>(
    stream: S,
    writer: &mut W,
) -> Result<(usize, Vec<u32>), CasObjectError>
where
    B: Buf,
    E: Into<std::io::Error>,
    S: Stream<Item = Result<B, E>> + Unpin,
    W: Write,
{
    let mut stream_reader = StreamReader::new(stream);
    deserialize_chunks_to_writer_from_async_read(&mut stream_reader, writer).await
}

pub async fn deserialize_chunks_from_stream<B, E, S>(stream: S) -> Result<(Vec<u8>, Vec<u32>), CasObjectError>
where
    B: Buf,
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
    use rand::{thread_rng, Rng};

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
            serialize_chunk(&data, &mut out, compression_scheme).unwrap();
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
            .chunks(data.len() / (2 + rng.gen::<usize>() % 8))
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
        let rng = &mut thread_rng();
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
