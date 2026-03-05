use std::io::Write;
use std::mem::size_of;

use anyhow::anyhow;
use futures::io::{AsyncRead, AsyncReadExt};
use futures::{Stream, TryStreamExt};

use crate::error::XorbObjectError;
use crate::{XORB_CHUNK_HEADER_LENGTH, XorbChunkHeader, parse_chunk_header};

pub async fn deserialize_chunk_header<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<XorbChunkHeader, XorbObjectError> {
    let mut buf = [0u8; size_of::<XorbChunkHeader>()];
    reader.read_exact(&mut buf).await?;
    parse_chunk_header(buf)
}

/// Returns the compressed chunk size along with the uncompressed chunk size as a tuple, (compressed, uncompressed)
pub async fn deserialize_chunk_to_writer<R: AsyncRead + Unpin, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, u32), XorbObjectError> {
    let header = deserialize_chunk_header(reader).await?;
    deserialize_chunk_with_header_to_writer(reader, writer, header).await
}

async fn deserialize_chunk_with_header_to_writer<R: AsyncRead + Unpin, W: Write>(
    reader: &mut R,
    writer: &mut W,
    header: XorbChunkHeader,
) -> Result<(usize, u32), XorbObjectError> {
    let mut compressed_data = vec![0u8; header.get_compressed_length() as usize];
    reader.read_exact(&mut compressed_data).await?;

    let uncompressed_data = header.get_compression_scheme()?.decompress_from_slice(&compressed_data)?;
    let uncompressed_len = uncompressed_data.len();

    if uncompressed_len != header.get_uncompressed_length() as usize {
        return Err(XorbObjectError::FormatError(anyhow!(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header"
        )));
    }

    writer.write_all(&uncompressed_data)?;

    Ok((header.get_compressed_length() as usize + XORB_CHUNK_HEADER_LENGTH, uncompressed_len as u32))
}

/// deserialize 1 chunk returning a Vec<u8>, the compressed length and the uncompressed length of the chunk
pub async fn deserialize_chunk<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(Vec<u8>, usize, u32), XorbObjectError> {
    let mut buf = Vec::new();
    let (compressed_len, uncompressed_len) = deserialize_chunk_to_writer(reader, &mut buf).await?;
    Ok((buf, compressed_len, uncompressed_len))
}

/// Reads the next chunk header from an async reader, returning `None` on clean EOF.
///
/// Uses a single `read()` call to detect EOF (returns 0), then completes
/// any partial header with `read_exact`. An `UnexpectedEof` from `read_exact`
/// means the stream was truncated mid-header.
async fn try_read_chunk_header_async<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<XorbChunkHeader>, XorbObjectError> {
    let mut header_buf = [0u8; XORB_CHUNK_HEADER_LENGTH];
    let n = match AsyncReadExt::read(reader, &mut header_buf).await {
        Ok(0) => return Ok(None),
        Ok(n) => n,
        Err(e) => return Err(XorbObjectError::InternalIOError(e)),
    };
    if n < XORB_CHUNK_HEADER_LENGTH {
        reader.read_exact(&mut header_buf[n..]).await?;
    }
    parse_chunk_header(header_buf).map(Some)
}

pub async fn deserialize_chunks_to_writer_from_async_read<R: AsyncRead + Unpin, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, Vec<u32>), XorbObjectError> {
    let mut num_compressed_written = 0;
    let mut num_uncompressed_written = 0;

    // chunk indices are expected to record the byte indices of uncompressed chunks
    // as they are read from the reader, so start with [0, len(uncompressed chunk 0..n), total length]
    let mut chunk_byte_indices = Vec::<u32>::new();
    chunk_byte_indices.push(num_uncompressed_written);

    while let Some(header) = try_read_chunk_header_async(reader).await? {
        let (delta_written, uncompressed_chunk_len) =
            deserialize_chunk_with_header_to_writer(reader, writer, header).await?;
        num_compressed_written += delta_written;
        num_uncompressed_written += uncompressed_chunk_len;
        chunk_byte_indices.push(num_uncompressed_written);
    }

    Ok((num_compressed_written, chunk_byte_indices))
}

pub async fn deserialize_chunks_from_async_read<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(Vec<u8>, Vec<u32>), XorbObjectError> {
    let mut buf = Vec::new();
    let (_, chunk_byte_indices) = deserialize_chunks_to_writer_from_async_read(reader, &mut buf).await?;
    Ok((buf, chunk_byte_indices))
}

pub async fn deserialize_chunks_to_writer_from_stream<B, E, S, W>(
    stream: S,
    writer: &mut W,
) -> Result<(usize, Vec<u32>), XorbObjectError>
where
    B: AsRef<[u8]>,
    E: Into<std::io::Error>,
    S: Stream<Item = Result<B, E>> + Unpin,
    W: Write,
{
    let mut stream_reader = stream.map_err(|e| e.into()).into_async_read();
    deserialize_chunks_to_writer_from_async_read(&mut stream_reader, writer).await
}

pub async fn deserialize_chunks_from_stream<B, E, S>(stream: S) -> Result<(Vec<u8>, Vec<u32>), XorbObjectError>
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
    use rand::{Rng, rng};

    use crate::deserialize_async::deserialize_chunks_to_writer_from_stream;
    use crate::{CompressionScheme, serialize_chunk};

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

    #[tokio::test]
    async fn test_truncated_stream_returns_error() {
        use crate::XORB_CHUNK_HEADER_LENGTH;

        let rng = &mut rng();
        let data = get_chunks(rng, 3, CompressionScheme::None);

        let first_chunk_end = XORB_CHUNK_HEADER_LENGTH + CHUNK_SIZE;

        // Truncate mid-header
        let mid_header = first_chunk_end + 2;
        let stream = futures::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::copy_from_slice(&data[..mid_header]))]);
        let res = deserialize_chunks_to_writer_from_stream(stream, &mut Vec::new()).await;
        assert!(res.is_err(), "truncation mid-header should error, not silently succeed");

        // Truncate mid-data
        let mid_data = first_chunk_end + XORB_CHUNK_HEADER_LENGTH + 10;
        let stream = futures::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::copy_from_slice(&data[..mid_data]))]);
        let res = deserialize_chunks_to_writer_from_stream(stream, &mut Vec::new()).await;
        assert!(res.is_err(), "truncation mid-data should error, not silently succeed");
    }

    #[tokio::test]
    async fn test_exact_eof_after_complete_chunk_succeeds() {
        use crate::XORB_CHUNK_HEADER_LENGTH;

        let rng = &mut rng();
        let data = get_chunks(rng, 3, CompressionScheme::None);
        let first_chunk_end = XORB_CHUNK_HEADER_LENGTH + CHUNK_SIZE;

        // Truncate exactly at end of first chunk. This should be clean EOF.
        let stream = futures::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::copy_from_slice(
            &data[..first_chunk_end],
        ))]);
        let mut out = Vec::new();
        let (num_read, chunk_byte_indices) = deserialize_chunks_to_writer_from_stream(stream, &mut out).await.unwrap();

        assert_eq!(num_read, first_chunk_end);
        assert_eq!(chunk_byte_indices, vec![0, CHUNK_SIZE as u32]);
        assert_eq!(out.len(), CHUNK_SIZE);
    }
}
