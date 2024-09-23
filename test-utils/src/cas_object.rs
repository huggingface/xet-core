use std::io::{Cursor, Write};

use cas_object::{serialize_chunk, CasChunkInfo, CasObject, CompressionScheme};
use merklehash::compute_data_hash;
use rand::Rng;

pub fn gen_random_bytes(uncompressed_chunk_size: u32) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; uncompressed_chunk_size as usize];
    rng.fill(&mut data[..]);
    data
}

fn build_cas_object(
    num_chunks: u32,
    uncompressed_chunk_size: u32,
    use_random_chunk_size: bool,
    compression_scheme: CompressionScheme,
) -> (CasObject, Vec<u8>, Vec<u8>) {
    let mut c = CasObject::default();

    let mut chunk_size_info = Vec::<CasChunkInfo>::new();
    let mut writer = Cursor::new(Vec::<u8>::new());

    let mut total_bytes = 0;
    let mut uncompressed_bytes: u32 = 0;

    let mut data_contents_raw =
        Vec::<u8>::with_capacity(num_chunks as usize * uncompressed_chunk_size as usize);

    for _idx in 0..num_chunks {
        let chunk_size: u32 = if use_random_chunk_size {
            let mut rng = rand::thread_rng();
            rng.gen_range(512..=uncompressed_chunk_size)
        } else {
            uncompressed_chunk_size
        };

        let bytes = gen_random_bytes(chunk_size);
        let len: u32 = bytes.len() as u32;

        data_contents_raw.extend_from_slice(&bytes);

        // build chunk, create ChunkInfo and keep going

        let bytes_written = serialize_chunk(&bytes, &mut writer, compression_scheme).unwrap();

        let chunk_info = CasChunkInfo {
            start_byte_index: total_bytes,
            cumulative_uncompressed_len: uncompressed_bytes + len,
        };

        chunk_size_info.push(chunk_info);
        total_bytes += bytes_written as u32;
        uncompressed_bytes += len;
    }

    let chunk_info = CasChunkInfo {
        start_byte_index: total_bytes,
        cumulative_uncompressed_len: uncompressed_bytes,
    };
    chunk_size_info.push(chunk_info);

    c.info.num_chunks = chunk_size_info.len() as u32;

    c.info.cashash = compute_data_hash(writer.get_ref());
    c.info.chunk_size_info = chunk_size_info;

    // now serialize info to end Xorb length
    let len = c.info.serialize(&mut writer).unwrap();
    c.info_length = len as u32;

    writer.write_all(&c.info_length.to_le_bytes()).unwrap();

    (c, writer.get_ref().to_vec(), data_contents_raw)
}

const DEFAULT_NUM_CHUNKS: u32 = 100;
const DEFAULT_UNCOMPRESSED_CHUNK_SIZE: u32 = 1000;
const DEFAULT_COMPRESSION_SCHEME: CompressionScheme = CompressionScheme::None;

#[derive(Debug, Clone)]
pub struct TestCasObjectBuilder {
    num_chunks: u32,
    uncompressed_chunk_size: u32,
    use_random_chunk_size: bool,
    compression_scheme: CompressionScheme,
}

impl Default for TestCasObjectBuilder {
    fn default() -> Self {
        Self {
            num_chunks: DEFAULT_NUM_CHUNKS,
            uncompressed_chunk_size: DEFAULT_UNCOMPRESSED_CHUNK_SIZE,
            use_random_chunk_size: false,
            compression_scheme: DEFAULT_COMPRESSION_SCHEME,
        }
    }
}

impl TestCasObjectBuilder {
    pub fn build(&self) -> (CasObject, Vec<u8>, Vec<u8>) {
        let TestCasObjectBuilder {
            num_chunks,
            uncompressed_chunk_size,
            use_random_chunk_size,
            compression_scheme,
        } = self;
        build_cas_object(
            *num_chunks,
            *uncompressed_chunk_size,
            *use_random_chunk_size,
            *compression_scheme,
        )
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn num_chunks<T: Into<u32>>(&mut self, num_chunks: T) -> &mut Self {
        self.num_chunks = num_chunks.into();
        self
    }

    pub fn uncompressed_chunk_size<T: Into<u32>>(
        &mut self,
        uncompressed_chunk_size: T,
    ) -> &mut Self {
        self.uncompressed_chunk_size = uncompressed_chunk_size.into();
        self
    }

    pub fn random_chunk_size<T: Into<u32>>(&mut self) -> &mut Self {
        self.use_random_chunk_size = true;
        self
    }

    pub fn compression_scheme(&mut self, compression_scheme: CompressionScheme) -> &mut Self {
        self.compression_scheme = compression_scheme;
        self
    }
}
