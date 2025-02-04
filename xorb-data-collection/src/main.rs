use aws_config::Region;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use utils::serialization_utils::{read_hash, read_u32, write_hash, write_u32};

struct XorbEntry {
    hash: MerkleHash,
    chunks: Vec<ChunkMD>,
}

struct ChunkMD {
    hash: MerkleHash,
    uncompressed_len: u32,
    compressed_len: u32,
}

impl XorbEntry {
    fn serialize(&self, w: &mut impl Write) {
        write_hash(w, &self.hash).unwrap();
        write_u32(w, self.chunks.len() as u32).unwrap();
        for chunk in self.chunks {
            write_hash(w, &chunk.hash).unwrap();
            write_u32(w, chunk.uncompressed_len).unwrap();
            write_u32(w, chunk.compressed_len).unwrap();
        }
    }

    fn deserialize(&self, r: &mut impl Read) -> Self {
        let hash = read_hash(r).unwrap();
        let num_chunks = read_u32(r).unwrap();
        let mut chunks = Vec::with_capacity(num_chunks as usize);
        for _ in 0..num_chunks {
            let hash = read_hash(r).unwrap();
            let uncompressed_len = read_u32(r).unwrap();
            let compressed_len = read_u32(r).unwrap();
            chunks.push(ChunkMD {
                hash,
                uncompressed_len,
                compressed_len,
            });
        }
        Self { hash, chunks }
    }
}

const BUCKET: &str = "xethub-poc-xorb-bucket";
const XORBS_PREFIX: &str = "xorbs/default/";

#[tokio::main]
async fn main() {
    // List xorbs from s3
    let sdk_config = aws_config::from_env().region(Region::new("us-east-1")).load().await;
    let client = Arc::new(aws_sdk_s3::Client::from_conf(aws_sdk_s3::Config::from(&sdk_config)));

    let result = client.list_objects().bucket(BUCKET).prefix(XORBS_PREFIX).send().await.unwrap();
    result.next_marker

    // let xorb_result =
}
