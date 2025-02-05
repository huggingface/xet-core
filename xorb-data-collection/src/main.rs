use aws_config::Region;
use aws_sdk_s3::Client;
use cas_object::{parse_chunk_header, CasObject, CAS_CHUNK_HEADER_LENGTH};
use file_utils::SafeFileCreator;
use futures_util::task::SpawnExt;
use merklehash::MerkleHash;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use utils::serialization_utils::{read_hash, read_u32, write_hash, write_u32};

struct XorbEntry {
    hash: MerkleHash,
    chunks: Vec<ChunkMD>,
}

struct ChunkMD {
    hash: MerkleHash,
    uncompressed_len: u32,
    compressed_len: u32,
    compression_scheme: u8,
}

impl XorbEntry {
    fn serialize(&self, w: &mut impl Write) {
        write_hash(w, &self.hash).unwrap();
        write_u32(w, self.chunks.len() as u32).unwrap();
        for chunk in &self.chunks {
            write_hash(w, &chunk.hash).unwrap();
            write_u32(w, chunk.uncompressed_len).unwrap();
            write_u32(w, chunk.compressed_len).unwrap();
            w.write_all(&[chunk.compression_scheme]).unwrap();
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
            let compression_scheme = {
                let mut _buf = [0u8; 1];
                r.read_exact(&mut _buf).unwrap();
                _buf[0]
            };
            chunks.push(ChunkMD {
                hash,
                uncompressed_len,
                compressed_len,
                compression_scheme,
            });
        }
        Self { hash, chunks }
    }
}

const BUCKET: &str = "xethub-poc-xorb-bucket";
const XORBS_PREFIX: &str = "xorbs/default/";
const FILENAME: &str = "xorb.chunks";

#[tokio::main]
async fn main() {
    println!("started");
    let sdk_config = aws_config::from_env().region(Region::new("us-east-1")).load().await;
    let s3 = Arc::new(Client::from_conf(aws_sdk_s3::Config::from(&sdk_config)));

    let mut js = JoinSet::new();

    let (xmd_send, xmd_recv) = tokio::sync::mpsc::channel(500);
    js.spawn(write_results(xmd_recv));

    let (xkey_send, xkey_recv) = tokio::sync::mpsc::channel(5000);
    js.spawn(list_bucket(s3.clone(), xkey_send));

    js.spawn(gather_xorb_info(s3.clone(), xkey_recv, xmd_send));

    println!("spawned all, waiting");
    js.join_all().await;
}

const NUM_JOBS_CONCURRENT: usize = 1000;

async fn gather_xorb_info(s3: Arc<Client>, mut jobs: Receiver<String>, out: Sender<XorbEntry>) {
    let mut done = false;
    let mut js = JoinSet::new();
    for _ in 0..NUM_JOBS_CONCURRENT {
        if done {
            break;
        }
        if let Some(job) = jobs.recv().await {
            js.spawn(process_job(s3.clone(), job));
        } else {
            done = true;
        }
    }
    while let Some(Ok(entry)) = js.join_next().await {
        out.send(entry).await.unwrap();
        if done {
            continue;
        }
        if let Some(job) = jobs.recv().await {
            js.spawn(process_job(s3.clone(), job));
        } else {
            done = true;
        }
    }
    println!("gather_xorb_info done");
}

async fn process_job(s3: Arc<Client>, job: String) -> XorbEntry {
    let xorb_bytes = s3
        .get_object()
        .bucket(BUCKET)
        .key(&job)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();

    let xorb = {
        let mut rs = Cursor::new(&xorb_bytes);
        CasObject::deserialize(&mut rs).unwrap()
    };

    let hash = MerkleHash::from_hex(&job[job.len() - 64..]).unwrap();
    if xorb.info.num_chunks == 0 {
        println!("skipping xorb {} since it has 0 chunks; {xorb:?}", hash.hex());
    }

    let mut chunks = Vec::with_capacity(xorb.info.num_chunks as usize);
    for (i, chunk_hash) in xorb.info.chunk_hashes.iter().enumerate() {
        let i = i as u32;
        let (chunk_header_offset, _) = xorb.get_byte_offset(i, i + 1).unwrap();
        let chunk_header_offset = chunk_header_offset as usize;
        let chunk_header_bytes = &xorb_bytes[chunk_header_offset..chunk_header_offset + CAS_CHUNK_HEADER_LENGTH];
        let chunk_header = parse_chunk_header(chunk_header_bytes.try_into().unwrap()).unwrap();
        chunks.push(ChunkMD {
            hash: chunk_hash.clone(),
            uncompressed_len: chunk_header.get_uncompressed_length(),
            compressed_len: chunk_header.get_compressed_length(),
            compression_scheme: chunk_header.get_compression_scheme().unwrap() as u8,
        })
    }
    XorbEntry { hash, chunks }
}

const MAX_KEYS: i32 = 1000;

async fn list_bucket(s3: Arc<Client>, send: Sender<String>) {
    println!("begin listing bucket");
    let mut response = s3
        .list_objects_v2()
        .max_keys(MAX_KEYS)
        .bucket(BUCKET)
        .prefix(XORBS_PREFIX)
        .send()
        .await
        .unwrap();
    println!("listed {}, sending...", response.contents().len());
    for key in response.contents() {
        send.send(key.key().unwrap().to_string()).await.unwrap();
    }
    while let Some(next_continuation_token) = response.next_continuation_token() {
        response = s3
            .list_objects_v2()
            .bucket(BUCKET)
            .max_keys(MAX_KEYS)
            .prefix(XORBS_PREFIX)
            .continuation_token(next_continuation_token)
            .send()
            .await
            .unwrap();
        println!("listed {}, sending...", response.contents().len());
        for key in response.contents() {
            send.send(key.key().unwrap().to_string()).await.unwrap();
        }
    }
    println!("list_bucket done");
}

async fn write_results(mut recv: Receiver<XorbEntry>) {
    let mut file = SafeFileCreator::new(FILENAME).unwrap();
    let mut i = 0;
    while let Some(entry) = recv.recv().await {
        println!("writing results for xorb ({i}) {} num chunks({})", entry.hash.hex(), entry.chunks.len());
        entry.serialize(&mut file);
        i += 1;
    }
    println!("write_results done");
}
