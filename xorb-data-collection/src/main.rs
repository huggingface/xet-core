use aws_config::Region;
use aws_sdk_s3::Client;
use cas_object::{parse_chunk_header, CasObject, CompressionScheme, CAS_CHUNK_HEADER_LENGTH};
use cas_types::HexMerkleHash;
use clap::Parser as _;
use clap_derive::{Args, Parser, Subcommand};
use file_utils::SafeFileCreator;
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Write};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use utils::output_bytes;
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

    fn deserialize(r: &mut impl Read) -> Option<Self> {
        let hash = match read_hash(r) {
            Ok(hash) => hash,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                panic!("{e}");
            },
        };
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
        Some(Self { hash, chunks })
    }
}

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    Collect(CollectArgs),
    Print(PrintArgs),
    Reformat(ReformatArgs),
}

#[derive(Args, Debug, Clone)]
struct ReformatArgs {
    #[clap(long, short)]
    filename: String,
}

#[derive(Args, Debug, Clone)]
struct PrintArgs {
    #[clap(long, short)]
    num: Option<usize>,
    #[clap(long, short)]
    filename: String,
}

#[derive(Args, Debug, Clone)]
struct CollectArgs {
    #[clap(long, short, default_value = "xethub-poc-xorb-bucket")]
    bucket: String,
    #[clap(long, short, default_value = "")]
    prefix: String,
    #[clap(long, short, default_value = "100")]
    num_workers: usize,
}

const BUCKET: &str = "xethub-poc-xorb-bucket";
const XORBS_PREFIX: &str = "xorbs/default/";
const FILENAME: &str = "xorb.chunks";

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    match args.command {
        Command::Collect(collect_args) => collect(collect_args).await,
        Command::Print(print_args) => print(print_args),
        Command::Reformat(reformat_args) => reformat(reformat_args),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct XorbCsv {
    hash: HexMerkleHash,
    id: u32,
    num_chunks: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkCsv {
    hash: HexMerkleHash,
    xorb_id: u32,
    chunk_index: u32,
    uncompressed_len: u32,
    compressed_len: u32,
    compression_scheme: &'static str,
}

#[derive(Debug, Serialize, Deserialize)]
struct AllEncompassingCsv {
    hash: HexMerkleHash,
    xorb_hash: HexMerkleHash,
    chunk_index: u32,
    uncompressed_len: u32,
    compressed_len: u32,
    compression_scheme: &'static str,
}

fn reformat(reformat_args: ReformatArgs) {
    let filename = &reformat_args.filename;
    let file = File::open(filename).unwrap();
    let mut reader = BufReader::new(file);

    let xorbs_csv_file = SafeFileCreator::new(format!("{filename}.xorbs.csv")).unwrap();
    let chunks_csv_file = SafeFileCreator::new(format!("{filename}.chunks.csv")).unwrap();
    let mix_csv_file = SafeFileCreator::new(format!("{filename}.mix.csv")).unwrap();

    let mut xorb_csv_writer = csv::Writer::from_writer(xorbs_csv_file);
    let mut chunks_csv_writer = csv::Writer::from_writer(chunks_csv_file);
    let mut mix_csv_writer = csv::Writer::from_writer(mix_csv_file);

    let mut i: u32 = 0;
    loop {
        if i % 1000 == 0 {
            println!("processing index {i}");
        }
        let Some(entry) = XorbEntry::deserialize(&mut reader) else {
            break;
        };
        xorb_csv_writer
            .serialize(XorbCsv {
                hash: entry.hash.into(),
                id: i,
                num_chunks: entry.chunks.len() as u32,
            })
            .unwrap();
        for (
            chunk_index,
            ChunkMD {
                hash,
                uncompressed_len,
                compressed_len,
                compression_scheme,
            },
        ) in entry.chunks.into_iter().enumerate()
        {
            chunks_csv_writer
                .serialize(ChunkCsv {
                    hash: hash.into(),
                    xorb_id: i,
                    chunk_index: chunk_index as u32,
                    uncompressed_len,
                    compressed_len,
                    compression_scheme: CompressionScheme::try_from(compression_scheme).unwrap().into(),
                })
                .unwrap();
            mix_csv_writer
                .serialize(AllEncompassingCsv {
                    hash: hash.into(),
                    xorb_hash: entry.hash.into(),
                    chunk_index: chunk_index as u32,
                    uncompressed_len,
                    compressed_len,
                    compression_scheme: CompressionScheme::try_from(compression_scheme).unwrap().into(),
                })
                .unwrap();
        }
        i += 1;
    }
}

fn print(print_args: PrintArgs) {
    let file = File::open(print_args.filename).unwrap();
    let mut reader = BufReader::new(file);
    let max = print_args.num;
    let mut i = 0;
    loop {
        let Some(entry) = XorbEntry::deserialize(&mut reader) else {
            break;
        };
        println!("Xorb: {}", entry.hash.hex());
        for (
            chunk_index,
            ChunkMD {
                hash,
                uncompressed_len,
                compressed_len,
                compression_scheme,
            },
        ) in entry.chunks.into_iter().enumerate()
        {
            let compression_scheme = CompressionScheme::try_from(compression_scheme).unwrap();
            let compressed_len = output_bytes(compressed_len as usize);
            let uncompressed_len = output_bytes(uncompressed_len as usize);
            println!("  Chunk {chunk_index}: uncompressed({uncompressed_len}) compressed({compressed_len}) scheme: {compression_scheme} hash({hash})")
        }

        i += 1;
        if let Some(max) = max {
            if i >= max {
                break;
            }
        }
    }
}

async fn collect(collect_args: CollectArgs) {
    println!("started");
    let sdk_config = aws_config::from_env().region(Region::new("us-east-1")).load().await;
    let s3 = Arc::new(Client::from_conf(aws_sdk_s3::Config::from(&sdk_config)));

    let mut js = JoinSet::new();

    let (xmd_send, xmd_recv) = tokio::sync::mpsc::channel(500);
    js.spawn(write_results(xmd_recv));

    let (xkey_send, xkey_recv) = tokio::sync::mpsc::channel(2000);
    js.spawn(list_bucket(s3.clone(), xkey_send, format!("{XORBS_PREFIX}{}", collect_args.prefix)));

    js.spawn(gather_xorb_info(s3.clone(), xkey_recv, xmd_send));

    println!("spawned all, waiting");
    js.join_all().await;
}

const NUM_JOBS_CONCURRENT: usize = 100;

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

async fn list_bucket(s3: Arc<Client>, send: Sender<String>, prefix: String) {
    println!("begin listing bucket");
    let mut response = s3
        .list_objects_v2()
        .max_keys(MAX_KEYS)
        .bucket(BUCKET)
        .prefix(&prefix)
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
            .prefix(&prefix)
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
