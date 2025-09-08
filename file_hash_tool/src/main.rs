use cas_types::HexMerkleHash;
use clap::Parser;
use deduplication::constants::TARGET_CHUNK_SIZE;
use deduplication::Chunker;
use merklehash::MerkleHash;
use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    path: String,
    #[clap(short, long)]
    original_file: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Chunk {
    pub hash: HexMerkleHash,
    pub length: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FileFormat {
    file_hash: HexMerkleHash,
    file_chunks: Vec<Chunk>,
}

fn main() {
    let args = Args::parse();
    let file = File::open(&args.path).unwrap();
    let FileFormat { file_hash, file_chunks } = serde_json::from_reader(file).unwrap();

    let chunks: Vec<(MerkleHash, u64)> = file_chunks
        .into_iter()
        .map(|chunk| (chunk.hash.into(), chunk.length as u64))
        .collect();

    let computed_file_hash = merklehash::file_hash(&chunks);

    println!("json content:\t\t{file_hash}");
    println!("computed file hash:\t{computed_file_hash}");

    let Some(original_file) = args.original_file else {
        return;
    };

    let mut file = File::open(original_file).unwrap();
    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);
    let mut computed_chunks: Vec<(MerkleHash, u64)> = vec![];
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        let num_read = file.read(&mut buf).unwrap();
        if num_read == 0 {
            break;
        }
        let new_chunks = chunker.next_block(&buf[..num_read], false);
        computed_chunks.extend(new_chunks.into_iter().map(|chunk| (chunk.hash, chunk.data.len() as u64)));
    }
    if let Some(chunk) = chunker.finish() {
        computed_chunks.push((chunk.hash, chunk.data.len() as u64));
    }

    let computed_computed_file_hash = merklehash::file_hash(&computed_chunks);
    println!("file content computed hash:\t{computed_computed_file_hash}");

    if chunks.len() != computed_chunks.len() {
        println!("chunks len don't match comp {} != {} rec", computed_chunks.len(), chunks.len());
        println!(
            "first chunks of each\nchunked\t: {} {}\nrec\t: {} {}",
            computed_chunks[0].0, computed_chunks[0].1, chunks[0].0, chunks[0].1
        );
        return;
    }

    println!("lens match");
    for (i, ((comp_hash, comp_len), (rec_hash, rec_len))) in computed_chunks.iter().zip(chunks.iter()).enumerate() {
        if rec_hash == comp_hash && rec_len == comp_len {
            continue;
        }
        println!("first mismatch at index: {i}");
        println!("chunker: {comp_hash} {comp_len}");
        println!("from json: {rec_hash} {rec_len}");
        return;
    }

    println!("all chunks match");
}
