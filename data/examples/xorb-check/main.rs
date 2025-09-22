use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use clap::Parser;
use merklehash::{compute_data_hash, xorb_hash, MerkleHash};
use utils::output_bytes;

#[derive(Debug, Parser)]
struct XorbCheckArgs {
    #[arg(short, long)]
    file: Option<PathBuf>,
    #[arg(short, long)]
    hash: Option<String>,
    #[arg(long, conflicts_with = "hash")]
    hash_from_path: bool,
    #[arg(short, long)]
    output_chunks: Option<PathBuf>,
    #[arg(long, conflicts_with = "output_chunks")]
    output_chunks_stdout: bool,
}

fn main() {
    let args = XorbCheckArgs::parse();

    if args.hash_from_path && args.file.is_none() {
        panic!("--hash-from-path requires --file to be set");
    }

    let mut provided_hash = None;
    if let Some(hash_str) = args.hash {
        provided_hash = Some(MerkleHash::from_hex(&hash_str).unwrap())
    } else if args.hash_from_path {
        let mut path_hash = args.file.clone().unwrap().file_name().unwrap().to_str().unwrap().to_string();
        path_hash.truncate(64);
        provided_hash = Some(MerkleHash::from_hex(&path_hash).unwrap())
    }

    let _input: Box<dyn Read> = match args.file {
        Some(path) => Box::new(File::open(path).unwrap()),
        None => Box::new(std::io::stdin()),
    };
    let mut input = BufReader::new(_input);

    let (data, boundaries) = match cas_object::deserialize_chunks(&mut input) {
        Ok(chunks) => chunks,
        Err(e) => panic!("failed to deserialize xorb: {e}"),
    };

    eprintln!(
        "Successfully deserialized xorb with {} chunks totalling {} Bytes ({})!",
        boundaries.len() - 1,
        data.len(),
        output_bytes(data.len() as u64)
    );

    let mut chunk_hashes = Vec::with_capacity(boundaries.len() - 1);
    for (chunk_start, next_chunk_start) in boundaries.iter().take(boundaries.len() - 1).zip(boundaries.iter().skip(1)) {
        let chunk = &data[(*chunk_start as usize)..(*next_chunk_start as usize)];
        let chunk_hash = compute_data_hash(chunk);
        chunk_hashes.push((chunk_hash, (next_chunk_start - chunk_start) as u64));
    }

    let computed_xorb_hash = xorb_hash(&chunk_hashes);

    eprintln!("computed xorb hash: {computed_xorb_hash}");

    if let Some(provided_hash) = provided_hash {
        if computed_xorb_hash != provided_hash {
            eprintln!("provided hash does not match computed hash!");
        } else {
            eprintln!("provided hash matches computed hash!");
        }
    }

    let mut chunks_writer: BufWriter<Box<dyn Write>> =
        BufWriter::new(match (args.output_chunks_stdout, args.output_chunks) {
            (true, _) => Box::new(std::io::stdout()),
            (false, Some(path)) => Box::new(File::create(path).unwrap()),
            (false, None) => {
                return;
            },
        });

    for (hash, size) in chunk_hashes {
        chunks_writer.write_all(format!("{hash} {size}\n").as_bytes()).unwrap();
    }
}
