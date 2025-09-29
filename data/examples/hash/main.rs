use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;

use clap::Parser;
use mdb_shard::chunk_verification::range_hash_from_chunks;
use merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash};
use regex::Regex;

#[derive(Debug, Copy, Clone)]
enum HashType {
    Chunk,
    Xorb,
    File,
    Range,
}

impl std::str::FromStr for HashType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "chunk" => Ok(HashType::Chunk),
            "xorb" => Ok(HashType::Xorb),
            "file" => Ok(HashType::File),
            "range" => Ok(HashType::Range),
            _ => Err(format!("Invalid hash type: {s}")),
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    version,
    about,
    long_about = "Example of computing different hash functions for different inputs"
)]
struct HashArgs {
    /// Hash type to compute, choices are: chunk, xorb, file, range
    #[arg(short, long)]
    hash_type: HashType,
    /// Output file or uses stdout if not specified, where to write the hash
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Input file or uses stdin if not specified. For File, Xorb and Range hash types, expects input format to be a
    /// newline-separated list of chunk hashes and lengths in the format of `<hash> <length>`
    #[arg(short, long)]
    input: Option<PathBuf>,
}

fn main() {
    let args = HashArgs::parse();

    let mut input: Box<dyn BufRead> = if let Some(path) = args.input {
        Box::new(BufReader::new(File::open(path).unwrap()))
    } else {
        Box::new(std::io::stdin().lock())
    };

    let mut output: Box<dyn Write> = if let Some(path) = args.output {
        Box::new(File::create(path).unwrap())
    } else {
        Box::new(std::io::stdout())
    };

    if matches!(args.hash_type, HashType::Chunk) {
        let mut buf = vec![];
        input.read_to_end(&mut buf).unwrap();
        let hash = compute_data_hash(&buf);
        output.write_all(format!("{hash}").as_bytes()).unwrap();
        return;
    }

    let chunks_list = read_input_as_chunks_list(&mut input);
    let hash = match args.hash_type {
        HashType::Chunk => unreachable!("already handled"),
        HashType::Xorb => xorb_hash(&chunks_list),
        HashType::File => file_hash(&chunks_list),
        HashType::Range => {
            let hashes_only = chunks_list.into_iter().map(|(hash, _len)| hash).collect::<Vec<_>>();
            range_hash_from_chunks(&hashes_only)
        },
    };

    output.write_all(format!("{hash}").as_bytes()).unwrap();
}

fn read_input_as_chunks_list(input: &mut impl BufRead) -> Vec<(MerkleHash, u64)> {
    let line_regex = Regex::new(r"^([0-9a-fA-F]+)\s+(\d+)$").unwrap();

    let mut res = vec![];
    for line_result in input.lines() {
        let line: String = line_result.expect("Failed to read line");
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let caps = line_regex.captures(line).expect("Failed to parse line");
        let hash = MerkleHash::from_hex(caps.get(1).expect("Failed to find hash in line ").as_str())
            .expect("Failed to parse hash");
        let length: u64 = caps
            .get(2)
            .expect("Failed to find length in line")
            .as_str()
            .parse()
            .expect("Failed to parse length");
        res.push((hash, length));
    }

    res
}
