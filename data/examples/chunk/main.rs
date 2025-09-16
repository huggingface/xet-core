use clap::Parser;
use deduplication::constants::TARGET_CHUNK_SIZE;
use deduplication::{Chunk, Chunker};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    version,
    about,
    long_about = "Example of using the chunker. Splits the input file or stdin into chunks and writes to stdout or the specified file the chunk hash in string format and the chunk size on a new line for each chunk in order in the file"
)]
struct ChunkArgs {
    #[arg(short, long)]
    file: Option<PathBuf>,
    #[arg(short, long)]
    save: Option<PathBuf>,
}

fn main() {
    let args = ChunkArgs::parse();

    // setup content reader
    let mut input: Box<dyn Read> = if let Some(file_path) = args.file {
        Box::new(File::open(file_path).unwrap())
    } else {
        Box::new(std::io::stdin())
    };

    // set up writer to output chunks information
    let mut output: Box<dyn Write> = if let Some(save) = args.save {
        Box::new(BufWriter::new(File::create(save).unwrap()))
    } else {
        Box::new(std::io::stdout())
    };

    let mut write_chunk = |chunk: Chunk| {
        output
            .write_all(format!("{} {}\n", chunk.hash, chunk.data.len()).as_bytes())
            .unwrap();
    };

    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);

    // read input in up to 8 MB sections and pass through chunker
    const INGESTION_BLOCK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
    let mut buf = vec![0u8; INGESTION_BLOCK_SIZE];
    loop {
        let num_read = input.read(&mut buf).unwrap();
        if num_read == 0 {
            break;
        }
        let chunks = chunker.next_block(&buf[..num_read], false);
        for chunk in chunks {
            write_chunk(chunk);
        }
    }
    if let Some(chunk) = chunker.finish() {
        write_chunk(chunk);
    }
}
