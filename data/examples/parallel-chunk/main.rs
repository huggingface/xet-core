#[cfg(feature = "parallel-chunking")]
use std::fs::File;
#[cfg(feature = "parallel-chunking")]
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
#[cfg(feature = "parallel-chunking")]
use deduplication::chunk_file_parallel;

#[derive(Debug, Parser)]
#[command(
    version,
    about,
    long_about = "Parallel chunker with memory mapping and multi-threading. Requires --features parallel-chunking."
)]
struct ParallelChunkArgs {
    /// Input file (required - stdin not supported in parallel mode)
    #[arg(short, long)]
    input: PathBuf,

    /// Output file or uses stdout if not specified
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of threads for parallel processing (0 = auto-detect)
    #[arg(short, long, default_value = "0")]
    threads: usize,
}

#[cfg(feature = "parallel-chunking")]
fn main() -> std::io::Result<()> {
    let args = ParallelChunkArgs::parse();

    // Process file with parallel implementation
    let chunks = chunk_file_parallel(&args.input, Some(args.threads as u32))?;

    // Setup output writer
    let mut output: Box<dyn Write> = if let Some(save) = &args.output {
        Box::new(BufWriter::new(File::create(save)?))
    } else {
        Box::new(std::io::stdout())
    };

    // Write results
    for chunk in chunks {
        output.write_all(format!("{} {}\n", chunk.hash, chunk.data.len()).as_bytes())?;
    }

    output.flush()?;

    Ok(())
}

#[cfg(not(feature = "parallel-chunking"))]
fn main() -> std::io::Result<()> {
    eprintln!("Error: parallel-chunk requires --features parallel-chunking");
    eprintln!("Build with: cargo build --features parallel-chunking");
    eprintln!("Or use the regular 'chunk' example for sequential processing.");
    std::process::exit(1);
}
