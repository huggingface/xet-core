/// Run this on a directory of files to generate a csv of different compression stats and
/// whether bg4 helps or not. This can be run using:
///
/// find <directory> | xargs ./collect_compression_stats -o compression_stats.csv
///
/// Then, use compression_prediction_tests.py to analyze this data.
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use cas_object::serialize_chunk;
use clap::Parser;
use csv::Writer;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

/// Compute the distribution of the number of 1-bits in each byte.
///
/// Returns an array of length 9, where index `i` corresponds to how many bytes
/// had exactly `i` 1-bits (i in [0..8]).
fn byte_bit_count_distribution(data: &[u8]) -> [usize; 9] {
    let mut dist = [0usize; 9]; // Put in ghost counts for the kl divergence

    for &b in data {
        // Since Rust 1.37+, u8::count_ones() is stable.
        // It returns a u32 but in [0..8] for a u8.
        let ones = b.count_ones() as usize;
        dist[ones] += 1;
    }

    dist
}

fn as_distribution<const N: usize>(dist: &[usize; N]) -> [f64; N] {
    // Normalize the input array to probabilities
    let total: usize = dist.iter().sum();
    if total == 0 {
        return [0.0; N];
    }
    let mut ret = [0.0; N];
    for i in 0..N {
        ret[i] = dist[i] as f64 / total as f64;
    }

    ret
}

/// Compute the Shannon entropy (base 2) from a bit-count distribution array.
#[inline]
fn distribution_entropy(dist: &[f64]) -> f64 {
    let mut entropy = 0.0;
    for &p in dist.iter() {
        if p > 0. {
            entropy -= p * p.log2();
        }
    }
    entropy
}

fn kl_divergence(pv: &[f64], qv: &[f64]) -> f64 {
    let mut kl = 0.0;
    for i in 0..pv.len() {
        if pv[i] > 0.0 {
            kl += pv[i] * (pv[i] / qv[i].max(1e-6)).ln();
        }
    }

    kl
}

fn lz4_compress_size(data: &[u8]) -> usize {
    serialize_chunk(data, &mut std::io::Empty::default(), Some(cas_object::CompressionScheme::LZ4)).unwrap()
}

fn bg4_lz4_compress_size(data: &[u8]) -> usize {
    serialize_chunk(data, &mut std::io::Empty::default(), Some(cas_object::CompressionScheme::ByteGrouping4LZ4))
        .unwrap()
}

struct AnalysisResult {
    pub full_entropy: f64,
    pub slice_entropies: [f64; 4],
    pub slice_kl: [f64; 4],
    pub size_scheme_1: usize,
    pub size_scheme_2: usize,
}

/// Analyze a block of data by:
/// 1. Computing the bit-count distribution and entropy for the full data.
/// 2. Doing the same for four slices: data[0::4], data[1::4], data[2::4], data[3::4].
/// 3. Compressing the data using two different schemes, returning compressed sizes.
fn analyze_data(data: &[u8]) -> AnalysisResult {
    // 1. Full distribution + entropy
    let full_dist = byte_bit_count_distribution(data);
    let full_prob = as_distribution(&full_dist);
    let full_entropy = distribution_entropy(&full_prob);

    // 2. Slice distributions + entropies
    let mut slice_entropies = [0.0; 4];
    let mut slice_kl = [0.0; 4];

    for offset in 0..4 {
        // gather the interleaved slice: data[offset::4]
        let slice_data: Vec<u8> = data.iter().skip(offset).step_by(4).copied().collect();

        let slice_dist = byte_bit_count_distribution(&slice_data);
        let slice_prob = as_distribution(&slice_dist);

        slice_entropies[offset] = distribution_entropy(&slice_prob);
        slice_kl[offset] = kl_divergence(&slice_prob, &full_prob);
    }

    // 3. Compressed sizes from the two schemes (We'll assume you have these two functions available in scope)
    let size_scheme_1 = lz4_compress_size(data); // Scheme 1
    let size_scheme_2 = bg4_lz4_compress_size(data); // Scheme 2

    // Return the results
    AnalysisResult {
        full_entropy,
        slice_entropies,
        slice_kl,
        size_scheme_1,
        size_scheme_2,
    }
}

/// Command-line arguments.
#[derive(Parser, Debug)]
struct Args {
    /// Output CSV file path (e.g. results.csv)
    #[arg(short, long, value_name = "FILE")]
    output: String,

    // Take at most these from a file before moving on to the next one.
    #[arg(short, long, value_name = "FILE", default_value = "16768")]
    max_per_file: usize,

    // Take at least these, even if both compression schemes are worse than no compression.
    #[arg(short, long, value_name = "FILE", default_value = "256")]
    min_per_file: usize,

    /// Input file paths
    #[arg(required = true)]
    files: Vec<String>,
}

#[cfg(target_family = "wasm")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    unimplemented!()
}

#[cfg(not(target_family = "wasm"))]
#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Prepare CSV writer
    let mut wtr = Writer::from_path(&args.output)?;

    // Write CSV header
    wtr.write_record([
        "file",
        "offset",
        "length",
        "full_entropy",
        "slice_0_entropy",
        "slice_1_entropy",
        "slice_2_entropy",
        "slice_3_entropy",
        "slice_0_kl",
        "slice_1_kl",
        "slice_2_kl",
        "slice_3_kl",
        "size_scheme_1",
        "size_scheme_2",
    ])?;

    let writer = Arc::new(Mutex::new(wtr));

    eprintln!("Output File: {}", &args.output);
    eprintln!("Input Files: {:?}", &args.files);

    let max_limiter = Arc::new(Semaphore::new(32 * 1024));

    let mut proc_set = JoinSet::new();

    // For each file:
    for (f_idx, file_path) in args.files.iter().enumerate() {
        // Open the file, get size
        let Ok(mut file) = OpenOptions::new().read(true).open(file_path) else {
            // Just use this as a lock to print.
            let _lg = writer.lock().await;
            eprintln!("Skipping {file_path}.");
            continue;
        };

        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        // Number of random blocks ~ file_size / 8K
        // Make sure we do at least 1 if the file is very small
        let nblocks = (file_size / 8192).clamp(1, 256 * 1024);
        let mut rng = StdRng::seed_from_u64(f_idx as u64);

        let target_count = args.max_per_file.min(nblocks);
        let count = Arc::new(AtomicUsize::new(0));
        let block_tries = Arc::new(AtomicUsize::new(0));

        // Choose random offset so block fits in file
        // If file_size == block_size, offset must be 0
        if file_size == 0 {
            return Ok(());
        }

        // For each random block
        for idx in 0..nblocks {
            // Choose random block size in [32K..96K], clamp to file size
            let block_size = rng.random_range(32_768..=98_304).min(file_size);

            let offset = rng.random_range(0..=(file_size - block_size));

            let load_permit = max_limiter.clone().acquire_owned().await.unwrap();

            // Read that block
            let Ok(_) = file.seek(SeekFrom::Start(offset as u64)) else {
                break;
            };
            let mut buffer = vec![0u8; block_size];
            let Ok(_) = file.read_exact(&mut buffer) else {
                break;
            };

            let file_path = file_path.to_owned();
            let count = count.clone();
            let writer = writer.clone();
            let block_tries = block_tries.clone();

            proc_set.spawn(async move {
                let bt = block_tries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                if bt == 0 {
                    // Just use this as a lock to print.
                    let _lg = writer.lock().await;
                    eprintln!("File {file_path}: Sampling {nblocks}; keeping first {target_count}");
                }
                if count.load(std::sync::atomic::Ordering::Relaxed) >= target_count {
                    return;
                }

                let analysis = analyze_data(&buffer);

                if analysis.size_scheme_1 < block_size || analysis.size_scheme_2 < block_size || idx < args.min_per_file
                {
                    // Write record to CSV
                    let mut writer_lg = writer.lock().await;
                    writer_lg
                        .write_record([
                            &file_path,
                            &offset.to_string(),
                            &block_size.to_string(),
                            &analysis.full_entropy.to_string(),
                            &analysis.slice_entropies[0].to_string(),
                            &analysis.slice_entropies[1].to_string(),
                            &analysis.slice_entropies[2].to_string(),
                            &analysis.slice_entropies[3].to_string(),
                            &analysis.slice_kl[0].to_string(),
                            &analysis.slice_kl[1].to_string(),
                            &analysis.slice_kl[2].to_string(),
                            &analysis.slice_kl[3].to_string(),
                            &analysis.size_scheme_1.to_string(),
                            &analysis.size_scheme_2.to_string(),
                        ])
                        .unwrap();

                    let c = count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                    if c == target_count {
                        // Just use this as a lock to print.
                        eprintln!("File {file_path}: Extracted {c} samples.");
                        return;
                    }
                }

                if bt + 1 == nblocks {
                    let c = count.load(std::sync::atomic::Ordering::Relaxed);
                    eprintln!("File {file_path}: Extracted {c} samples.");
                }

                drop(load_permit);
            });
        }
    }

    let _ = proc_set.join_all().await;

    // Finalize CSV
    writer.lock().await.flush()?;
    Ok(())
}
