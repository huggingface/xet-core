use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Ok, Result};
use clap::Parser;
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::shard_file_manager::ShardFileManager;
use mdb_shard::shard_format::test_routines::rng_hash;
use mdb_shard::shard_format::MDBShardInfo;
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use merklehash::MerkleHash;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;
use tokio::time;

const CAS_BLOCK_SIZE: usize = 512;
const PAR_TASK: usize = 1;

fn make_shard(size: u64, seed: &mut u64) -> MDBInMemoryShard {
    let mut shard = MDBInMemoryShard::default();

    while shard.shard_file_size() < size {
        let mut cas_block = Vec::<_>::new();
        let mut pos = 0u32;

        for _ in 0..CAS_BLOCK_SIZE {
            let h = rng_hash(*seed);

            let r = (1000 + (&h as &[u64; 4])[0] % 1000) as u32;
            cas_block.push(CASChunkSequenceEntry::new(rng_hash(*seed), r, pos));
            pos += r;
            *seed += 1;
        }

        shard
            .add_cas_block(MDBCASInfo {
                metadata: CASChunkSequenceHeader::new(rng_hash(!(*seed)), CAS_BLOCK_SIZE, pos),
                chunks: cas_block,
            })
            .unwrap();
    }

    shard
}

async fn run_shard_benchmark(
    shard_sizes: Vec<(u64, u64)>,
    file_contiguity: usize,
    contiguity: usize,
    block_hit_proportion: f64,
    dir: &Path,
) -> Result<()> {
    let mut seed = 0u64;

    eprintln!("Creating shards.");

    for (n_shards, target_size) in shard_sizes {
        for i in 0..n_shards {
            let shard = make_shard(target_size, &mut seed);
            let path = shard.write_to_directory(dir, None)?;

            eprintln!(
                "-> Target size {target_size:?}: Created shard {:?} / {n_shards:?} with {} CAS blocks and {} chunks",
                i + 1,
                shard.num_cas_entries(),
                shard.num_cas_entries() * CAS_BLOCK_SIZE
            );
            MDBShardInfo::load_from_reader(&mut File::open(path)?)?.print_report();
        }
    }
    eprintln!("Shards created.");

    // Now, spawn tasks to
    let counter = Arc::new(AtomicUsize::new(0));
    let mdb = ShardFileManager::new_in_session_directory(dir, false).await?;

    let start_time = Instant::now();

    // Spawn worker tasks
    let mut tasks = Vec::new();
    for t in 0..PAR_TASK {
        let top = seed;
        let counter_clone = counter.clone();
        let mdb_ref = mdb.clone();

        tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(t as u64);
            eprintln!("Worker {t:?} running.");

            loop {
                let mut hash_val = rng.random();

                let mut file_info = Vec::<MerkleHash>::with_capacity(file_contiguity);
                let hit = rng.random_bool(block_hit_proportion);

                for _ in 0..file_contiguity {
                    let h_seed = if hit { hash_val % top } else { hash_val };
                    hash_val += 1;
                    file_info.push(rng_hash(h_seed));
                }

                let mut query_loc = 0;

                while query_loc < file_contiguity {
                    let res = mdb_ref
                        .chunk_hash_dedup_query(&file_info[query_loc..(query_loc + contiguity).min(file_info.len())])
                        .await
                        .unwrap();

                    query_loc += match res {
                        Some((i, _)) => i,
                        None => 1,
                    };
                }
                counter_clone.fetch_add(query_loc, Ordering::Relaxed);
            }
        }));
    }

    // Spawn the printing task
    let counter_clone = counter.clone();

    let print_task = tokio::spawn({
        async move {
            loop {
                time::sleep(Duration::from_secs(1)).await;
                let elapsed_time = start_time.elapsed().as_secs_f64();
                let count = counter_clone.load(Ordering::Relaxed);
                println!("{count} queries, queries per second: {}", count as f64 / elapsed_time);
            }
        }
    });

    // Wait for all tasks to complete
    for task in tasks {
        task.await?;
    }
    print_task.await?;
    Ok(())
}

fn parse_arg(arg: &str) -> Result<(u64, u64)> {
    let parts: Vec<&str> = arg.split(':').collect();
    if parts.len() != 2 {
        return Err(anyhow!("Failed to parse argument: {arg}"));
    }

    let size1 = u64::from_str(parts[0]).map_err(|e| anyhow!("Failed to parse size1: {e:?}"))?;
    let size2 = u64::from_str(parts[1]).map_err(|e| anyhow!("Failed to parse size2: {e:?}"))?;

    Ok((size1, size2))
}

/// A program to run shard benchmarks
#[derive(Debug, Parser)]
struct ShardBenchmarkArgs {
    /// Sizes to be parsed
    #[clap(id = "SIZE", value_parser = parse_arg)]
    shard_sizes: Vec<(u64, u64)>,

    /// Number of contiguous hashes to call dedup with
    #[clap(long, default_value = "1")]
    contiguity: usize,

    /// The percentage of queries to hit a known block
    #[clap(long, default_value = "50")]
    hit_percent: f64,

    /// How many blocks in a file are contiguous in the same hash
    #[clap(long, default_value = "16")]
    file_contiguity: usize,

    /// Directory to use
    #[clap(long)]
    dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = ShardBenchmarkArgs::parse();

    let temp_dir = TempDir::with_prefix("git-xet-shard").expect("Failed to create temp dir");
    let dir = args.dir.unwrap_or_else(|| temp_dir.path().into());
    eprintln!("Using dir {dir:?}");

    let dir = std::fs::canonicalize(dir).unwrap();
    eprintln!("Using dir {dir:?}");

    assert!(dir.exists());

    run_shard_benchmark(
        args.shard_sizes,
        args.contiguity,
        args.file_contiguity,
        args.hit_percent.clamp(0.0, 100.0) / 100.0,
        &dir,
    )
    .await
    .unwrap();
}
