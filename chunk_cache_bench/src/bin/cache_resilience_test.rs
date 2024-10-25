use std::path::PathBuf;
use std::process::{exit, Stdio};
use std::time::{Duration, SystemTime};

use cas_types::{ChunkRange, Key};
use chunk_cache::{CacheConfig, ChunkCache, DiskCache, RandomEntryIterator};
use clap::{Args, Parser, Subcommand};
use tempdir::TempDir;

#[derive(Parser, Debug)]
#[command(about)]
struct ResilienceTestArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Parent(ParentArgs),
    Child(ChildArgs),
}

#[derive(Args, Debug)]
struct ParentArgs {
    #[arg(short, long, default_value_t = 4)]
    num_children: u8,

    #[arg(short, long, default_value_t = 60)]
    seconds: u64,

    #[arg(short, long, default_value_t = 1 << 30)]
    capacity: u64,
}

#[derive(Args, Debug)]
struct ChildArgs {
    #[arg(short, long)]
    seconds: u64,

    #[arg(long)]
    cache_root: String,

    #[arg(short, long, default_value_t = 10 << 30)]
    capacity: u64,
}

fn main() {
    let args = ResilienceTestArgs::parse();
    match args.command {
        Commands::Parent(parent) => parent_main(parent),
        Commands::Child(child) => child_main(child),
    }
}

fn parent_main(args: ParentArgs) {
    let binary = std::env::current_exe().unwrap();
    let binary_str = binary.to_str().unwrap();
    let cache_root = TempDir::new("resilience").unwrap();
    let cache_root_path = cache_root.into_path();
    let cache_root_str = cache_root_path.to_str().unwrap();
    let capacity_str = format!("{}", args.capacity);
    let seconds_str = format!("{}", args.seconds);

    eprintln!("{cache_root_str}");

    let mut handles = Vec::with_capacity(args.num_children as usize);
    for _ in 0..args.num_children {
        let handle = std::process::Command::new(binary_str)
            .args([
                "child",
                "--cache-root",
                cache_root_str,
                "--capacity",
                capacity_str.as_str(),
                "--seconds",
                seconds_str.as_str(),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .unwrap();
        handles.push(handle);
    }

    for handle in handles.iter_mut() {
        handle.wait().unwrap();
    }
    eprintln!("done");
    exit(0);
}

fn child_main(args: ChildArgs) {
    let id = std::process::id();
    let end_time = SystemTime::now().checked_add(Duration::from_secs(args.seconds)).unwrap();

    let config = CacheConfig {
        cache_directory: PathBuf::from(args.cache_root),
        cache_size: args.capacity,
    };
    let cache = DiskCache::initialize(&config).unwrap();

    eprintln!("initialized id: {id} with {} entries", cache.num_items().unwrap());

    let mut saved = (0, Key::default(), ChunkRange::default());

    let mut i = 0;
    let mut hits = 0f64;
    let mut attempts = 0f64;
    let mut it = RandomEntryIterator::default();
    while SystemTime::now() < end_time {
        let (key, range, chunk_byte_indices, data) = it.next().unwrap();
        cache.put(&key, &range, &chunk_byte_indices, &data).unwrap();
        cache.get(&key, &range).unwrap();
        if i % 1000 == 1 {
            saved = (i, key, range);
        }
        if i != 0 && i % 1000 == 0 {
            let (_old_i, key, range) = &saved;
            attempts += 1f64;
            match cache.get(key, range).unwrap() {
                Some(_) => {
                    // eprintln!("id: {id} old test got a hit {old_i} @ {i}");
                    hits += 1f64;
                },
                None => {
                    // eprintln!("id: {id} old test got a miss {old_i} @ {i}"),
                },
            };
        }
        i += 1;
    }

    if attempts > 0f64 {
        let rate = hits / attempts;
        eprintln!("{id} done {i} iterations, old key test, rate: {rate} over {attempts} attempts");
    } else {
        eprintln!("{id} done {i} iterations, not enough time to attempt old key gets");
    }

    exit(0);
}
