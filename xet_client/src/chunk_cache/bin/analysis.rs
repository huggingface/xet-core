use std::path::PathBuf;
use std::u64;

use clap::Parser;
use xet_client::chunk_cache::{CacheConfig, DiskCache};

#[derive(Debug, Parser)]
struct CacheAnalysisArgs {
    #[clap(long, short, default_value = "./xet/cache")]
    root: PathBuf,
}

/// Usage: ./cache_analysis --root "path to cache root"
/// prints out the state of the cache
#[tokio::main]
async fn main() {
    let args = CacheAnalysisArgs::parse();
    print_main(args.root).await;
}

async fn print_main(root: PathBuf) {
    let cache = DiskCache::initialize(&CacheConfig {
        cache_directory: root,
        cache_size: u64::MAX,
    })
    .unwrap();
    cache.print().await;
}
