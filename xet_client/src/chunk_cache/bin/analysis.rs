use std::path::PathBuf;
use std::u64;

use clap::Parser;
use xet_runtime::config::XetConfig;

use crate::chunk_cache::{CacheConfig, DiskCache};

#[derive(Debug, Parser)]
struct CacheAnalysisArgs {
    #[clap(long, short, default_value = "./xet/cache")]
    root: PathBuf,
}

/// Usage: ./cache_analysis --root "path to cache root"
/// prints out the state of the cache
fn main() {
    let args = CacheAnalysisArgs::parse();
    print_main(args.root);
}

fn print_main(root: PathBuf) {
    let xet_config = XetConfig::new();
    let cache = DiskCache::initialize(
        &xet_config,
        &CacheConfig {
            cache_directory: root,
            cache_size: u64::MAX,
        },
    )
    .unwrap();
    cache.print();
}
