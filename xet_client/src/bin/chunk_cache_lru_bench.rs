use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use futures::executor::block_on;
use xet_client::chunk_cache::{CacheConfig, ChunkCache, DiskCache, RandomEntryIterator};
use xet_runtime::config::XetConfig;

const RECORD_EVERY_ACCESS_INTERVAL_NS: u64 = 0;
const RECORD_NO_MEASURED_ACCESS_INTERVAL_NS: u64 = u64::MAX;

#[derive(Debug, Clone, Copy)]
struct BenchCase {
    label: &'static str,
    eviction_policy: &'static str,
    access_update_interval_ns: Option<u64>,
}

const BENCH_CASES: &[BenchCase] = &[
    BenchCase {
        label: "lru/every-access",
        eviction_policy: "lru",
        access_update_interval_ns: Some(RECORD_EVERY_ACCESS_INTERVAL_NS),
    },
    BenchCase {
        label: "lru/no-update",
        eviction_policy: "lru",
        access_update_interval_ns: Some(RECORD_NO_MEASURED_ACCESS_INTERVAL_NS),
    },
    BenchCase {
        label: "random",
        eviction_policy: "random",
        access_update_interval_ns: None,
    },
];

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value_t = 128)]
    warm_items: usize,

    #[clap(long, default_value_t = 128)]
    write_items: usize,

    #[clap(long, default_value_t = 5)]
    read_rounds: usize,

    #[clap(long, default_value_t = 1 << 20)]
    entry_bytes: u32,

    #[clap(long)]
    cache_root: Option<PathBuf>,

    #[clap(long)]
    cache_size_bytes: Option<u64>,
}

struct Entry {
    key: xet_client::cas_types::Key,
    range: xet_client::cas_types::ChunkRange,
    offsets: Vec<u32>,
    data: Vec<u8>,
}

struct BenchResult {
    operation: &'static str,
    ops: usize,
    bytes: u64,
    elapsed: Duration,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }

    fn mib_per_sec(&self) -> f64 {
        self.bytes as f64 / (1024.0 * 1024.0) / self.elapsed.as_secs_f64()
    }

    fn micros_per_op(&self) -> f64 {
        self.elapsed.as_secs_f64() * 1_000_000.0 / self.ops as f64
    }
}

struct PolicyBenchResult {
    bench_case: BenchCase,
    temp_dir: tempfile::TempDir,
    results: Vec<BenchResult>,
}

fn main() {
    let args = Args::parse();
    block_on(run(args));
}

async fn run(args: Args) {
    let cache_size_bytes = args
        .cache_size_bytes
        .unwrap_or_else(|| (args.warm_items as u64 + args.write_items as u64 + 1) * args.entry_bytes as u64 * 4);

    let warm_entries = make_entries(args.warm_items, 42, args.entry_bytes);
    let write_entries = make_entries(args.write_items, 4242, args.entry_bytes);

    let mut results = Vec::with_capacity(BENCH_CASES.len());
    for bench_case in BENCH_CASES {
        results.push(run_policy_bench(*bench_case, &args, cache_size_bytes, &warm_entries, &write_entries).await);
    }

    print_results(&args, cache_size_bytes, &results);
}

async fn run_policy_bench(
    bench_case: BenchCase,
    args: &Args,
    cache_size_bytes: u64,
    warm_entries: &[Entry],
    write_entries: &[Entry],
) -> PolicyBenchResult {
    let temp_dir_prefix = format!("chunk-cache-{}-bench-", bench_case.label.replace('/', "-"));
    let temp_dir = match &args.cache_root {
        Some(root) => tempfile::Builder::new()
            .prefix(temp_dir_prefix.as_str())
            .tempdir_in(root)
            .unwrap(),
        None => tempfile::Builder::new().prefix(temp_dir_prefix.as_str()).tempdir().unwrap(),
    };
    let mut xet_config = XetConfig::default()
        .with_config("chunk_cache.eviction_policy", bench_case.eviction_policy)
        .unwrap();
    if let Some(access_update_interval_ns) = bench_case.access_update_interval_ns {
        xet_config = xet_config
            .with_config("chunk_cache.access_update_interval_ns", access_update_interval_ns)
            .unwrap();
    }
    let cache = DiskCache::initialize(
        &xet_config,
        &CacheConfig {
            cache_directory: temp_dir.path().to_path_buf(),
            cache_size: cache_size_bytes,
        },
    )
    .unwrap();

    for entry in warm_entries {
        cache.put(&entry.key, &entry.range, &entry.offsets, &entry.data).await.unwrap();
    }

    for entry in warm_entries {
        let result = cache.get(&entry.key, &entry.range).await.unwrap().unwrap();
        black_box(result.data.len());
    }

    PolicyBenchResult {
        bench_case,
        temp_dir,
        results: vec![
            measure_read_hits(&cache, warm_entries, args.read_rounds, args.entry_bytes).await,
            measure_duplicate_puts(&cache, warm_entries, args.read_rounds, args.entry_bytes).await,
            measure_fresh_puts(&cache, write_entries, args.entry_bytes).await,
        ],
    }
}

fn make_entries(count: usize, seed: u64, entry_bytes: u32) -> Vec<Entry> {
    RandomEntryIterator::std_from_seed(seed)
        .with_range_len(entry_bytes)
        .with_one_chunk_ranges(true)
        .take(count)
        .map(|(key, range, offsets, data)| Entry {
            key,
            range,
            offsets,
            data,
        })
        .collect()
}

async fn measure_read_hits(cache: &DiskCache, entries: &[Entry], rounds: usize, entry_bytes: u32) -> BenchResult {
    let started_at = Instant::now();
    for _ in 0..rounds {
        for entry in entries {
            let result = cache.get(&entry.key, &entry.range).await.unwrap().unwrap();
            black_box(result.data.len());
        }
    }

    BenchResult {
        operation: "read hit",
        ops: entries.len() * rounds,
        bytes: entries.len() as u64 * rounds as u64 * entry_bytes as u64,
        elapsed: started_at.elapsed(),
    }
}

async fn measure_duplicate_puts(cache: &DiskCache, entries: &[Entry], rounds: usize, entry_bytes: u32) -> BenchResult {
    let started_at = Instant::now();
    for _ in 0..rounds {
        for entry in entries {
            cache.put(&entry.key, &entry.range, &entry.offsets, &entry.data).await.unwrap();
        }
    }

    BenchResult {
        operation: "duplicate put",
        ops: entries.len() * rounds,
        bytes: entries.len() as u64 * rounds as u64 * entry_bytes as u64,
        elapsed: started_at.elapsed(),
    }
}

async fn measure_fresh_puts(cache: &DiskCache, entries: &[Entry], entry_bytes: u32) -> BenchResult {
    let started_at = Instant::now();
    for entry in entries {
        cache.put(&entry.key, &entry.range, &entry.offsets, &entry.data).await.unwrap();
    }

    BenchResult {
        operation: "fresh put (no eviction)",
        ops: entries.len(),
        bytes: entries.len() as u64 * entry_bytes as u64,
        elapsed: started_at.elapsed(),
    }
}

fn print_results(args: &Args, cache_size_bytes: u64, policy_results: &[PolicyBenchResult]) {
    println!("Chunk cache ideal read/write benchmark");
    println!();
    println!("Workload");
    println!("  Entry size:        {}", format_bytes(args.entry_bytes as u64));
    println!("  Warm items:        {}", args.warm_items);
    println!("  Fresh write items: {}", args.write_items);
    println!("  Read rounds:       {}", args.read_rounds);
    println!("  Cache size:        {}", format_bytes(cache_size_bytes));
    println!("  Eviction:          none expected");
    println!();
    println!("LRU access update intervals");
    for result in policy_results {
        if result.bench_case.eviction_policy == "lru" {
            println!(
                "  {:<22} {}",
                result.bench_case.label,
                format_access_update_interval(result.bench_case.access_update_interval_ns)
            );
        }
    }
    println!();
    println!("Cache roots");
    for result in policy_results {
        println!("  {:<22} {}", result.bench_case.label, result.temp_dir.path().display());
    }
    println!();

    println!("Results");
    println!(
        "{:<22}  {:<24}  {:>7}  {:>12}  {:>14}  {:>12}  {:>12}",
        "Case", "Operation", "Ops", "Elapsed", "Throughput", "Ops/sec", "Latency"
    );
    println!("{}", "-".repeat(115));
    for policy_result in policy_results {
        for result in &policy_result.results {
            println!(
                "{:<22}  {:<24}  {:>7}  {:>12}  {:>14}  {:>12.2}  {:>12}",
                policy_result.bench_case.label,
                result.operation,
                result.ops,
                format_duration(result.elapsed),
                format!("{:.2} MiB/s", result.mib_per_sec()),
                result.ops_per_sec(),
                format_latency(result.micros_per_op())
            );
        }
    }

    if policy_results.len() >= 2 {
        print_comparison(&policy_results[0], &policy_results[1]);
    }
}

fn print_comparison(left: &PolicyBenchResult, right: &PolicyBenchResult) {
    println!();
    println!("{} vs {}", left.bench_case.label, right.bench_case.label);
    println!("{:<24}  {:>20}  {:>20}", "Operation", "Throughput ratio", "Latency ratio");
    println!("{}", "-".repeat(70));
    for (left_result, right_result) in left.results.iter().zip(right.results.iter()) {
        println!(
            "{:<24}  {:>20}  {:>20}",
            left_result.operation,
            format!("{:.2}x", left_result.mib_per_sec() / right_result.mib_per_sec()),
            format!("{:.2}x", left_result.micros_per_op() / right_result.micros_per_op())
        );
    }
}

fn format_access_update_interval(access_update_interval_ns: Option<u64>) -> String {
    match access_update_interval_ns {
        Some(0) => "0 ns (record every access)".to_owned(),
        Some(u64::MAX) => "u64::MAX ns (skip repeated measured accesses)".to_owned(),
        Some(value) => format!("{value} ns"),
        None => "n/a".to_owned(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds >= 1.0 {
        format!("{seconds:.3} s")
    } else {
        format!("{:.3} ms", seconds * 1_000.0)
    }
}

fn format_latency(micros: f64) -> String {
    if micros >= 1_000.0 {
        format!("{:.3} ms/op", micros / 1_000.0)
    } else {
        format!("{micros:.2} us/op")
    }
}
