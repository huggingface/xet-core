use std::fs;
use std::hint::black_box;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use futures::executor::block_on;
use xet_client::chunk_cache::{CacheConfig, ChunkCache, DiskCache, RandomEntryIterator};
use xet_runtime::config::XetConfig;
use xet_runtime::utils::ByteSize;

const RECORD_EVERY_ACCESS_INTERVAL_NS: u64 = 0;
const RECORD_NO_MEASURED_ACCESS_INTERVAL_NS: u64 = u64::MAX;
const READY_WAIT_TIMEOUT_SECS: u64 = 30;
const WORKER_RESULT_PREFIX: &str = "chunk_cache_lru_bench_worker_result\t";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchOperation {
    ReadHit,
    DuplicatePut,
    FreshPut,
    FreshPutWithEviction,
}

const BENCH_OPERATIONS: &[BenchOperation] = &[
    BenchOperation::ReadHit,
    BenchOperation::DuplicatePut,
    BenchOperation::FreshPut,
    BenchOperation::FreshPutWithEviction,
];

impl BenchOperation {
    fn arg(self) -> &'static str {
        match self {
            BenchOperation::ReadHit => "read-hit",
            BenchOperation::DuplicatePut => "duplicate-put",
            BenchOperation::FreshPut => "fresh-put",
            BenchOperation::FreshPutWithEviction => "fresh-put-eviction",
        }
    }

    fn label(self) -> &'static str {
        match self {
            BenchOperation::ReadHit => "read hit",
            BenchOperation::DuplicatePut => "duplicate put",
            BenchOperation::FreshPut => "fresh put (no eviction)",
            BenchOperation::FreshPutWithEviction => "fresh put (eviction)",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "read-hit" => Some(BenchOperation::ReadHit),
            "duplicate-put" => Some(BenchOperation::DuplicatePut),
            "fresh-put" => Some(BenchOperation::FreshPut),
            "fresh-put-eviction" => Some(BenchOperation::FreshPutWithEviction),
            _ => None,
        }
    }

    fn expects_eviction(self) -> bool {
        matches!(self, BenchOperation::FreshPutWithEviction)
    }
}

#[derive(Debug, Clone, Copy)]
struct BenchCase {
    label: &'static str,
    eviction_policy: &'static str,
    access_update_interval_ns: Option<u64>,
}

const BENCH_CASES: &[BenchCase] = &[
    BenchCase {
        label: "lru/record-access/every",
        eviction_policy: "lru",
        access_update_interval_ns: Some(RECORD_EVERY_ACCESS_INTERVAL_NS),
    },
    BenchCase {
        label: "lru/record-access/skip",
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

    #[clap(long, value_parser = parse_entry_size, default_value = "1MiB")]
    entry_size: u32,

    #[clap(long, default_value_t = 1, value_parser = parse_nproc)]
    nproc: usize,

    #[clap(long)]
    cache_root: Option<PathBuf>,

    #[clap(long)]
    cache_size_bytes: Option<u64>,

    #[clap(long, hide = true)]
    worker: bool,

    #[clap(long, hide = true)]
    worker_bench_case: Option<String>,

    #[clap(long, hide = true)]
    worker_operation: Option<String>,

    #[clap(long, hide = true)]
    worker_index: Option<usize>,

    #[clap(long, hide = true)]
    worker_cache_directory: Option<PathBuf>,

    #[clap(long, hide = true)]
    worker_ready_file: Option<PathBuf>,

    #[clap(long, hide = true)]
    worker_start_file: Option<PathBuf>,
}

struct WorkerProcess {
    index: usize,
    ready_file: PathBuf,
    child: Child,
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
    let args = Args::parse_from(std::env::args().filter(|arg| arg != "--bench"));
    if args.worker {
        block_on(run_worker(args));
    } else {
        block_on(run(args));
    }
}

fn parse_entry_size(value: &str) -> Result<u32, String> {
    let bytes = value
        .parse::<ByteSize>()
        .map_err(|e| format!("invalid entry size {value:?}: {e}"))?
        .as_u64();
    u32::try_from(bytes).map_err(|_| format!("entry size {bytes} bytes exceeds u32::MAX"))
}

fn parse_nproc(value: &str) -> Result<usize, String> {
    let nproc = value
        .parse::<usize>()
        .map_err(|e| format!("invalid process count {value:?}: {e}"))?;
    if nproc == 0 {
        Err("process count must be at least 1".to_owned())
    } else {
        Ok(nproc)
    }
}

async fn run(args: Args) {
    let cache_size_bytes = args.cache_size_bytes.unwrap_or_else(|| {
        (args.warm_items as u64 + args.write_items as u64 * args.nproc as u64 + 1) * args.entry_size as u64 * 4
    });
    let eviction_cache_size_bytes = default_eviction_cache_size_bytes(&args);

    let warm_entries = make_entries(args.warm_items, 42, args.entry_size);
    let write_entries = make_entries(args.write_items, write_seed(0), args.entry_size);

    let mut results = Vec::with_capacity(BENCH_CASES.len());
    for bench_case in BENCH_CASES {
        results.push(
            run_policy_bench(
                *bench_case,
                &args,
                cache_size_bytes,
                eviction_cache_size_bytes,
                &warm_entries,
                &write_entries,
            )
            .await,
        );
    }

    print_results(&args, cache_size_bytes, eviction_cache_size_bytes, &results);
}

async fn run_worker(args: Args) {
    let bench_case = args
        .worker_bench_case
        .as_deref()
        .and_then(find_bench_case)
        .expect("worker requires a valid --worker-bench-case");
    let operation = args
        .worker_operation
        .as_deref()
        .and_then(BenchOperation::parse)
        .expect("worker requires a valid --worker-operation");
    let worker_index = args.worker_index.expect("worker requires --worker-index");
    let cache_directory = args
        .worker_cache_directory
        .clone()
        .expect("worker requires --worker-cache-directory");
    let ready_file = args.worker_ready_file.clone().expect("worker requires --worker-ready-file");
    let start_file = args.worker_start_file.clone().expect("worker requires --worker-start-file");
    let cache_size_bytes = args.cache_size_bytes.expect("worker requires --cache-size-bytes");

    let cache = initialize_cache(bench_case, cache_directory, cache_size_bytes);
    let warm_entries = make_entries(args.warm_items, 42, args.entry_size);
    let write_entries = make_entries(args.write_items, write_seed(worker_index), args.entry_size);

    fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&start_file, Duration::from_secs(READY_WAIT_TIMEOUT_SECS));

    let result =
        measure_operation(&cache, operation, &warm_entries, &write_entries, args.read_rounds, args.entry_size).await;
    println!(
        "{WORKER_RESULT_PREFIX}operation={} ops={} bytes={} elapsed_ns={}",
        operation.arg(),
        result.ops,
        result.bytes,
        result.elapsed.as_nanos()
    );
}

async fn run_policy_bench(
    bench_case: BenchCase,
    args: &Args,
    cache_size_bytes: u64,
    eviction_cache_size_bytes: u64,
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
    let cache = initialize_cache(bench_case, temp_dir.path().to_path_buf(), cache_size_bytes);

    for entry in warm_entries {
        cache.put(&entry.key, &entry.range, &entry.offsets, &entry.data).await.unwrap();
    }

    for entry in warm_entries {
        let result = cache.get(&entry.key, &entry.range).await.unwrap().unwrap();
        black_box(result.data.len());
    }

    let results = if args.nproc == 1 {
        let mut results = Vec::with_capacity(BENCH_OPERATIONS.len());
        for operation in BENCH_OPERATIONS {
            if operation.expects_eviction() {
                let eviction_cache = initialize_prefilled_eviction_cache(
                    bench_case,
                    temp_dir.path(),
                    eviction_cache_size_bytes,
                    args.entry_size,
                )
                .await;
                results.push(
                    measure_operation(
                        &eviction_cache,
                        *operation,
                        warm_entries,
                        write_entries,
                        args.read_rounds,
                        args.entry_size,
                    )
                    .await,
                );
            } else {
                results.push(
                    measure_operation(
                        &cache,
                        *operation,
                        warm_entries,
                        write_entries,
                        args.read_rounds,
                        args.entry_size,
                    )
                    .await,
                );
            }
        }
        results
    } else {
        drop(cache);
        let mut results = Vec::with_capacity(BENCH_OPERATIONS.len());
        for operation in BENCH_OPERATIONS {
            if operation.expects_eviction() {
                results.push(
                    run_eviction_operation_in_processes(
                        bench_case,
                        args,
                        eviction_cache_size_bytes,
                        temp_dir.path(),
                        *operation,
                    )
                    .await,
                );
            } else {
                results.push(run_operation_in_processes(
                    bench_case,
                    args,
                    cache_size_bytes,
                    temp_dir.path(),
                    *operation,
                ));
            }
        }
        results
    };

    PolicyBenchResult {
        bench_case,
        temp_dir,
        results,
    }
}

fn initialize_cache(bench_case: BenchCase, cache_directory: PathBuf, cache_size_bytes: u64) -> DiskCache {
    let mut xet_config = XetConfig::default()
        .with_config("chunk_cache.eviction_policy", bench_case.eviction_policy)
        .unwrap();
    if let Some(access_update_interval_ns) = bench_case.access_update_interval_ns {
        xet_config = xet_config
            .with_config("chunk_cache.access_update_interval_ns", access_update_interval_ns)
            .unwrap();
    }

    DiskCache::initialize(
        &xet_config,
        &CacheConfig {
            cache_directory,
            cache_size: cache_size_bytes,
        },
    )
    .unwrap()
}

fn find_bench_case(label: &str) -> Option<BenchCase> {
    BENCH_CASES.iter().copied().find(|bench_case| bench_case.label == label)
}

async fn initialize_prefilled_eviction_cache(
    bench_case: BenchCase,
    cache_root: &Path,
    cache_size_bytes: u64,
    entry_size: u32,
) -> DiskCache {
    let cache_directory = cache_root.join("eviction-cache");
    let _ = fs::remove_dir_all(&cache_directory);
    let cache = initialize_cache(bench_case, cache_directory, cache_size_bytes);
    prefill_cache_for_eviction(&cache, cache_size_bytes, entry_size).await;
    cache
}

async fn prefill_cache_for_eviction(cache: &DiskCache, cache_size_bytes: u64, entry_size: u32) {
    let item_size_bytes = generated_cache_item_size_bytes(entry_size);
    assert!(
        cache_size_bytes >= item_size_bytes,
        "eviction cache size {} is smaller than one generated cache item {}",
        format_bytes(cache_size_bytes),
        format_bytes(item_size_bytes)
    );

    let prefill_items =
        usize::try_from(cache_size_bytes / item_size_bytes).expect("eviction cache item count exceeds usize::MAX") + 1;
    for (key, range, offsets, data) in RandomEntryIterator::std_from_seed(eviction_prefill_seed())
        .with_range_len(entry_size)
        .with_one_chunk_ranges(true)
        .take(prefill_items)
    {
        cache.put(&key, &range, &offsets, &data).await.unwrap();
    }
}

async fn run_eviction_operation_in_processes(
    bench_case: BenchCase,
    args: &Args,
    cache_size_bytes: u64,
    cache_root: &Path,
    operation: BenchOperation,
) -> BenchResult {
    let cache_directory = cache_root.join("eviction-cache");
    let _ = fs::remove_dir_all(&cache_directory);
    {
        let cache = initialize_cache(bench_case, cache_directory.clone(), cache_size_bytes);
        prefill_cache_for_eviction(&cache, cache_size_bytes, args.entry_size).await;
    }

    run_operation_in_processes(bench_case, args, cache_size_bytes, &cache_directory, operation)
}

fn run_operation_in_processes(
    bench_case: BenchCase,
    args: &Args,
    cache_size_bytes: u64,
    cache_directory: &Path,
    operation: BenchOperation,
) -> BenchResult {
    let executable = std::env::current_exe().unwrap();
    let start_file = cache_directory.join(format!(".chunk-cache-bench-start-{}", operation.arg()));
    let _ = fs::remove_file(&start_file);

    let mut workers = Vec::with_capacity(args.nproc);
    for worker_index in 0..args.nproc {
        let ready_file = cache_directory.join(format!(".chunk-cache-bench-ready-{}-{worker_index}", operation.arg()));
        let _ = fs::remove_file(&ready_file);
        let child = Command::new(&executable)
            .arg("--worker")
            .arg("--worker-bench-case")
            .arg(bench_case.label)
            .arg("--worker-operation")
            .arg(operation.arg())
            .arg("--worker-index")
            .arg(worker_index.to_string())
            .arg("--worker-cache-directory")
            .arg(cache_directory)
            .arg("--worker-ready-file")
            .arg(&ready_file)
            .arg("--worker-start-file")
            .arg(&start_file)
            .arg("--warm-items")
            .arg(args.warm_items.to_string())
            .arg("--write-items")
            .arg(args.write_items.to_string())
            .arg("--read-rounds")
            .arg(args.read_rounds.to_string())
            .arg("--entry-size")
            .arg(args.entry_size.to_string())
            .arg("--cache-size-bytes")
            .arg(cache_size_bytes.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn worker {worker_index}: {e}"));
        workers.push(WorkerProcess {
            index: worker_index,
            ready_file,
            child,
        });
    }

    wait_for_ready_files(&mut workers);
    fs::write(&start_file, b"start").unwrap();

    let mut worker_results = Vec::with_capacity(workers.len());
    for worker in workers {
        let output = worker
            .child
            .wait_with_output()
            .unwrap_or_else(|e| panic!("failed to wait for worker {}: {e}", worker.index));
        if !output.status.success() {
            panic!(
                "worker {} failed with status {}\nstdout:\n{}\nstderr:\n{}",
                worker.index,
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        worker_results.push(parse_worker_result(worker.index, operation, &output));
        let _ = fs::remove_file(worker.ready_file);
    }
    let _ = fs::remove_file(start_file);

    aggregate_worker_results(operation, &worker_results)
}

fn wait_for_ready_files(workers: &mut [WorkerProcess]) {
    let started_at = Instant::now();
    loop {
        if workers.iter().all(|worker| worker.ready_file.exists()) {
            return;
        }
        for worker in workers.iter_mut() {
            if !worker.ready_file.exists() {
                if let Some(status) = worker
                    .child
                    .try_wait()
                    .unwrap_or_else(|e| panic!("failed to poll worker {}: {e}", worker.index))
                {
                    panic!("worker {} exited before becoming ready with status {status}", worker.index);
                }
            }
        }
        if started_at.elapsed() > Duration::from_secs(READY_WAIT_TIMEOUT_SECS) {
            let missing = workers
                .iter()
                .filter(|worker| !worker.ready_file.exists())
                .map(|worker| worker.index.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            panic!("worker(s) did not become ready within {READY_WAIT_TIMEOUT_SECS}s: {missing}");
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_file(path: &Path, timeout: Duration) {
    let started_at = Instant::now();
    while !path.exists() {
        if started_at.elapsed() > timeout {
            panic!("timed out waiting for {}", path.display());
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn parse_worker_result(worker_index: usize, operation: BenchOperation, output: &std::process::Output) -> BenchResult {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix(WORKER_RESULT_PREFIX))
        .unwrap_or_else(|| panic!("worker {worker_index} did not print a result line\nstdout:\n{stdout}"));

    let mut result_operation = None;
    let mut ops = None;
    let mut bytes = None;
    let mut elapsed_ns = None;
    for field in line.split_whitespace() {
        let Some((key, value)) = field.split_once('=') else {
            continue;
        };
        match key {
            "operation" => result_operation = Some(value.to_owned()),
            "ops" => ops = Some(value.parse::<usize>().unwrap()),
            "bytes" => bytes = Some(value.parse::<u64>().unwrap()),
            "elapsed_ns" => elapsed_ns = Some(value.parse::<u64>().unwrap()),
            _ => {},
        }
    }

    let result_operation = result_operation.unwrap_or_else(|| panic!("worker {worker_index} result missing operation"));
    assert_eq!(result_operation, operation.arg(), "worker {worker_index} returned the wrong operation");
    BenchResult {
        operation: operation.label(),
        ops: ops.unwrap_or_else(|| panic!("worker {worker_index} result missing ops")),
        bytes: bytes.unwrap_or_else(|| panic!("worker {worker_index} result missing bytes")),
        elapsed: Duration::from_nanos(
            elapsed_ns.unwrap_or_else(|| panic!("worker {worker_index} result missing elapsed_ns")),
        ),
    }
}

fn aggregate_worker_results(operation: BenchOperation, worker_results: &[BenchResult]) -> BenchResult {
    BenchResult {
        operation: operation.label(),
        ops: worker_results.iter().map(|result| result.ops).sum(),
        bytes: worker_results.iter().map(|result| result.bytes).sum(),
        elapsed: worker_results.iter().map(|result| result.elapsed).max().unwrap_or_default(),
    }
}

fn write_seed(worker_index: usize) -> u64 {
    4242 + worker_index as u64
}

fn eviction_prefill_seed() -> u64 {
    2424
}

fn generated_cache_item_size_bytes(entry_size: u32) -> u64 {
    entry_size as u64 + 3 * size_of::<u32>() as u64
}

fn default_eviction_cache_size_bytes(args: &Args) -> u64 {
    generated_cache_item_size_bytes(args.entry_size) * args.warm_items.max(1) as u64
}

fn make_entries(count: usize, seed: u64, entry_size: u32) -> Vec<Entry> {
    RandomEntryIterator::std_from_seed(seed)
        .with_range_len(entry_size)
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

async fn measure_read_hits(cache: &DiskCache, entries: &[Entry], rounds: usize, entry_size: u32) -> BenchResult {
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
        bytes: entries.len() as u64 * rounds as u64 * entry_size as u64,
        elapsed: started_at.elapsed(),
    }
}

async fn measure_operation(
    cache: &DiskCache,
    operation: BenchOperation,
    warm_entries: &[Entry],
    write_entries: &[Entry],
    rounds: usize,
    entry_size: u32,
) -> BenchResult {
    match operation {
        BenchOperation::ReadHit => measure_read_hits(cache, warm_entries, rounds, entry_size).await,
        BenchOperation::DuplicatePut => measure_duplicate_puts(cache, warm_entries, rounds, entry_size).await,
        BenchOperation::FreshPut => measure_fresh_puts(cache, write_entries, entry_size, operation.label()).await,
        BenchOperation::FreshPutWithEviction => {
            measure_fresh_puts(cache, write_entries, entry_size, operation.label()).await
        },
    }
}

async fn measure_duplicate_puts(cache: &DiskCache, entries: &[Entry], rounds: usize, entry_size: u32) -> BenchResult {
    let started_at = Instant::now();
    for _ in 0..rounds {
        for entry in entries {
            cache.put(&entry.key, &entry.range, &entry.offsets, &entry.data).await.unwrap();
        }
    }

    BenchResult {
        operation: "duplicate put",
        ops: entries.len() * rounds,
        bytes: entries.len() as u64 * rounds as u64 * entry_size as u64,
        elapsed: started_at.elapsed(),
    }
}

async fn measure_fresh_puts(
    cache: &DiskCache,
    entries: &[Entry],
    entry_size: u32,
    operation: &'static str,
) -> BenchResult {
    let started_at = Instant::now();
    for entry in entries {
        cache.put(&entry.key, &entry.range, &entry.offsets, &entry.data).await.unwrap();
    }

    BenchResult {
        operation,
        ops: entries.len(),
        bytes: entries.len() as u64 * entry_size as u64,
        elapsed: started_at.elapsed(),
    }
}

fn print_results(
    args: &Args,
    cache_size_bytes: u64,
    eviction_cache_size_bytes: u64,
    policy_results: &[PolicyBenchResult],
) {
    println!("Chunk cache read/write benchmark");
    println!();
    println!("Workload");
    println!("  Entry size:        {}", format_bytes(args.entry_size as u64));
    println!("  Warm items:        {}", args.warm_items);
    println!("  Fresh write items: {}", args.write_items);
    println!("  Read rounds:       {}", args.read_rounds);
    println!("  Processes:         {}", args.nproc);
    println!("  Cache size:        {}", format_bytes(cache_size_bytes));
    println!("  Eviction cache:    {}", format_bytes(eviction_cache_size_bytes));
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
