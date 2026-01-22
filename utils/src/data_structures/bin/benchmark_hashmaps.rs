//! Benchmark comparing HashMap, PassThroughHashMap, and ParallelHashMap performance.
//!
//! Run with: cargo run --bin benchmark_hashmaps --release

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use merklehash::{DataHash, MerkleHash};
use rand::Rng;
use tempfile::TempDir;
use tokio::task::JoinSet;
use utils::data_structures::{ParallelHashMap, ParallelMerkleHashMap, PassThroughHashMap};

// Type aliases for cleaner code
type MerklePassThroughHashMap = PassThroughHashMap<DataHash, u64>;

const OPERATION_COUNTS: &[usize] = &[100_000, 1_000_000, 10_000_000];
const THREAD_COUNTS: &[usize] = &[4, 8];

// Data structure identifiers for columns
const DS_HASHMAP: &str = "HashMap";
const DS_PASSTHROUGH: &str = "PassThrough";
const DS_PARALLEL: &str = "Parallel";
const DS_MUTEX_HASHMAP: &str = "Mutex<HashMap>";
const DS_MUTEX_PASSTHROUGH: &str = "Mutex<PassThrough>";

/// Results storage for summary table
#[derive(Default)]
struct BenchmarkResults {
    /// Map of (test_name, size, data_structure) -> duration
    results: BTreeMap<(String, usize, String), Duration>,
}

impl BenchmarkResults {
    fn record(&mut self, test: &str, size: usize, ds: &str, duration: Duration) {
        self.results.insert((test.to_string(), size, ds.to_string()), duration);
    }

    fn print_summary_tables(&self) {
        // Table 1: Single-threaded
        self.print_single_threaded_table();

        // Table 2: 4 Threads
        self.print_parallel_table(4);

        // Table 3: 8 Threads
        self.print_parallel_table(8);
    }

    fn print_single_threaded_table(&self) {
        let data_structures = [DS_HASHMAP, DS_PASSTHROUGH, DS_PARALLEL];
        let tests = ["Insert", "Lookup", "Insert+Lookup", "Serialize", "Deserialize"];

        println!("\n{}", "=".repeat(85));
        println!("SINGLE-THREADED PERFORMANCE (times in ms, lower is better)");
        println!("{}", "=".repeat(85));

        // Print header
        print!("{:<25}", "Test");
        for ds in &data_structures {
            print!("{:>20}", ds);
        }
        println!();
        println!("{}", "-".repeat(85));

        for &size in OPERATION_COUNTS {
            println!("--- {} ---", format_count(size));
            for test in &tests {
                let test_key = format!("1T: {}", test);
                print!("  {:<23}", test);
                for ds in &data_structures {
                    if let Some(duration) = self.results.get(&(test_key.clone(), size, ds.to_string())) {
                        print!("{:>20}", format_duration_ms(*duration));
                    } else {
                        print!("{:>20}", "-");
                    }
                }
                println!();
            }
            println!();
        }
        println!("{}", "=".repeat(85));
    }

    fn print_parallel_table(&self, num_threads: usize) {
        let data_structures = [DS_MUTEX_HASHMAP, DS_MUTEX_PASSTHROUGH, DS_PARALLEL];
        let tests = ["Insert", "Lookup", "Insert+Lookup"];

        println!("\n{}", "=".repeat(85));
        println!("PARALLEL {} THREADS PERFORMANCE (times in ms, lower is better)", num_threads);
        println!("{}", "=".repeat(85));

        // Print header
        print!("{:<25}", "Test");
        for ds in &data_structures {
            print!("{:>20}", ds);
        }
        println!();
        println!("{}", "-".repeat(85));

        for &size in OPERATION_COUNTS {
            println!("--- {} ---", format_count(size));
            for test in &tests {
                let test_key = format!("{}T: {}", num_threads, test);
                print!("  {:<23}", test);
                for ds in &data_structures {
                    if let Some(duration) = self.results.get(&(test_key.clone(), size, ds.to_string())) {
                        print!("{:>20}", format_duration_ms(*duration));
                    } else {
                        print!("{:>20}", "-");
                    }
                }
                println!();
            }
            println!();
        }
        println!("{}", "=".repeat(85));
    }
}

fn generate_merkle_keys(count: usize) -> Vec<DataHash> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            MerkleHash::from(bytes)
        })
        .collect()
}

fn format_count(count: usize) -> String {
    if count >= 1_000_000 {
        format!("{}M", count / 1_000_000)
    } else if count >= 1_000 {
        format!("{}K", count / 1_000)
    } else {
        format!("{}", count)
    }
}

fn format_duration(d: Duration) -> String {
    if d.as_secs() > 0 {
        format!("{:.2}s", d.as_secs_f64())
    } else if d.as_millis() > 0 {
        format!("{:.2}ms", d.as_secs_f64() * 1000.0)
    } else {
        format!("{:.2}µs", d.as_secs_f64() * 1_000_000.0)
    }
}

fn format_duration_ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1000.0)
}

fn format_ops_per_sec(count: usize, d: Duration) -> String {
    let ops = count as f64 / d.as_secs_f64();
    if ops >= 1_000_000_000.0 {
        format!("{:.2}B/s", ops / 1_000_000_000.0)
    } else if ops >= 1_000_000.0 {
        format!("{:.2}M/s", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.2}K/s", ops / 1_000.0)
    } else {
        format!("{:.2}/s", ops)
    }
}

fn print_result(name: &str, count: usize, duration: Duration) {
    println!("  {:<40} {:>12} ({:>12})", name, format_duration(duration), format_ops_per_sec(count, duration));
}

// ============================================================================
// Single-threaded Benchmarks
// ============================================================================

fn bench_insert(keys: &[DataHash], map_type: &str) -> Duration {
    match map_type {
        DS_HASHMAP => {
            let start = Instant::now();
            let mut map: HashMap<DataHash, u64> = HashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            std::hint::black_box(&map);
            start.elapsed()
        },
        DS_PASSTHROUGH => {
            let start = Instant::now();
            let mut map: MerklePassThroughHashMap = PassThroughHashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            std::hint::black_box(&map);
            start.elapsed()
        },
        DS_PARALLEL => {
            let start = Instant::now();
            let map: ParallelMerkleHashMap<u64> = ParallelHashMap::new();
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            std::hint::black_box(&map);
            start.elapsed()
        },
        _ => Duration::ZERO,
    }
}

fn bench_lookup(keys: &[DataHash], map_type: &str) -> Duration {
    // First build the map
    match map_type {
        DS_HASHMAP => {
            let mut map: HashMap<DataHash, u64> = HashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let start = Instant::now();
            let mut sum = 0u64;
            for key in keys {
                if let Some(v) = map.get(key) {
                    sum = sum.wrapping_add(*v);
                }
            }
            std::hint::black_box(sum);
            start.elapsed()
        },
        DS_PASSTHROUGH => {
            let mut map: MerklePassThroughHashMap = PassThroughHashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let start = Instant::now();
            let mut sum = 0u64;
            for key in keys {
                if let Some(v) = map.get(key) {
                    sum = sum.wrapping_add(*v);
                }
            }
            std::hint::black_box(sum);
            start.elapsed()
        },
        DS_PARALLEL => {
            let map: ParallelMerkleHashMap<u64> = ParallelHashMap::new();
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let start = Instant::now();
            let mut sum = 0u64;
            for key in keys {
                if let Some(v) = map.get(key) {
                    sum = sum.wrapping_add(v);
                }
            }
            std::hint::black_box(sum);
            start.elapsed()
        },
        _ => Duration::ZERO,
    }
}

fn bench_insert_then_lookup(keys: &[DataHash], map_type: &str) -> Duration {
    match map_type {
        DS_HASHMAP => {
            let start = Instant::now();
            let mut map: HashMap<DataHash, u64> = HashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let mut sum = 0u64;
            for key in keys {
                if let Some(v) = map.get(key) {
                    sum = sum.wrapping_add(*v);
                }
            }
            std::hint::black_box(sum);
            start.elapsed()
        },
        DS_PASSTHROUGH => {
            let start = Instant::now();
            let mut map: MerklePassThroughHashMap = PassThroughHashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let mut sum = 0u64;
            for key in keys {
                if let Some(v) = map.get(key) {
                    sum = sum.wrapping_add(*v);
                }
            }
            std::hint::black_box(sum);
            start.elapsed()
        },
        DS_PARALLEL => {
            let start = Instant::now();
            let map: ParallelMerkleHashMap<u64> = ParallelHashMap::new();
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let mut sum = 0u64;
            for key in keys {
                if let Some(v) = map.get(key) {
                    sum = sum.wrapping_add(v);
                }
            }
            std::hint::black_box(sum);
            start.elapsed()
        },
        _ => Duration::ZERO,
    }
}

fn bench_serialize(keys: &[DataHash], map_type: &str) -> Duration {
    match map_type {
        DS_HASHMAP => {
            let mut map: HashMap<DataHash, u64> = HashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let start = Instant::now();
            let bytes = bincode::serialize(&map).unwrap();
            std::hint::black_box(&bytes);
            start.elapsed()
        },
        DS_PASSTHROUGH => {
            let mut map: MerklePassThroughHashMap = PassThroughHashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let start = Instant::now();
            let bytes = bincode::serialize(&map).unwrap();
            std::hint::black_box(&bytes);
            start.elapsed()
        },
        _ => Duration::ZERO,
    }
}

fn bench_deserialize(keys: &[DataHash], map_type: &str) -> Duration {
    match map_type {
        DS_HASHMAP => {
            let mut map: HashMap<DataHash, u64> = HashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let bytes = bincode::serialize(&map).unwrap();
            let start = Instant::now();
            let map2: HashMap<DataHash, u64> = bincode::deserialize(&bytes).unwrap();
            std::hint::black_box(&map2);
            start.elapsed()
        },
        DS_PASSTHROUGH => {
            let mut map: MerklePassThroughHashMap = PassThroughHashMap::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }
            let bytes = bincode::serialize(&map).unwrap();
            let start = Instant::now();
            let map2: MerklePassThroughHashMap = bincode::deserialize(&bytes).unwrap();
            std::hint::black_box(&map2);
            start.elapsed()
        },
        _ => Duration::ZERO,
    }
}

// ============================================================================
// Multi-threaded Benchmarks
// ============================================================================

async fn bench_insert_threaded(keys: Arc<Vec<DataHash>>, map_type: &str, num_threads: usize) -> Duration {
    let chunk_size = keys.len() / num_threads;

    match map_type {
        DS_PARALLEL => {
            let map: Arc<ParallelMerkleHashMap<u64>> = Arc::new(ParallelHashMap::new());
            let start = Instant::now();
            let mut join_set = JoinSet::new();

            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    for i in start_idx..end_idx {
                        map.insert(keys[i], i as u64);
                    }
                });
            }

            while join_set.join_next().await.is_some() {}
            let elapsed = start.elapsed();
            std::hint::black_box(&map);
            elapsed
        },
        DS_MUTEX_HASHMAP => {
            let map: Arc<Mutex<HashMap<DataHash, u64>>> = Arc::new(Mutex::new(HashMap::with_capacity(keys.len())));
            let start = Instant::now();
            let mut join_set = JoinSet::new();

            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    for i in start_idx..end_idx {
                        map.lock().unwrap().insert(keys[i], i as u64);
                    }
                });
            }

            while join_set.join_next().await.is_some() {}
            let elapsed = start.elapsed();
            std::hint::black_box(&map);
            elapsed
        },
        DS_MUTEX_PASSTHROUGH => {
            let map: Arc<Mutex<MerklePassThroughHashMap>> =
                Arc::new(Mutex::new(PassThroughHashMap::with_capacity(keys.len())));
            let start = Instant::now();
            let mut join_set = JoinSet::new();

            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    for i in start_idx..end_idx {
                        map.lock().unwrap().insert(keys[i], i as u64);
                    }
                });
            }

            while join_set.join_next().await.is_some() {}
            let elapsed = start.elapsed();
            std::hint::black_box(&map);
            elapsed
        },
        _ => Duration::ZERO,
    }
}

async fn bench_lookup_threaded(keys: Arc<Vec<DataHash>>, map_type: &str, num_threads: usize) -> Duration {
    let chunk_size = keys.len() / num_threads;

    match map_type {
        DS_PARALLEL => {
            let map: Arc<ParallelMerkleHashMap<u64>> = Arc::new(ParallelHashMap::new());
            for (i, key) in keys.iter().enumerate() {
                map.insert(*key, i as u64);
            }

            let start = Instant::now();
            let mut join_set = JoinSet::new();

            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    let mut sum = 0u64;
                    for i in start_idx..end_idx {
                        if let Some(v) = map.get(&keys[i]) {
                            sum = sum.wrapping_add(v);
                        }
                    }
                    sum
                });
            }

            let mut total = 0u64;
            while let Some(result) = join_set.join_next().await {
                total = total.wrapping_add(result.unwrap());
            }
            let elapsed = start.elapsed();
            std::hint::black_box(total);
            elapsed
        },
        DS_MUTEX_HASHMAP => {
            let map: Arc<Mutex<HashMap<DataHash, u64>>> = Arc::new(Mutex::new(HashMap::with_capacity(keys.len())));
            for (i, key) in keys.iter().enumerate() {
                map.lock().unwrap().insert(*key, i as u64);
            }

            let start = Instant::now();
            let mut join_set = JoinSet::new();

            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    let mut sum = 0u64;
                    for i in start_idx..end_idx {
                        if let Some(v) = map.lock().unwrap().get(&keys[i]) {
                            sum = sum.wrapping_add(*v);
                        }
                    }
                    sum
                });
            }

            let mut total = 0u64;
            while let Some(result) = join_set.join_next().await {
                total = total.wrapping_add(result.unwrap());
            }
            let elapsed = start.elapsed();
            std::hint::black_box(total);
            elapsed
        },
        DS_MUTEX_PASSTHROUGH => {
            let map: Arc<Mutex<MerklePassThroughHashMap>> =
                Arc::new(Mutex::new(PassThroughHashMap::with_capacity(keys.len())));
            for (i, key) in keys.iter().enumerate() {
                map.lock().unwrap().insert(*key, i as u64);
            }

            let start = Instant::now();
            let mut join_set = JoinSet::new();

            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    let mut sum = 0u64;
                    for i in start_idx..end_idx {
                        if let Some(v) = map.lock().unwrap().get(&keys[i]) {
                            sum = sum.wrapping_add(*v);
                        }
                    }
                    sum
                });
            }

            let mut total = 0u64;
            while let Some(result) = join_set.join_next().await {
                total = total.wrapping_add(result.unwrap());
            }
            let elapsed = start.elapsed();
            std::hint::black_box(total);
            elapsed
        },
        _ => Duration::ZERO,
    }
}

async fn bench_insert_then_lookup_threaded(keys: Arc<Vec<DataHash>>, map_type: &str, num_threads: usize) -> Duration {
    let chunk_size = keys.len() / num_threads;

    match map_type {
        DS_PARALLEL => {
            let map: Arc<ParallelMerkleHashMap<u64>> = Arc::new(ParallelHashMap::new());

            let start = Instant::now();

            // Insert phase
            let mut join_set = JoinSet::new();
            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    for i in start_idx..end_idx {
                        map.insert(keys[i], i as u64);
                    }
                });
            }
            while join_set.join_next().await.is_some() {}

            // Lookup phase
            let mut join_set = JoinSet::new();
            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    let mut sum = 0u64;
                    for i in start_idx..end_idx {
                        if let Some(v) = map.get(&keys[i]) {
                            sum = sum.wrapping_add(v);
                        }
                    }
                    sum
                });
            }

            let mut total = 0u64;
            while let Some(result) = join_set.join_next().await {
                total = total.wrapping_add(result.unwrap());
            }
            let elapsed = start.elapsed();
            std::hint::black_box(total);
            elapsed
        },
        DS_MUTEX_HASHMAP => {
            let map: Arc<Mutex<HashMap<DataHash, u64>>> = Arc::new(Mutex::new(HashMap::with_capacity(keys.len())));

            let start = Instant::now();

            // Insert phase
            let mut join_set = JoinSet::new();
            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    for i in start_idx..end_idx {
                        map.lock().unwrap().insert(keys[i], i as u64);
                    }
                });
            }
            while join_set.join_next().await.is_some() {}

            // Lookup phase
            let mut join_set = JoinSet::new();
            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    let mut sum = 0u64;
                    for i in start_idx..end_idx {
                        if let Some(v) = map.lock().unwrap().get(&keys[i]) {
                            sum = sum.wrapping_add(*v);
                        }
                    }
                    sum
                });
            }

            let mut total = 0u64;
            while let Some(result) = join_set.join_next().await {
                total = total.wrapping_add(result.unwrap());
            }
            let elapsed = start.elapsed();
            std::hint::black_box(total);
            elapsed
        },
        DS_MUTEX_PASSTHROUGH => {
            let map: Arc<Mutex<MerklePassThroughHashMap>> =
                Arc::new(Mutex::new(PassThroughHashMap::with_capacity(keys.len())));

            let start = Instant::now();

            // Insert phase
            let mut join_set = JoinSet::new();
            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    for i in start_idx..end_idx {
                        map.lock().unwrap().insert(keys[i], i as u64);
                    }
                });
            }
            while join_set.join_next().await.is_some() {}

            // Lookup phase
            let mut join_set = JoinSet::new();
            for t in 0..num_threads {
                let map = map.clone();
                let keys = keys.clone();
                let start_idx = t * chunk_size;
                let end_idx = if t == num_threads - 1 {
                    keys.len()
                } else {
                    (t + 1) * chunk_size
                };

                join_set.spawn(async move {
                    let mut sum = 0u64;
                    for i in start_idx..end_idx {
                        if let Some(v) = map.lock().unwrap().get(&keys[i]) {
                            sum = sum.wrapping_add(*v);
                        }
                    }
                    sum
                });
            }

            let mut total = 0u64;
            while let Some(result) = join_set.join_next().await {
                total = total.wrapping_add(result.unwrap());
            }
            let elapsed = start.elapsed();
            std::hint::black_box(total);
            elapsed
        },
        _ => Duration::ZERO,
    }
}

// ============================================================================
// Archive Serialization (ParallelHashMap only)
// ============================================================================

async fn bench_parallel_serialize(keys: &[DataHash]) -> Duration {
    let map: ParallelMerkleHashMap<u64> = ParallelHashMap::new();
    for (i, key) in keys.iter().enumerate() {
        map.insert(*key, i as u64);
    }

    let temp_dir = TempDir::new().unwrap();
    let archive_path = temp_dir.path().join("benchmark_archive");

    let start = Instant::now();
    map.serialize_to_archive(&archive_path).await.unwrap();
    start.elapsed()
}

async fn bench_parallel_deserialize(keys: &[DataHash]) -> Duration {
    let map: ParallelMerkleHashMap<u64> = ParallelHashMap::new();
    for (i, key) in keys.iter().enumerate() {
        map.insert(*key, i as u64);
    }

    let temp_dir = TempDir::new().unwrap();
    let archive_path = temp_dir.path().join("benchmark_archive");
    map.serialize_to_archive(&archive_path).await.unwrap();

    let start = Instant::now();
    let map2: ParallelMerkleHashMap<u64> = ParallelHashMap::deserialize_from_archive(&archive_path).await.unwrap();
    std::hint::black_box(&map2);
    start.elapsed()
}

// ============================================================================
// Main Benchmark Runner
// ============================================================================

async fn run_benchmarks(count: usize, results: &mut BenchmarkResults) {
    println!("\n{}", "=".repeat(80));
    println!("Benchmarking {} operations", count);
    println!("{}", "=".repeat(80));

    println!("\n--- Generating keys ---");
    let keys = generate_merkle_keys(count);
    let keys_arc = Arc::new(keys.clone());
    println!("  Generated {} MerkleHash keys", count);

    // Single-threaded benchmarks
    println!("\n--- Single-threaded ---");

    for ds in [DS_HASHMAP, DS_PASSTHROUGH, DS_PARALLEL] {
        let d = bench_insert(&keys, ds);
        print_result(&format!("{} Insert", ds), count, d);
        results.record("1T: Insert", count, ds, d);
    }

    for ds in [DS_HASHMAP, DS_PASSTHROUGH, DS_PARALLEL] {
        let d = bench_lookup(&keys, ds);
        print_result(&format!("{} Lookup", ds), count, d);
        results.record("1T: Lookup", count, ds, d);
    }

    for ds in [DS_HASHMAP, DS_PASSTHROUGH, DS_PARALLEL] {
        let d = bench_insert_then_lookup(&keys, ds);
        print_result(&format!("{} Insert+Lookup", ds), count, d);
        results.record("1T: Insert+Lookup", count, ds, d);
    }

    for ds in [DS_HASHMAP, DS_PASSTHROUGH] {
        let d = bench_serialize(&keys, ds);
        print_result(&format!("{} Serialize", ds), count, d);
        results.record("1T: Serialize", count, ds, d);
    }

    // Parallel serialize
    let d = bench_parallel_serialize(&keys).await;
    print_result("Parallel Serialize", count, d);
    results.record("1T: Serialize", count, DS_PARALLEL, d);

    for ds in [DS_HASHMAP, DS_PASSTHROUGH] {
        let d = bench_deserialize(&keys, ds);
        print_result(&format!("{} Deserialize", ds), count, d);
        results.record("1T: Deserialize", count, ds, d);
    }

    // Parallel deserialize
    let d = bench_parallel_deserialize(&keys).await;
    print_result("Parallel Deserialize", count, d);
    results.record("1T: Deserialize", count, DS_PARALLEL, d);

    // Multi-threaded benchmarks
    for &num_threads in THREAD_COUNTS {
        println!("\n--- {} threads ---", num_threads);
        let thread_label = format!("{}T", num_threads);

        for ds in [DS_PARALLEL, DS_MUTEX_HASHMAP, DS_MUTEX_PASSTHROUGH] {
            let d = bench_insert_threaded(keys_arc.clone(), ds, num_threads).await;
            print_result(&format!("{} Insert", ds), count, d);
            results.record(&format!("{}: Insert", thread_label), count, ds, d);
        }

        for ds in [DS_PARALLEL, DS_MUTEX_HASHMAP, DS_MUTEX_PASSTHROUGH] {
            let d = bench_lookup_threaded(keys_arc.clone(), ds, num_threads).await;
            print_result(&format!("{} Lookup", ds), count, d);
            results.record(&format!("{}: Lookup", thread_label), count, ds, d);
        }

        for ds in [DS_PARALLEL, DS_MUTEX_HASHMAP, DS_MUTEX_PASSTHROUGH] {
            let d = bench_insert_then_lookup_threaded(keys_arc.clone(), ds, num_threads).await;
            print_result(&format!("{} Insert+Lookup", ds), count, d);
            results.record(&format!("{}: Insert+Lookup", thread_label), count, ds, d);
        }
    }
}

#[tokio::main]
async fn main() {
    println!("HashMap Benchmark Suite");
    println!("=======================");
    println!();
    println!("Comparing:");
    println!("  - HashMap: std::collections::HashMap");
    println!("  - PassThrough: PassThroughHashMap (optimized hasher)");
    println!("  - Parallel: ParallelHashMap (concurrent submaps)");
    println!("  - Mutex<HashMap>: Mutex wrapper around HashMap");
    println!("  - Mutex<PassThrough>: Mutex wrapper around PassThroughHashMap");
    println!();
    println!("Key type: MerkleHash (32 bytes)");
    println!("Value type: u64");

    let mut results = BenchmarkResults::default();

    for &count in OPERATION_COUNTS {
        run_benchmarks(count, &mut results).await;
    }

    // Print summary tables
    results.print_summary_tables();

    println!("\nBenchmark complete!");
}
