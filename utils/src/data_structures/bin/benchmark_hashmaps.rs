//! Benchmark comparing HashMap and PassThroughHashMap performance.
//!
//! Run with: cargo run --bin benchmark_hashmaps --release

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use merklehash::{DataHash, MerkleHash};
use rand::Rng;
use utils::data_structures::PassThroughHashMap;

// Type aliases for cleaner code
type MerklePassThroughHashMap = PassThroughHashMap<DataHash, u64>;

const OPERATION_COUNTS: &[usize] = &[100_000, 1_000_000, 10_000_000];

// Data structure identifiers for columns
const DS_HASHMAP: &str = "HashMap";
const DS_PASSTHROUGH: &str = "PassThrough";

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

    fn print_summary_table(&self) {
        let data_structures = [DS_HASHMAP, DS_PASSTHROUGH];
        let tests = ["Insert", "Lookup", "Insert+Lookup", "Serialize", "Deserialize"];

        println!("\n{}", "=".repeat(65));
        println!("PERFORMANCE SUMMARY (times in ms, lower is better)");
        println!("{}", "=".repeat(65));

        // Print header
        print!("{:<25}", "Test");
        for ds in &data_structures {
            print!("{:>20}", ds);
        }
        println!();
        println!("{}", "-".repeat(65));

        for &size in OPERATION_COUNTS {
            println!("--- {} ---", format_count(size));
            for test in &tests {
                print!("  {:<23}", test);
                for ds in &data_structures {
                    if let Some(duration) = self.results.get(&(test.to_string(), size, ds.to_string())) {
                        print!("{:>20}", format_duration_ms(*duration));
                    } else {
                        print!("{:>20}", "-");
                    }
                }
                println!();
            }
            println!();
        }
        println!("{}", "=".repeat(65));
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
// Benchmarks
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
// Main Benchmark Runner
// ============================================================================

fn run_benchmarks(count: usize, results: &mut BenchmarkResults) {
    println!("\n{}", "=".repeat(80));
    println!("Benchmarking {} operations", count);
    println!("{}", "=".repeat(80));

    println!("\n--- Generating keys ---");
    let keys = generate_merkle_keys(count);
    println!("  Generated {} MerkleHash keys", count);

    println!("\n--- Benchmarks ---");

    for ds in [DS_HASHMAP, DS_PASSTHROUGH] {
        let d = bench_insert(&keys, ds);
        print_result(&format!("{} Insert", ds), count, d);
        results.record("Insert", count, ds, d);
    }

    for ds in [DS_HASHMAP, DS_PASSTHROUGH] {
        let d = bench_lookup(&keys, ds);
        print_result(&format!("{} Lookup", ds), count, d);
        results.record("Lookup", count, ds, d);
    }

    for ds in [DS_HASHMAP, DS_PASSTHROUGH] {
        let d = bench_insert_then_lookup(&keys, ds);
        print_result(&format!("{} Insert+Lookup", ds), count, d);
        results.record("Insert+Lookup", count, ds, d);
    }

    for ds in [DS_HASHMAP, DS_PASSTHROUGH] {
        let d = bench_serialize(&keys, ds);
        print_result(&format!("{} Serialize", ds), count, d);
        results.record("Serialize", count, ds, d);
    }

    for ds in [DS_HASHMAP, DS_PASSTHROUGH] {
        let d = bench_deserialize(&keys, ds);
        print_result(&format!("{} Deserialize", ds), count, d);
        results.record("Deserialize", count, ds, d);
    }
}

fn main() {
    println!("HashMap Benchmark Suite");
    println!("=======================");
    println!();
    println!("Comparing:");
    println!("  - HashMap: std::collections::HashMap");
    println!("  - PassThrough: PassThroughHashMap (optimized hasher for MerkleHash keys)");
    println!();
    println!("Key type: MerkleHash (32 bytes)");
    println!("Value type: u64");

    let mut results = BenchmarkResults::default();

    for &count in OPERATION_COUNTS {
        run_benchmarks(count, &mut results);
    }

    // Print summary table
    results.print_summary_table();

    println!("\nBenchmark complete!");
}
