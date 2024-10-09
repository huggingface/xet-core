use std::sync::Arc;
use std::time::Duration;
use std::u64;

use chunk_cache::{random_key, random_range, ChunkCache, DiskCache, RandomEntryIterator};
use criterion::{criterion_group, BenchmarkId, Criterion};

use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use sccache::SCCache;
use tempdir::TempDir;
use tokio::task::JoinSet;

mod sccache;

const SEED: u64 = 42;
const NUM_PUTS: u64 = 100;
const RANGE_LEN: u32 = 1 << 20; // 1 MB
const CAPACITY: u64 = 1 << 30; // 1 GB

fn benchmark_cache_get(c: &mut Criterion, cache: &mut impl ChunkCache, variant: &str) {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut it: RandomEntryIterator<StdRng> =
        RandomEntryIterator::from_seed(SEED).with_range_len(RANGE_LEN);

    for _ in 0..NUM_PUTS {
        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
    }
    let name = format!("cache_get_{variant}");
    c.bench_function(name.as_str(), |b| {
        b.iter(|| {
            let key = random_key(&mut rng);
            let range = random_range(&mut rng);
            cache.get(&key, &range).unwrap();
        })
    });
}

fn benchmark_cache_get_mt(c: &mut Criterion, cache: Arc<impl ChunkCache + 'static>, variant: &str) {
    let mut it: RandomEntryIterator<StdRng> =
        RandomEntryIterator::from_seed(SEED).with_range_len(RANGE_LEN);

    for _ in 0..NUM_PUTS {
        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
    }
    let name = format!("cache_get_mt_{variant}");

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_with_input(BenchmarkId::new(name, ""), &0, |b, _| {
        b.to_async(&rt).iter(|| async {
            let mut handles = JoinSet::new();
            for _ in 0..4 {
                let c = cache.clone();
                handles.spawn(async move {
                    let mut rng = thread_rng();
                    let key = random_key(&mut rng);
                    let range = random_range(&mut rng);
                    c.get(&key, &range).unwrap()
                });
            }
            handles.join_all().await;
        });
    });
}

fn benchmark_cache_put(c: &mut Criterion, cache: &mut impl ChunkCache, variant: &str) {
    let mut it: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED);
    let mut total_bytes = 0;
    while total_bytes < CAPACITY {
        let (key, range, chunk_byte_indicies, data) = it.next().unwrap();
        cache
            .put(&key, &range, &chunk_byte_indicies, &data)
            .unwrap();
        total_bytes += data.len() as u64;
    }

    let name = format!("cache_put_{variant}");
    c.bench_function(name.as_str(), |b| {
        b.iter(|| {
            let (key, range, offsets, data) = it.next().unwrap();
            cache.put(&key, &range, &offsets, &data).unwrap();
        })
    });
}

fn benchmark_cache_get_hits(c: &mut Criterion, cache: &mut impl ChunkCache, variant: &str) {
    let mut it: RandomEntryIterator<StdRng> =
        RandomEntryIterator::from_seed(SEED).with_range_len(RANGE_LEN);

    let mut kr = Vec::with_capacity(NUM_PUTS as usize);
    for _ in 0..NUM_PUTS {
        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
        kr.push((key, range));
    }

    let mut i: usize = 0;
    let name = format!("cache_get_hit_{variant}");
    c.bench_function(name.as_str(), |b| {
        b.iter(|| {
            let (key, range) = &kr[i];
            cache.get(key, range).unwrap().unwrap();
            i = (i + 1) % NUM_PUTS as usize;
        })
    });
}

fn benchmark_cache_get_std_cap_1_gb(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_1GB").unwrap();
    let mut cache = DiskCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_get(c, &mut cache, "std_1_GB");
}

fn benchmark_cache_get_sccache(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_sccache").unwrap();
    let mut cache = SCCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_get(c, &mut cache, "sccache");
}

fn benchmark_cache_put_std_cap_1_gb(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_1GB").unwrap();
    let mut cache = DiskCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_put(c, &mut cache, "std_1_GB");
}

fn benchmark_cache_put_sccache(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_sccache").unwrap();
    let mut cache = SCCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_put(c, &mut cache, "sccache");
}

fn benchmark_cache_get_hits_std_cap_1_gb(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_1GB").unwrap();
    let mut cache = DiskCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_get_hits(c, &mut cache, "std_1_GB");
}

fn benchmark_cache_get_hits_sccache(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_sccache").unwrap();
    let mut cache = SCCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_get_hits(c, &mut cache, "sccache");
}

fn benchmark_cache_get_mt_std_cap_1_gb(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_1GB").unwrap();
    let cache = DiskCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_get_mt(c, Arc::new(cache), "std_1_GB");
}

fn benchmark_cache_get_mt_sccache(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_sccache").unwrap();
    let cache = SCCache::initialize(cache_root.into_path().to_path_buf(), CAPACITY).unwrap();
    benchmark_cache_get_mt(c, Arc::new(cache), "sccache");
}

criterion_group!(
    name = benches_get;
    config = Criterion::default();
    targets =
        benchmark_cache_get_std_cap_1_gb,
        benchmark_cache_get_sccache,
);

criterion_group!(
    name = benches_get_hits;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets =
        benchmark_cache_get_hits_std_cap_1_gb,
        benchmark_cache_get_hits_sccache,
);

criterion_group!(
    name = benches_put;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets =
        benchmark_cache_put_std_cap_1_gb,
        benchmark_cache_put_sccache,
);

criterion_group!(
    name = benches_get_multithreaded;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets = benchmark_cache_get_mt_std_cap_1_gb, benchmark_cache_get_mt_sccache,
);

fn main() {
    benches_get();
    benches_get_hits();
    benches_put();
    benches_get_multithreaded();
    Criterion::default().configure_from_args().final_summary();
}
