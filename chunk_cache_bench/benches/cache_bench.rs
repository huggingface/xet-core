use std::time::Duration;
use std::u64;

use chunk_cache::{random_key, random_range, DiskCache, RandomEntryIterator};
use chunk_cache_bench::sccache::SCCache;
use chunk_cache_bench::solid_cache::SolidCache;
use chunk_cache_bench::ChunkCacheExt;
use criterion::{criterion_group, BenchmarkId, Criterion};
use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use tempdir::TempDir;
use tokio::task::JoinSet;

const SEED: u64 = 42;
const NUM_PUTS: u64 = 100;
const RANGE_LEN: u32 = 1 << 20; // 1 MB
const CAPACITY: u64 = 1 << 30; // 1 GB
const NUM_CONCURRENT_TASKS: u8 = 8;

fn benchmark_cache_get<T: ChunkCacheExt>(c: &mut Criterion) {
    let cache_root = TempDir::new(format!("benchmark_cache_get_{}", T::name()).as_str()).unwrap();
    let cache = T::_initialize(cache_root.path().to_path_buf(), CAPACITY).unwrap();

    let mut rng = StdRng::seed_from_u64(SEED);
    let mut it: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED).with_range_len(RANGE_LEN);

    for _ in 0..NUM_PUTS {
        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
    }
    let name = format!("cache_get_{}", T::name());
    c.bench_function(name.as_str(), |b| {
        b.iter(|| {
            let key = random_key(&mut rng);
            let range = random_range(&mut rng);
            cache.get(&key, &range).unwrap();
        })
    });
}

fn benchmark_cache_get_mt<T: ChunkCacheExt + 'static>(c: &mut Criterion) {
    let cache_root = TempDir::new(format!("benchmark_cache_get_mt_{}", T::name()).as_str()).unwrap();
    let cache = T::_initialize(cache_root.path().to_path_buf(), CAPACITY).unwrap();

    let mut it: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED).with_range_len(RANGE_LEN);

    for _ in 0..NUM_PUTS {
        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_with_input(BenchmarkId::new("cache_get_mt", T::name()), &0, |b, _| {
        b.to_async(&rt).iter(|| async {
            let mut handles = JoinSet::new();
            for _ in 0..NUM_CONCURRENT_TASKS {
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

fn benchmark_cache_put_mt<T: ChunkCacheExt + 'static>(c: &mut Criterion) {
    let cache_root = TempDir::new(format!("benchmark_cache_put_mt_{}", T::name()).as_str()).unwrap();
    let cache = T::_initialize(cache_root.path().to_path_buf(), CAPACITY).unwrap();
    let mut it: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED);
    let mut total_bytes = 0;
    while total_bytes < CAPACITY {
        let (key, range, chunk_byte_indices, data) = it.next().unwrap();
        cache.put(&key, &range, &chunk_byte_indices, &data).unwrap();
        total_bytes += data.len() as u64;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_with_input(BenchmarkId::new("cache_put_mt", T::name()), &0, |b, _| {
        b.to_async(&rt).iter(|| async {
            let mut handles = JoinSet::new();
            for _ in 0..NUM_CONCURRENT_TASKS {
                let c = cache.clone();
                handles.spawn(async move {
                    let mut it = RandomEntryIterator::new(thread_rng());
                    let (key, range, offsets, data) = it.next().unwrap();
                    c.put(&key, &range, &offsets, &data).unwrap();
                });
            }
            handles.join_all().await;
        });
    });
}

fn benchmark_cache_put<T: ChunkCacheExt + 'static>(c: &mut Criterion) {
    let cache_root = TempDir::new(format!("benchmark_cache_put_{}", T::name()).as_str()).unwrap();
    let cache = T::_initialize(cache_root.path().to_path_buf(), CAPACITY).unwrap();

    let mut it: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED);
    let mut total_bytes = 0;
    while total_bytes < CAPACITY {
        let (key, range, chunk_byte_indices, data) = it.next().unwrap();
        cache.put(&key, &range, &chunk_byte_indices, &data).unwrap();
        total_bytes += data.len() as u64;
    }

    let name = format!("cache_put_{}", T::name());
    c.bench_function(name.as_str(), |b| {
        b.iter(|| {
            let (key, range, offsets, data) = it.next().unwrap();
            cache.put(&key, &range, &offsets, &data).unwrap();
        })
    });
}

fn benchmark_cache_get_hits<T: ChunkCacheExt + 'static>(c: &mut Criterion) {
    let cache_root = TempDir::new(format!("benchmark_cache_get_hits_{}", T::name()).as_str()).unwrap();
    let cache = T::_initialize(cache_root.path().to_path_buf(), CAPACITY).unwrap();

    let mut it: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED).with_range_len(RANGE_LEN);

    let mut kr = Vec::with_capacity(NUM_PUTS as usize);
    for _ in 0..NUM_PUTS {
        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
        kr.push((key, range));
    }

    let mut i: usize = 0;
    let name = format!("cache_get_hit_{}", T::name());
    c.bench_function(name.as_str(), |b| {
        b.iter(|| {
            let (key, range) = &kr[i];
            cache.get(key, range).unwrap().unwrap();
            i = (i + 1) % NUM_PUTS as usize;
        })
    });
}

criterion_group!(
    name = benches_get;
    config = Criterion::default();
    targets =
        benchmark_cache_get::<DiskCache>,
        benchmark_cache_get::<SCCache>,
        benchmark_cache_get::<SolidCache>,
);

criterion_group!(
    name = benches_get_hits;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets =
        benchmark_cache_get_hits::<DiskCache>,
        benchmark_cache_get_hits::<SCCache>,
        benchmark_cache_get_hits::<SolidCache>,
);

criterion_group!(
    name = benches_put;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets =
        benchmark_cache_put::<DiskCache>,
        benchmark_cache_put::<SCCache>,
        benchmark_cache_put::<SolidCache>,
);

criterion_group!(
    name = benches_get_multithreaded;
    config = Criterion::default();
    targets =
        benchmark_cache_get_mt::<DiskCache>,
        benchmark_cache_get_mt::<SCCache>,
);

criterion_group!(
    name = benches_put_multithreaded;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets =
        benchmark_cache_put_mt::<DiskCache>,
        benchmark_cache_put_mt::<SCCache>,
);

fn main() {
    benches_get();
    benches_get_hits();
    benches_put();
    benches_get_multithreaded();
    benches_put_multithreaded();
    Criterion::default().configure_from_args().final_summary();
}
