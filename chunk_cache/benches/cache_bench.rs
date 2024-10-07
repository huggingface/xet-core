use std::u64;

use cas_types::{Key, Range};
use chunk_cache::{ChunkCache, DiskCache};
use criterion::{criterion_group, criterion_main, Criterion};
use merklehash::MerkleHash;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempdir::TempDir;

const SEED: u64 = 42;
const AVG_CHUNK_LEN: u64 = 64 * 1024;

pub fn random_key(rng: &mut impl Rng) -> Key {
    Key {
        prefix: "default".to_string(),
        hash: MerkleHash::from_slice(&rng.gen::<[u8; 32]>()).unwrap(),
    }
}

pub fn random_range(rng: &mut impl Rng) -> Range {
    let start = rng.gen::<u32>() % 1024;
    let end = 1024.min(start + rng.gen::<u32>() % 256);
    Range { start, end }
}

pub fn random_bytes(rng: &mut impl Rng, range: &Range) -> (Vec<u32>, Vec<u8>) {
    let len: u32 = AVG_CHUNK_LEN as u32 * (range.end - range.start);
    let random_vec: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
    let mut offsets: Vec<u32> = Vec::with_capacity((range.end - range.start + 1) as usize);
    offsets.push(0);
    for _ in range.start..range.end - 1 {
        let mut num = rng.gen::<u32>() % len;
        while offsets.contains(&num) {
            num = rng.gen::<u32>() % len;
        }
        offsets.push(num);
    }
    offsets.push(4000);
    offsets.sort();
    (offsets, random_vec)
}

pub struct RandomEntryIterator {
    rng: StdRng,
}

impl RandomEntryIterator {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Iterator for RandomEntryIterator {
    type Item = (Key, Range, Vec<u32>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let key = random_key(&mut self.rng);
        let range = random_range(&mut self.rng);
        let (offsets, data) = random_bytes(&mut self.rng, &range);
        Some((key, range, offsets, data))
    }
}

const NUM_PUTS: u64 = 100;

fn benchmark_cache_put_get(c: &mut Criterion, cache: &mut impl ChunkCache) {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut it = RandomEntryIterator::new(SEED);

    for _ in 0..NUM_PUTS {
        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
    }

    c.bench_function("cache_get", |b| {
        b.iter(|| {
            let key = random_key(&mut rng);
            let range = random_range(&mut rng);
            cache.get(&key, &range).unwrap();
        })
    });
}

#[derive(Debug)]
struct NoGoodCache;

impl ChunkCache for NoGoodCache {
    fn get(
        &mut self,
        key: &Key,
        range: &Range,
    ) -> Result<Option<Vec<u8>>, chunk_cache::error::ChunkCacheError> {
        Ok(None)
    }

    fn put(
        &mut self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), chunk_cache::error::ChunkCacheError> {
        Ok(())
    }
}

fn benchmark_cache_put_get_no_good_cache(c: &mut Criterion) {
    benchmark_cache_put_get(c, &mut NoGoodCache);
}

fn benchmark_cache_put_get_std_no_cap(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_no_cap").unwrap();
    let mut cache = DiskCache::initialize(cache_root.into_path().to_path_buf(), u64::MAX).unwrap();
    benchmark_cache_put_get(c, &mut cache);
}

fn benchmark_cache_put_get_std_cap_10_gb(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_10GB").unwrap();
    let mut cache = DiskCache::initialize(cache_root.into_path().to_path_buf(), 10 << 30).unwrap();
    benchmark_cache_put_get(c, &mut cache);
}

fn benchmark_cache_put_get_std_cap_1_gb(c: &mut Criterion) {
    let cache_root = TempDir::new("bench_10GB").unwrap();
    let mut cache = DiskCache::initialize(cache_root.into_path().to_path_buf(), 1 << 30).unwrap();
    benchmark_cache_put_get(c, &mut cache);
}

criterion_group!(
    benches,
    benchmark_cache_put_get_no_good_cache,
    benchmark_cache_put_get_std_no_cap,
    benchmark_cache_put_get_std_cap_10_gb,
    benchmark_cache_put_get_std_cap_1_gb
);
criterion_main!(benches);
