use criterion::{criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use sccache::lru_disk_cache::LruDiskCache;

const CACHE_CAPACITY: u64 = 1024 * 1024 * 1024;
const SEED: u64 = 42;
const TEMP_BENCH_DIR: &str = "./lru_disk_cache";

fn generate_random_key(rng: StdRng) -> String {
    rng
        .sample_iter(&rand::distributions::Alphanumeric)
        .filter_map(|c| {
            let ch = char::from(c);
            if ch.is_ascii_hexdigit() {
                Some(ch)
            } else {
                None
            }
        })
        .take(64)
        .collect()
}

fn generate_random_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen::<u8>()).collect()
}

fn benchmark_cache_put_get(c: &mut Criterion) {
    let mut lru_disk_cache = LruDiskCache::new(TEMP_BENCH_DIR, CACHE_CAPACITY).unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);
    let key = generate_random_key(rng.clone());
    let value = generate_random_bytes(256*1024*1024);

    let chunks: Vec<&[u8]> = value.chunks(64*1024).collect();
    let chunk_len = chunks.len();
    for (i, chunk) in chunks.iter().enumerate(){
        lru_disk_cache.insert_bytes(format!("{}-{}", key, i), chunk).unwrap();
    }

    c.bench_function("cache_get", |b| {
        b.iter(|| {
            let key = rng.gen::<u64>();
            let random_id = rng.gen_range(0..chunk_len);
            let get_key = format!("{}-{}", key, random_id);
            let _ = lru_disk_cache.get(&get_key);
        })
    });

    
}

criterion_group!(benches, benchmark_cache_put_get);
criterion_main!(benches);
