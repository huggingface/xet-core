use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use xet_client::cas_client::{Client, MemoryClient};
use xet_data::file_reconstruction::FileReconstructor;
use xet_runtime::config::ReconstructionConfig;

struct BenchFixture {
    client: Arc<dyn Client>,
    file_hash: xet_core_structures::merklehash::MerkleHash,
    _file_size: u64,
}

async fn create_fixture(num_xorbs: usize, chunks_per_xorb: u64, chunk_size: usize) -> BenchFixture {
    let client = MemoryClient::new();

    let term_spec: Vec<(u64, (u64, u64))> = (0..num_xorbs).map(|i| ((i + 1) as u64, (0, chunks_per_xorb))).collect();

    let file_contents = client.insert_random_lazy_file(&term_spec, chunk_size).await.unwrap();
    let file_size = file_contents.data.len() as u64;

    BenchFixture {
        client: client as Arc<dyn Client>,
        file_hash: file_contents.file_hash,
        _file_size: file_size,
    }
}

fn bench_sequential_non_vectored(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = rt.block_on(create_fixture(4, 256, 65_536));

    let mut config = ReconstructionConfig::default();
    config.use_vectored_write = false;

    c.bench_with_input(
        BenchmarkId::new("reconstruct/sequential_write", format!("{}MB", fixture._file_size / (1024 * 1024))),
        &fixture,
        |b, fix| {
            b.to_async(&rt).iter(|| {
                let client = fix.client.clone();
                let hash = fix.file_hash;
                let cfg = config.clone();
                async move {
                    let dir = TempDir::new().unwrap();
                    let path = dir.path().join("out.bin");
                    FileReconstructor::new(&client, hash)
                        .with_config(cfg)
                        .reconstruct_to_file(&path, None)
                        .await
                        .unwrap();
                }
            });
        },
    );
}

fn bench_sequential_vectored(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = rt.block_on(create_fixture(4, 256, 65_536));

    let mut config = ReconstructionConfig::default();
    config.use_vectored_write = true;

    c.bench_with_input(
        BenchmarkId::new("reconstruct/vectored_write", format!("{}MB", fixture._file_size / (1024 * 1024))),
        &fixture,
        |b, fix| {
            b.to_async(&rt).iter(|| {
                let client = fix.client.clone();
                let hash = fix.file_hash;
                let cfg = config.clone();
                async move {
                    let dir = TempDir::new().unwrap();
                    let path = dir.path().join("out.bin");
                    FileReconstructor::new(&client, hash)
                        .with_config(cfg)
                        .reconstruct_to_file(&path, None)
                        .await
                        .unwrap();
                }
            });
        },
    );
}

#[cfg(target_os = "linux")]
fn bench_io_uring(c: &mut Criterion) {
    unsafe { std::env::set_var("HF_XET_DATA_ENABLE_IO_URING", "true") };
    let rt = Runtime::new().unwrap();
    let fixture = rt.block_on(create_fixture(4, 256, 65_536));

    let config = ReconstructionConfig::default();

    c.bench_with_input(
        BenchmarkId::new("reconstruct/io_uring", format!("{}MB", fixture._file_size / (1024 * 1024))),
        &fixture,
        |b, fix| {
            b.to_async(&rt).iter(|| {
                let client = fix.client.clone();
                let hash = fix.file_hash;
                let cfg = config.clone();
                async move {
                    let dir = TempDir::new().unwrap();
                    let path = dir.path().join("out.bin");
                    FileReconstructor::new(&client, hash)
                        .with_config(cfg)
                        .reconstruct_to_file(&path, None)
                        .await
                        .unwrap();
                }
            });
        },
    );
    unsafe { std::env::remove_var("HF_XET_DATA_ENABLE_IO_URING") };
}

#[cfg(target_os = "linux")]
criterion_group!(benches, bench_sequential_non_vectored, bench_sequential_vectored, bench_io_uring);

#[cfg(not(target_os = "linux"))]
criterion_group!(benches, bench_sequential_non_vectored, bench_sequential_vectored);

criterion_main!(benches);
