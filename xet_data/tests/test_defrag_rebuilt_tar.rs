//! A rebuilt container layer fragments into short terms: defrag prevention never refuses a run.
//!
//! An image layer is an uncompressed tar. A rebuild rewrites every file's mtime, so every 512 B
//! header changes while most contents stay identical: each unchanged file then dedups against the
//! previous build as its own short run, between new header chunks. `allow_dedup_on_next_range`
//! only checks the rolling mean chunks per range, which the long runs keep above
//! `min_n_chunks_per_range × hysteresis` (8 × 0.5), so it takes every short run.
//!
//! Real case: biggest layer of vllm/vllm-openai v0.29.0 → v0.30.0 (14 GB tar each), v2 uploaded
//! after v1 with hf_xet 1.6.0: v1 203 fetches, v2 19,493 terms / 8,754 fetches (median term 2
//! chunks); cold HP download on EC2 next to CAS 2–2.5× slower for v2.
//!
//! `HF_XET_DEDUPLICATION_MIN_N_CHUNKS_PER_RANGE=64` passes (322 terms, 45% new instead of 11%); on
//! the real case it gives 1,251 fetches and v2 downloads as fast as v1, for 8.7 GB new instead of 7.2.
//!
//!   cargo test --release -p xet-data --test test_defrag_rebuilt_tar -- --nocapture

use std::path::Path;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::data_client::clean_file;
use xet_data::processing::{FileUploadSession, Sha256Policy};
use xet_runtime::core::XetContext;

const LAYER_BYTES: usize = 512 << 20;

/// A tar-like layer: files of 1 KB–4 MB (log-uniform), each a 512 B header that changes with
/// `build` and contents that don't, padded to 512 B.
fn layer(build: u64) -> Vec<u8> {
    let mut sizes = StdRng::seed_from_u64(0);
    let mut out = Vec::with_capacity(LAYER_BYTES + (5 << 20));
    for file in 0u64.. {
        if out.len() >= LAYER_BYTES {
            break;
        }
        let size = (1024.0 * 4096f64.powf(sizes.random::<f64>())) as usize;
        for (seed, len) in [((file << 32) | build, 512), ((file << 32) | 0xffff_ffff, size)] {
            let start = out.len();
            out.resize(start + len, 0);
            StdRng::seed_from_u64(seed).fill(&mut out[start..]);
        }
        out.resize(out.len().next_multiple_of(512), 0);
    }
    out
}

/// Uploads `data` to the local CAS in `dir` and prints its layout; returns its bytes per term.
async fn upload(dir: &Path, name: &str, data: &[u8]) -> f64 {
    let path = dir.join(name);
    std::fs::write(&path, data).unwrap();
    let ctx = XetContext::default().unwrap();
    let config = TranslatorConfig::local_config(&ctx, dir.join("cas")).unwrap();
    let session = FileUploadSession::new(Arc::new(config)).await.unwrap();
    let (_, m) = clean_file(session.clone(), &path, Sha256Policy::Skip).await.unwrap();
    let (_, files) = session.finalize_with_file_info().await.unwrap();
    let fi = &files[0];

    let mut chunks: Vec<_> = fi.segments.iter().map(|s| s.chunk_index_end - s.chunk_index_start).collect();
    chunks.sort_unstable();
    let terms = fi.segments.len();
    let fetches = fetch_ranges(fi);
    let mb = data.len() as f64 / 1e6;
    eprintln!(
        "{name}: {mb:.0} MB, {terms} terms ({:.2} MB/term, median {} chunks), {fetches} fetch ranges ({:.1} MB each), \
         {:.0}% new, {:.0} MB refused by defrag prevention",
        mb / terms as f64,
        chunks[terms / 2],
        mb / fetches as f64,
        m.new_bytes as f64 * 100.0 / data.len() as f64,
        m.defrag_prevented_dedup_bytes as f64 / 1e6,
    );
    data.len() as f64 / terms as f64
}

/// Ranges a download fetches: the file's chunk ranges merged per xorb.
fn fetch_ranges(fi: &MDBFileInfo) -> usize {
    let mut ranges: Vec<_> = fi
        .segments
        .iter()
        .map(|s| (s.xorb_hash, s.chunk_index_start, s.chunk_index_end))
        .collect();
    ranges.sort_unstable();
    let mut n = 0;
    let mut last: Option<(_, u32)> = None;
    for (xorb, start, end) in ranges {
        match &mut last {
            Some((x, e)) if *x == xorb && start <= *e => *e = (*e).max(end),
            _ => {
                n += 1;
                last = Some((xorb, end));
            },
        }
    }
    n
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuilt_tar_keeps_documented_range_size() {
    let dir = tempfile::tempdir().unwrap();
    upload(dir.path(), "v1", &layer(1)).await;
    let v2 = upload(dir.path(), "v2", &layer(2)).await;
    // `deduplication.min_n_chunks_per_range` "targets an average of 1MB per range".
    assert!(v2 >= 1e6, "rebuilt layer: {:.2} MB per term, documented target 1 MB", v2 / 1e6);
}
