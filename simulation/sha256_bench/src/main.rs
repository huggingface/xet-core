//! Benchmark SHA-256 throughput: sha2 0.10.9 (`asm` feature, as used by xet_data
//! on non-Windows) vs sha2 0.11.0 (which removed the `asm` feature).
//!
//! Both crate versions are linked simultaneously via Cargo package renaming.
//! For each file size we hash an in-memory buffer with `Digest::update` +
//! `finalize`, matching how xet_data feeds the hasher. We run enough iterations
//! to reach a minimum wall-clock budget, then report the best (min-time)
//! throughput to reduce noise from scheduling jitter.

use std::hint::black_box;
use std::time::{Duration, Instant};

use sha2_010::{Digest as Digest010, Sha256 as Sha256_010};
use sha2_011::{Digest as Digest011, Sha256 as Sha256_011};

const KB: usize = 1024;
const MB: usize = 1024 * 1024;
const GB: usize = 1024 * 1024 * 1024;

/// (label, size in bytes)
const SIZES: &[(&str, usize)] = &[
    ("1KB", KB),
    ("50KB", 50 * KB),
    ("1MB", MB),
    ("50MB", 50 * MB),
    ("1GB", GB),
];

/// Minimum time to spend measuring each (version, size) pair.
const MIN_MEASURE: Duration = Duration::from_secs(3);
/// Minimum iterations regardless of time.
const MIN_ITERS: u64 = 3;
/// Cap iterations so tiny sizes don't run forever.
const MAX_ITERS: u64 = 5_000_000;

fn make_buffer(size: usize) -> Vec<u8> {
    // Content doesn't affect SHA-256 speed, but fill with a non-trivial,
    // deterministic pattern so the allocator/compiler can't cheat.
    let mut v = vec![0u8; size];
    let mut x: u32 = 0x9E37_79B9;
    for b in v.iter_mut() {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        *b = (x >> 24) as u8;
    }
    v
}

struct Stat {
    iters: u64,
    best: Duration,
    total: Duration,
}

impl Stat {
    fn best_gibs(&self, size: usize) -> f64 {
        (size as f64 / GB as f64) / self.best.as_secs_f64()
    }
    fn avg_gibs(&self, size: usize) -> f64 {
        let avg = self.total.as_secs_f64() / self.iters as f64;
        (size as f64 / GB as f64) / avg
    }
    fn best_per_op(&self) -> Duration {
        self.best
    }
}

/// Time a closure that hashes `buf` once, returning the 32-byte digest as a u8
/// (so `black_box` can consume it and prevent dead-code elimination).
fn measure<F: FnMut(&[u8]) -> u8>(buf: &[u8], mut hash_once: F) -> Stat {
    // Warm up (CPU freq ramp, cache, branch predictors).
    black_box(hash_once(black_box(buf)));

    let mut iters = 0u64;
    let mut best = Duration::MAX;
    let start = Instant::now();
    loop {
        let t = Instant::now();
        let out = hash_once(black_box(buf));
        let elapsed = t.elapsed();
        black_box(out);
        best = best.min(elapsed);
        iters += 1;

        if iters >= MAX_ITERS {
            break;
        }
        if iters >= MIN_ITERS && start.elapsed() >= MIN_MEASURE {
            break;
        }
    }
    Stat {
        iters,
        best,
        total: start.elapsed(),
    }
}

fn hash_010(buf: &[u8]) -> u8 {
    let mut h = Sha256_010::new();
    h.update(buf);
    h.finalize()[0]
}

fn hash_011(buf: &[u8]) -> u8 {
    let mut h = Sha256_011::new();
    h.update(buf);
    h.finalize()[0]
}

fn fmt_dur(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1e3)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else {
        format!("{:.3} s", ns as f64 / 1e9)
    }
}

fn main() {
    // Sanity: confirm both versions agree on a known digest.
    {
        let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let d010 = hex(&Sha256_010::digest(b"some data"));
        let d011 = hex(&Sha256_011::digest(b"some data"));
        let expected = "1307990e6ba5ca145eb35e99182a9bec46531bc54ddf656a602c780fa0240dee";
        assert_eq!(d010, expected, "sha2 0.10 digest mismatch");
        assert_eq!(d011, expected, "sha2 0.11 digest mismatch");
    }

    println!("SHA-256 throughput: sha2 0.10.9 (asm) vs 0.11.0");
    println!("build: --release (opt-level=3, lto=true), single-threaded\n");
    println!(
        "{:<6} {:>10} {:>12} {:>12} {:>12} {:>12} {:>8}",
        "size", "version", "best/op", "best GiB/s", "avg GiB/s", "iters", "delta"
    );
    println!("{}", "-".repeat(78));

    for &(label, size) in SIZES {
        let buf = make_buffer(size);

        let s010 = measure(&buf, hash_010);
        let s011 = measure(&buf, hash_011);

        let g010 = s010.best_gibs(size);
        let g011 = s011.best_gibs(size);
        // Positive => 0.11 faster; negative => 0.11 slower.
        let delta_pct = (g011 - g010) / g010 * 100.0;

        println!(
            "{:<6} {:>10} {:>12} {:>12.3} {:>12.3} {:>12} {:>8}",
            label,
            "0.10-asm",
            fmt_dur(s010.best_per_op()),
            g010,
            s010.avg_gibs(size),
            s010.iters,
            ""
        );
        println!(
            "{:<6} {:>10} {:>12} {:>12.3} {:>12.3} {:>12} {:>+7.1}%",
            label,
            "0.11",
            fmt_dur(s011.best_per_op()),
            g011,
            s011.avg_gibs(size),
            s011.iters,
            delta_pct
        );
        println!();
    }

    println!("delta = 0.11 best-throughput relative to 0.10-asm (positive => 0.11 faster)");
}
