//! Benchmark-only instrumentation that accumulates the time spent inside the
//! file write syscalls during reconstruction.
//!
//! Compiled only under the `write-timing` cargo feature; every call site in the
//! writers is `#[cfg(feature = "write-timing")]`-gated, so production builds
//! carry zero overhead and never touch these globals.
//!
//! The counters are process-global, so a process should reconstruct one file at
//! a time (as the benchmark harness does) for the numbers to be attributable.
//! For the parallel writer the writes run on concurrent blocking-pool threads,
//! so the accumulated nanoseconds are a **sum across concurrent writes** and can
//! exceed wall-clock time; for the sequential writer the single writer thread
//! serializes writes, so the sum approximates wall-clock write time.

use std::sync::atomic::{AtomicU64, Ordering};

static WRITE_NANOS: AtomicU64 = AtomicU64::new(0);
static WRITE_BYTES: AtomicU64 = AtomicU64::new(0);
static WRITE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Record one write syscall's duration and byte count.
#[inline]
pub fn record(nanos: u64, bytes: u64) {
    WRITE_NANOS.fetch_add(nanos, Ordering::Relaxed);
    WRITE_BYTES.fetch_add(bytes, Ordering::Relaxed);
    WRITE_CALLS.fetch_add(1, Ordering::Relaxed);
}

/// Zero the counters before a measured run.
pub fn reset() {
    WRITE_NANOS.store(0, Ordering::Relaxed);
    WRITE_BYTES.store(0, Ordering::Relaxed);
    WRITE_CALLS.store(0, Ordering::Relaxed);
}

/// `(total nanoseconds in write syscalls, bytes written, number of write calls)`.
pub fn snapshot() -> (u64, u64, u64) {
    (
        WRITE_NANOS.load(Ordering::Relaxed),
        WRITE_BYTES.load(Ordering::Relaxed),
        WRITE_CALLS.load(Ordering::Relaxed),
    )
}
