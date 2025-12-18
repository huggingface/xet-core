use std::sync::atomic::{AtomicU64, Ordering};

// A unique identifier type.
pub type UniqueId = u64;

// Use an AtomicU64 to generate a unique key that can be used as a program wide unique identifier.
pub fn unique_id() -> UniqueId {
    static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);
    UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
}
