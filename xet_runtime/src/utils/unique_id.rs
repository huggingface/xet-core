use std::sync::atomic::{AtomicU64, Ordering};

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct UniqueId(u64);

impl Default for UniqueId {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqueId {
    pub fn new() -> Self {
        static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    pub fn null() -> Self {
        Self(0)
    }
}
