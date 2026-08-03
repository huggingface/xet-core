use std::collections::VecDeque;
use std::sync::Mutex;

use super::now_ms;

/// Bounded ring of (epoch_ms, value) pairs. All console history ("recent
/// completions", concurrency-limit changes) goes through this type, which is
/// what keeps console memory independent of transfer size.
pub struct TimestampedRing<T> {
    capacity: usize,
    items: Mutex<VecDeque<(u64, T)>>,
}

impl<T: Clone> TimestampedRing<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub fn push(&self, value: T) {
        if self.capacity == 0 {
            return; // a zero-capacity ring keeps nothing
        }
        let Ok(mut items) = self.items.lock() else {
            return; // poisoned: drop the sample, never propagate
        };
        if items.len() >= self.capacity {
            items.pop_front();
        }
        items.push_back((now_ms(), value));
    }

    /// Oldest-first copy.
    pub fn snapshot(&self) -> Vec<(u64, T)> {
        self.items.lock().map(|i| i.iter().cloned().collect()).unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.items.lock().map(|i| i.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_only_last_capacity_items() {
        let ring = TimestampedRing::new(3);
        for i in 0..5u64 {
            ring.push(i);
        }
        let items: Vec<u64> = ring.snapshot().into_iter().map(|(_, v)| v).collect();
        assert_eq!(items, vec![2, 3, 4]);
    }

    #[test]
    fn ring_snapshot_is_oldest_first_with_timestamps() {
        let ring = TimestampedRing::new(8);
        ring.push("a");
        ring.push("b");
        let snap = ring.snapshot();
        assert_eq!(snap.len(), 2);
        assert!(snap[0].0 <= snap[1].0, "timestamps must be non-decreasing");
        assert_eq!(snap[0].1, "a");
    }

    #[test]
    fn empty_ring_snapshots_empty() {
        let ring: TimestampedRing<u32> = TimestampedRing::new(4);
        assert!(ring.snapshot().is_empty());
    }

    #[test]
    fn zero_capacity_ring_keeps_nothing() {
        let ring = TimestampedRing::new(0);
        ring.push(1u32);
        ring.push(2u32);
        assert!(ring.snapshot().is_empty());
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn capacity_one_keeps_only_newest() {
        let ring = TimestampedRing::new(1);
        ring.push(1u32);
        ring.push(2u32);
        let items: Vec<u32> = ring.snapshot().into_iter().map(|(_, v)| v).collect();
        assert_eq!(items, vec![2]);
    }

    #[test]
    fn eviction_starts_exactly_at_capacity_boundary() {
        let ring = TimestampedRing::new(3);
        ring.push(1u32);
        ring.push(2u32);
        ring.push(3u32);
        assert_eq!(ring.len(), 3);
        ring.push(4u32);
        assert_eq!(ring.len(), 3);
        let items: Vec<u32> = ring.snapshot().into_iter().map(|(_, v)| v).collect();
        assert_eq!(items, vec![2, 3, 4]);
    }
}
