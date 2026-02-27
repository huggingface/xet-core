use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use more_asserts::debug_assert_le;
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

/// Maximum number of internal (physical) permits. Capped at `u32::MAX` since
/// `acquire_many_owned` takes `u32`.
const PERMIT_LIMIT: u64 = {
    let sem_max = Semaphore::MAX_PERMITS as u64;
    let u32_max = u32::MAX as u64;
    if sem_max < u32_max { sem_max } else { u32_max }
};

/// An adjustable semaphore in which the total number of permits can be adjusted
/// at any time between a minimum and a maximum bound.
///
/// Unlike the tokio Semaphore, decreasing the number of permits may be done at
/// any time and are resolved lazily if needed; any permits currently issued
/// remain valid, but no new permits are issued until any requested decreases
/// are resolved.
///
/// When `max_permits` exceeds the platform semaphore limit, a scaling basis is
/// automatically computed so that all permit operations are transparently
/// mapped to a smaller number of internal permits. On 64-bit platforms the
/// basis is 1 for any practical permit count.
#[derive(Debug)]
pub struct AdjustableSemaphore {
    semaphore: Arc<Semaphore>,
    total_permits: AtomicU64,
    enqueued_permit_decreases: AtomicU64,
    min_physical_permits: u64,
    max_physical_permits: u64,
    basis: u64,

    /// Serializes permit adjustment operations so direct and target-based
    /// increment/decrement APIs cannot race with one another.
    adjustment_lock: Mutex<()>,
}

/// A permit issued by the AdjustableSemaphore.  On drop, this attempts to
/// resolve any enqueued permit decrease if one is needed.
pub struct AdjustableSemaphorePermit {
    permit: Option<OwnedSemaphorePermit>,
    num_physical_permits: u32,
    parent: Arc<AdjustableSemaphore>,
}

impl AdjustableSemaphorePermit {
    /// The number of logical permits held by this permit (scaled by basis).
    pub fn num_permits(&self) -> u64 {
        self.num_physical_permits as u64 * self.parent.basis
    }

    /// The number of internal (physical) permits held by this permit.
    pub fn num_physical_permits(&self) -> u32 {
        self.num_physical_permits
    }

    /// Split `n` logical permits off this permit into a new permit.
    /// Returns `None` if `n` is zero or the scaled physical count exceeds
    /// what this permit holds.
    pub fn split(&mut self, n: u64) -> Option<AdjustableSemaphorePermit> {
        let physical_n = n.div_ceil(self.parent.basis);
        if physical_n > self.num_physical_permits as u64 {
            return None;
        }
        let physical_n = physical_n as u32;

        self.num_physical_permits -= physical_n;

        if physical_n > 0 {
            let permit = self.permit.as_mut().and_then(|p| p.split(physical_n as usize));
            Some(AdjustableSemaphorePermit {
                permit,
                num_physical_permits: physical_n,
                parent: self.parent.clone(),
            })
        } else {
            None
        }
    }
}

impl Drop for AdjustableSemaphorePermit {
    fn drop(&mut self) {
        let parent = &self.parent;
        let num_permits = self.num_physical_permits as u64;

        let decreases_resolved = attempt_sub(&parent.enqueued_permit_decreases, num_permits, 0);

        if let Some(mut permit) = self.permit.take() {
            if decreases_resolved > 0 {
                // Split off the portion consumed by enqueued decreases and forget it;
                // the remainder drops normally, returning permits to the semaphore.
                if let Some(p) = permit.split(decreases_resolved as usize) {
                    p.forget();
                } else {
                    debug_assert!(false, "Failed to split permit; mismatch in self.num_permits.");
                }
            }
        } else {
            // The attempt_sub above should guarantee this, but let's make it explicit.
            debug_assert_le!(decreases_resolved, num_permits);

            // Virtual permit (from increment_total_permits): release non-consumed
            // permits into the semaphore.
            let to_return = (num_permits - decreases_resolved) as usize;
            if to_return > 0 {
                parent.semaphore.add_permits(to_return);
            }
        }
    }
}

impl AdjustableSemaphore {
    pub fn new(initial_permits: u64, permit_range: (u64, u64)) -> Arc<Self> {
        debug_assert!(permit_range.0 <= permit_range.1);
        debug_assert!(permit_range.0 <= initial_permits);
        debug_assert!(initial_permits <= permit_range.1);

        let basis = Self::compute_basis(permit_range.1);
        let min_physical = permit_range.0.div_ceil(basis);
        let max_physical = permit_range.1.div_ceil(basis);
        let initial_physical = initial_permits.div_ceil(basis).clamp(min_physical, max_physical);

        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(initial_physical as usize)),
            total_permits: AtomicU64::new(initial_physical),
            enqueued_permit_decreases: AtomicU64::new(0),
            min_physical_permits: min_physical,
            max_physical_permits: max_physical,
            basis,
            adjustment_lock: Mutex::new(()),
        })
    }

    pub fn total_permits(&self) -> u64 {
        self.total_permits.load(Ordering::Relaxed) * self.basis
    }

    pub fn available_permits(&self) -> u64 {
        self.semaphore.available_permits() as u64 * self.basis
    }

    pub fn active_permits(&self) -> u64 {
        (self.total_permits.load(Ordering::Relaxed) + self.enqueued_permit_decreases.load(Ordering::Relaxed))
            .saturating_sub(self.semaphore.available_permits() as u64)
            * self.basis
    }

    /// The scaling basis. Each internal permit represents this many logical
    /// permits. On 64-bit platforms this is 1 for any practical permit count.
    pub fn basis(&self) -> u64 {
        self.basis
    }

    /// Acquire a single permit.
    pub async fn acquire(self: &Arc<Self>) -> Result<AdjustableSemaphorePermit, AcquireError> {
        self.acquire_many(1).await
    }

    /// Acquire `n` logical permits. The request is scaled and clamped to the
    /// total permit count so that a single acquire can always succeed once
    /// enough permits are freed.
    pub async fn acquire_many(self: &Arc<Self>, n: u64) -> Result<AdjustableSemaphorePermit, AcquireError> {
        let physical = self.to_physical_acquire(n);
        let permit = self.semaphore.clone().acquire_many_owned(physical).await?;
        Ok(AdjustableSemaphorePermit {
            permit: Some(permit),
            num_physical_permits: physical,
            parent: self.clone(),
        })
    }

    /// Decrement the total number of permits by up to `n` (logical) down to
    /// the minimum bound.  Note that this does not affect any permits currently
    /// issued; in the case where all permits are currently issued, no new
    /// permits will be issued until the adjustment has been resolved.
    ///
    /// Returns the logical amount decreased, or `None` if no adjustment occurred.
    pub fn decrement_total_permits(&self, n: u64) -> Option<u64> {
        let lock = self.adjustment_lock.lock().unwrap();
        self.decrement_total_permits_impl(lock, n)
    }

    /// Adjust total permits downward to `target` (logical) if the current total
    /// is above it. Returns the logical amount that was requested to be
    /// decreased, or `None` if no adjustment was needed.
    ///
    /// Acquires an internal lock to serialize with other target-based
    /// adjustments, preventing read-then-modify races. The underlying
    /// `decrement_total_permits` handles clamping at the minimum bound.
    pub fn decrement_permits_to_target(&self, target: u64) -> Option<u64> {
        let lock = self.adjustment_lock.lock().unwrap();
        let current = self.total_permits();
        if target >= current {
            return None;
        }
        let requested_decrease = current - target;
        self.decrement_total_permits_impl(lock, requested_decrease)
    }

    fn decrement_total_permits_impl(&self, _lock: MutexGuard<'_, ()>, n: u64) -> Option<u64> {
        let physical_n = n.div_ceil(self.basis);
        if physical_n == 0 {
            return None;
        }

        let removed = attempt_sub(&self.total_permits, physical_n, self.min_physical_permits);
        if removed == 0 {
            return None;
        }

        if let Ok(permit) = self.semaphore.clone().try_acquire_many_owned(removed as u32) {
            permit.forget();
        } else {
            self.enqueued_permit_decreases.fetch_add(removed, Ordering::Relaxed);
        }

        Some(removed * self.basis)
    }

    /// Increment the total number of permits by up to `n` (logical) up to
    /// the maximum bound.
    ///
    /// Returns a permit holding the newly added capacity, or `None`
    /// if no permits could be added (already at max). The permits enter the
    /// semaphore when the returned permit is dropped.  This allows a user to
    /// acquire a permit immediately that bypasses the FIFO queue so the caller
    /// can hold them without contention.  If the returned permit is dropped immediately,
    /// then this simply increments the available permits in the semaphore.
    pub fn increment_total_permits(self: &Arc<Self>, n: u64) -> Option<AdjustableSemaphorePermit> {
        let lock = self.adjustment_lock.lock().unwrap();
        self.increment_total_permits_impl(lock, n)
    }

    /// Adjust total permits upward to `target` (logical) if the current total
    /// is below it. Returns a virtual permit holding the increase, or `None`
    /// if no adjustment was needed or possible.
    ///
    /// Acquires an internal lock to serialize with other target-based
    /// adjustments, preventing read-then-modify races. The underlying
    /// `increment_total_permits` handles clamping at the maximum bound.
    pub fn increment_permits_to_target(self: &Arc<Self>, target: u64) -> Option<AdjustableSemaphorePermit> {
        let lock = self.adjustment_lock.lock().unwrap();
        let current = self.total_permits();
        if target <= current {
            return None;
        }
        self.increment_total_permits_impl(lock, target - current)
    }

    fn increment_total_permits_impl(
        self: &Arc<Self>,
        _lock: MutexGuard<'_, ()>,
        n: u64,
    ) -> Option<AdjustableSemaphorePermit> {
        let physical_n = n.div_ceil(self.basis);
        if physical_n == 0 {
            return None;
        }

        let added = attempt_add(&self.total_permits, physical_n, self.max_physical_permits);
        if added == 0 {
            return None;
        }

        let cancelled = attempt_sub(&self.enqueued_permit_decreases, added, 0);
        let to_hold = (added - cancelled) as u32;

        Some(AdjustableSemaphorePermit {
            permit: None,
            num_physical_permits: to_hold,
            parent: self.clone(),
        })
    }

    /// Computes the smallest power-of-two basis such that
    /// `ceil(max_permits / basis)` fits within the platform semaphore limit.
    fn compute_basis(max_permits: u64) -> u64 {
        let mut basis: u64 = 1;
        while max_permits.div_ceil(basis) > PERMIT_LIMIT {
            basis *= 2;
        }
        basis
    }

    /// Scales a logical permit count to a physical count for acquire,
    /// clamped to `[1, total_physical_permits]`.
    fn to_physical_acquire(&self, n: u64) -> u32 {
        let total = self.total_permits.load(Ordering::Relaxed).max(1);
        n.div_ceil(self.basis).clamp(1, total) as u32
    }

    /// Creates an `AdjustableSemaphore` with a forced basis, for testing the
    /// scaling logic on platforms where the automatic basis would be 1.
    #[cfg(test)]
    fn with_forced_basis(initial: u64, min: u64, max: u64, basis: u64) -> Arc<Self> {
        assert!(basis > 0, "basis must be greater than zero");
        let min_physical_permits = min.div_ceil(basis);
        let max_physical_permits = max.div_ceil(basis).min(PERMIT_LIMIT);
        let initial_physical = initial.div_ceil(basis).clamp(min_physical_permits, max_physical_permits);

        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(initial_physical as usize)),
            total_permits: AtomicU64::new(initial_physical),
            enqueued_permit_decreases: AtomicU64::new(0),
            min_physical_permits,
            max_physical_permits,
            basis,
            adjustment_lock: Mutex::new(()),
        })
    }
}

/// Attempts to add up to `n`, clamped at `max_value`. Returns the actual
/// amount added.
#[inline]
fn attempt_add(v: &AtomicU64, n: u64, max_value: u64) -> u64 {
    match v.fetch_update(SeqCst, SeqCst, |x| {
        if x >= max_value {
            None
        } else {
            Some(x.saturating_add(n).min(max_value))
        }
    }) {
        Ok(old) => old.saturating_add(n).min(max_value) - old,
        Err(_) => 0,
    }
}

/// Attempts to subtract up to `n`, clamped at `min_value`. Returns the actual
/// amount subtracted.
#[inline]
fn attempt_sub(v: &AtomicU64, n: u64, min_value: u64) -> u64 {
    match v.fetch_update(SeqCst, SeqCst, |x| {
        if x <= min_value {
            None
        } else {
            Some(x.saturating_sub(n).max(min_value))
        }
    }) {
        Ok(old) => old - old.saturating_sub(n).max(min_value),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use more_asserts::{assert_ge, assert_le};
    use rand::prelude::*;
    use tokio::sync::Barrier;
    use tokio::task::JoinSet;

    use super::*;

    // ── Bounds and adjustment (parameterized over basis) ──────────

    #[tokio::test]
    async fn test_bounds_and_adjustment() {
        for basis in [1u64, 2] {
            let sem = AdjustableSemaphore::with_forced_basis(6, 2, 12, basis);
            assert_eq!(sem.total_permits(), 6);

            assert!(sem.increment_total_permits(4).is_some());
            assert_eq!(sem.total_permits(), 10);

            // Clamped at max
            assert!(sem.increment_total_permits(100).is_some());
            assert_eq!(sem.total_permits(), 12);
            assert!(sem.increment_total_permits(2).is_none());

            // Decrement by N, clamped at min
            assert_eq!(sem.decrement_total_permits(4), Some(4));
            assert_eq!(sem.total_permits(), 8);
            assert_eq!(sem.decrement_total_permits(100), Some(6));
            assert_eq!(sem.total_permits(), 2);
            assert!(sem.decrement_total_permits(2).is_none());

            // Rebalance back up
            assert!(sem.increment_total_permits(4).is_some());
            assert_eq!(sem.total_permits(), 6);

            // Target-based APIs use the same lock-protected adjustment path.
            assert!(sem.increment_permits_to_target(10).is_some());
            assert_eq!(sem.total_permits(), 10);
            assert!(sem.increment_permits_to_target(10).is_none());

            assert_eq!(sem.decrement_permits_to_target(6), Some(4));
            assert_eq!(sem.total_permits(), 6);
            assert!(sem.decrement_permits_to_target(6).is_none());

            // Target below min clips to min and returns the actual decrease.
            assert_eq!(sem.decrement_permits_to_target(0), Some(4));
            assert_eq!(sem.total_permits(), 2);

            // Restore to max via target-based increase.
            assert!(sem.increment_permits_to_target(12).is_some());
            assert_eq!(sem.total_permits(), 12);
        }
    }

    // ── Acquire, release, and clamping ────────────────────────────

    #[tokio::test]
    async fn test_acquire_and_release() {
        for basis in [1u64, 2] {
            let sem = AdjustableSemaphore::with_forced_basis(1024, 0, 1024, basis);
            assert_eq!(sem.available_permits(), 1024);

            let p1 = sem.acquire_many(256).await.unwrap();
            assert_eq!(p1.num_permits(), 256);
            assert_eq!(sem.available_permits(), 768);

            let p2 = sem.acquire_many(512).await.unwrap();
            assert_eq!(sem.available_permits(), 256);

            drop(p1);
            assert_eq!(sem.available_permits(), 512);
            drop(p2);
            assert_eq!(sem.available_permits(), 1024);

            // Acquire all, release via scope
            {
                let _p = sem.acquire_many(1024).await.unwrap();
                assert_eq!(sem.available_permits(), 0);
            }
            assert_eq!(sem.available_permits(), 1024);

            // Acquire > total is clamped to total
            let _p = sem.acquire_many(5000).await.unwrap();
            assert_eq!(sem.available_permits(), 0);
        }
    }

    // ── Enqueued decrease resolution on drop ──────────────────────

    #[tokio::test]
    async fn test_enqueued_decrease_resolution() {
        for basis in [1u64, 2] {
            // Single-permit enqueued resolution
            let sem = AdjustableSemaphore::with_forced_basis(4, 2, 6, basis);

            let p1 = sem.acquire_many(2).await.unwrap();
            let p2 = sem.acquire_many(2).await.unwrap();
            assert_eq!(sem.available_permits(), 0);

            assert!(sem.decrement_total_permits(2).is_some());
            assert_eq!(sem.total_permits(), 2);

            drop(p1); // resolves 1 enqueued decrease
            assert_eq!(sem.available_permits(), 0);

            drop(p2);
            assert_eq!(sem.available_permits(), 2);

            // Multi-permit enqueued resolution (single large permit)
            let sem = AdjustableSemaphore::with_forced_basis(1024, 0, 1024, basis);
            let p = sem.acquire_many(1024).await.unwrap();
            assert!(sem.decrement_total_permits(512).is_some());
            assert_eq!(sem.total_permits(), 512);

            drop(p);
            assert_eq!(sem.available_permits(), 512);
        }
    }

    // ── Increment cancels enqueued ────────────────────────────────

    #[tokio::test]
    async fn test_increment_cancels_enqueued() {
        for basis in [1u64, 2] {
            let sem = AdjustableSemaphore::with_forced_basis(4, 0, 10, basis);

            let p1 = sem.acquire_many(2).await.unwrap();
            let p2 = sem.acquire_many(2).await.unwrap();

            assert!(sem.decrement_total_permits(2).is_some());
            assert_eq!(sem.total_permits(), 2);

            let vp = sem.increment_total_permits(2).unwrap();
            assert_eq!(vp.num_permits(), 0);
            assert_eq!(sem.total_permits(), 4);
            drop(vp);

            drop(p1);
            assert_eq!(sem.available_permits(), 2);
            drop(p2);
            assert_eq!(sem.available_permits(), 4);
        }
    }

    // ── Virtual permit behavior ───────────────────────────────────

    #[tokio::test]
    async fn test_virtual_permit() {
        for basis in [1u64, 2] {
            let sem = AdjustableSemaphore::with_forced_basis(4, 0, 20, basis);

            // Virtual permit holds new capacity away from the semaphore
            let vp = sem.increment_total_permits(6).unwrap();
            assert_eq!(sem.total_permits(), 10);
            assert_eq!(vp.num_permits(), 6);
            assert_eq!(sem.available_permits(), 4);

            drop(vp);
            assert_eq!(sem.available_permits(), 10);

            // Incremental increment-then-acquire loop
            let sem = AdjustableSemaphore::with_forced_basis(0, 0, 22, basis);
            let mut permits = Vec::new();
            for i in 0..10u64 {
                assert_eq!(sem.available_permits(), 0);
                assert_eq!(sem.total_permits(), i * 2);
                sem.increment_total_permits(2);
                permits.push(sem.acquire_many(2).await.unwrap());
            }
            for i in 0..10u64 {
                assert_eq!(sem.available_permits(), i * 2);
                permits.pop();
            }
        }
    }

    // ── Permit split ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_permit_split() {
        for basis in [1u64, 2] {
            let sem = AdjustableSemaphore::with_forced_basis(10, 0, 10, basis);

            // Split acquired permit
            let mut p = sem.acquire_many(6).await.unwrap();
            let p2 = p.split(2).unwrap();
            assert_eq!(p.num_permits(), 4);
            assert_eq!(p2.num_permits(), 2);
            drop(p2);
            assert_eq!(sem.available_permits(), 6);
            drop(p);
            assert_eq!(sem.available_permits(), 10);

            // Split all permits off
            let mut p = sem.acquire_many(6).await.unwrap();
            let p2 = p.split(6).unwrap();
            assert_eq!(p.num_permits(), 0);
            assert_eq!(p2.num_permits(), 6);
            drop(p);
            assert_eq!(sem.available_permits(), 4);
            drop(p2);
            assert_eq!(sem.available_permits(), 10);

            // Split more than held → None
            let mut p = sem.acquire_many(4).await.unwrap();
            assert!(p.split(6).is_none());
            assert_eq!(p.num_permits(), 4);
            drop(p);
        }
    }

    // ── Virtual permit split ──────────────────────────────────────

    #[tokio::test]
    async fn test_virtual_permit_split() {
        for basis in [1u64, 2] {
            let sem = AdjustableSemaphore::with_forced_basis(4, 0, 20, basis);

            let mut vp = sem.increment_total_permits(8).unwrap();
            assert_eq!(sem.total_permits(), 12);
            assert_eq!(sem.available_permits(), 4);
            assert_eq!(vp.num_permits(), 8);

            let vp2 = vp.split(2).unwrap();
            assert_eq!(vp.num_permits(), 6);
            assert_eq!(vp2.num_permits(), 2);

            drop(vp2);
            assert_eq!(sem.available_permits(), 6);

            drop(vp);
            assert_eq!(sem.available_permits(), 12);
        }
    }

    // ── Basis computation ─────────────────────────────────────────

    #[test]
    fn test_basis_computation() {
        assert_eq!(AdjustableSemaphore::new(1024, (0, 1024)).basis(), 1);
        assert_eq!(AdjustableSemaphore::new(PERMIT_LIMIT, (0, PERMIT_LIMIT)).basis(), 1);
        assert_eq!(AdjustableSemaphore::new(PERMIT_LIMIT + 1, (0, PERMIT_LIMIT + 1)).basis(), 2);
    }

    // ── Forced basis rounding ─────────────────────────────────────

    #[test]
    fn test_forced_basis_rounding() {
        // Non-exact: ceil(1000/300) = 4 physical, 4*300 = 1200 logical
        let sem = AdjustableSemaphore::with_forced_basis(1000, 0, 1000, 300);
        assert_eq!(sem.total_permits(), 1200);

        // Exact: 900/300 = 3 physical, 3*300 = 900 logical
        let sem = AdjustableSemaphore::with_forced_basis(900, 0, 900, 300);
        assert_eq!(sem.total_permits(), 900);
    }

    // ── Rounding and physical permits (basis > 1) ─────────────────

    #[tokio::test]
    async fn test_rounding_and_physical_permits() {
        // Acquire rounds up: 1 logical → ceil(1/256) = 1 physical → 256 logical
        let sem = AdjustableSemaphore::with_forced_basis(1024, 0, 1024, 256);
        let p = sem.acquire_many(1).await.unwrap();
        assert_eq!(p.num_permits(), 256);
        assert_eq!(p.num_physical_permits(), 1);
        assert_eq!(sem.available_permits(), 768);
        drop(p);

        // Acquire non-aligned: 250 logical / basis 100 → 3 physical → 300 logical
        let sem = AdjustableSemaphore::with_forced_basis(1000, 0, 1000, 100);
        let p = sem.acquire_many(250).await.unwrap();
        assert_eq!(p.num_permits(), 300);
        assert_eq!(p.num_physical_permits(), 3);
        drop(p);

        // Virtual permit: 512 logical / basis 256 → 2 physical
        let sem = AdjustableSemaphore::with_forced_basis(1024, 0, 2048, 256);
        let vp = sem.increment_total_permits(512).unwrap();
        assert_eq!(vp.num_permits(), 512);
        assert_eq!(vp.num_physical_permits(), 2);
        drop(vp);

        // Split rounds up: 1 logical / basis 100 → 1 physical → 100 logical
        let sem = AdjustableSemaphore::with_forced_basis(500, 0, 500, 100);
        let mut p = sem.acquire_many(500).await.unwrap();
        let p2 = p.split(1).unwrap();
        assert_eq!(p2.num_permits(), 100);
        assert_eq!(p2.num_physical_permits(), 1);
        assert_eq!(p.num_permits(), 400);
        assert_eq!(p.num_physical_permits(), 4);
        drop(p2);
        drop(p);

        // Decrement at min with basis
        let sem = AdjustableSemaphore::with_forced_basis(500, 300, 500, 100);
        assert!(sem.decrement_total_permits(300).is_some());
        assert_eq!(sem.total_permits(), 300);
        assert!(sem.decrement_total_permits(1).is_none());
    }

    // ── Zero capacity ─────────────────────────────────────────────

    #[test]
    fn test_zero_capacity() {
        let sem = AdjustableSemaphore::new(0, (0, 0));
        assert_eq!(sem.total_permits(), 0);
        assert_eq!(sem.available_permits(), 0);
    }

    // ── Concurrent stress test ───────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_stress() {
        const TASKS: usize = 50;
        const OPS_PER_TASK: usize = 1000;

        const MIN_PERMITS: u64 = 10;
        const MAX_PERMITS: u64 = 50;

        let sem = AdjustableSemaphore::new(30, (MIN_PERMITS, MAX_PERMITS));

        let mut js = JoinSet::new();
        let barrier = Arc::new(Barrier::new(TASKS + 1));

        for t in 0..TASKS {
            let sem = sem.clone();
            let mut rng = SmallRng::seed_from_u64(t as u64);
            let barrier = barrier.clone();

            js.spawn(async move {
                barrier.wait().await;
                for _ in 0..OPS_PER_TASK {
                    if rng.random_bool(0.1) {
                        sem.increment_total_permits(1);
                    }

                    if rng.random_bool(0.1) {
                        let _ = sem.decrement_total_permits(1);
                    }

                    let p = sem.acquire().await;
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    drop(p);

                    assert!(sem.total_permits() >= MIN_PERMITS);
                    assert!(sem.total_permits() <= MAX_PERMITS);
                    assert!(sem.available_permits() <= MAX_PERMITS);
                }
            });
        }

        barrier.wait().await;

        js.join_all().await;

        let final_permits = sem.total_permits();
        assert_le!(final_permits, MAX_PERMITS);
        assert_ge!(final_permits, MIN_PERMITS);
        let avail_permits = sem.available_permits();
        assert_eq!(avail_permits, final_permits);
    }

    // ── Concurrent stress with acquire_many ──────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_stress_acquire_many() {
        const TASKS: usize = 30;
        const OPS_PER_TASK: usize = 500;

        const MIN_PERMITS: u64 = 100;
        const MAX_PERMITS: u64 = 500;

        let sem = AdjustableSemaphore::new(300, (MIN_PERMITS, MAX_PERMITS));

        let mut js = JoinSet::new();
        let barrier = Arc::new(Barrier::new(TASKS + 1));

        for t in 0..TASKS {
            let sem = sem.clone();
            let mut rng = SmallRng::seed_from_u64(t as u64);
            let barrier = barrier.clone();

            js.spawn(async move {
                barrier.wait().await;
                for _ in 0..OPS_PER_TASK {
                    if rng.random_bool(0.05) {
                        sem.increment_total_permits(rng.random_range(1..=10));
                    }

                    if rng.random_bool(0.05) {
                        let _ = sem.decrement_total_permits(rng.random_range(1..=10));
                    }

                    let amount = rng.random_range(1..=50);
                    let p = sem.acquire_many(amount).await;
                    tokio::time::sleep(Duration::from_micros(50)).await;
                    drop(p);

                    assert!(sem.total_permits() >= MIN_PERMITS);
                    assert!(sem.total_permits() <= MAX_PERMITS);
                }
            });
        }

        barrier.wait().await;

        js.join_all().await;

        let final_permits = sem.total_permits();
        assert_le!(final_permits, MAX_PERMITS);
        assert_ge!(final_permits, MIN_PERMITS);
    }
}
