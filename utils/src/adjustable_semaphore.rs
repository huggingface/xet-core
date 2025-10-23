use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{AcquireError, Semaphore};

/// An adjustable semaphore in which the total number of permits can be adjusted at any time
/// between a minimum and a maximum bound.  
///
/// Unlike the tokio Semaphore, decreasing the number of permits may be done at any time and
/// are resolved lazily if needed; any permits currently issued remain valid, but no new permits
/// are issued until any requested decreases are resolved.
pub struct AdjustableSemaphore {
    semaphore: Arc<Semaphore>,
    total_permits: AtomicUsize,
    enqueued_permit_decreases: AtomicUsize,
    min_permits: usize,
    max_permits: usize,
}

/// A permit issued by the AdjustableSemaphore.  On drop, this attempts to
/// resolve any enqueued permit decrease if one is needed.
pub struct AdjustableSemaphorePermit {
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    parent: Arc<AdjustableSemaphore>,
}

impl Drop for AdjustableSemaphorePermit {
    fn drop(&mut self) {
        // Check to see if we need to forget this permit due to enqueued permit decreases.
        if attempt_decrement(&self.parent.enqueued_permit_decreases, 0) {
            self.permit.take().unwrap().forget();
        }
    }
}

impl AdjustableSemaphore {
    pub fn new(initial_permits: usize, permit_range: (usize, usize)) -> Arc<Self> {
        debug_assert!(permit_range.0 <= permit_range.1);
        debug_assert!(permit_range.0 <= initial_permits);
        debug_assert!(initial_permits <= permit_range.1);

        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(initial_permits)),
            total_permits: initial_permits.into(),
            enqueued_permit_decreases: 0.into(),
            min_permits: permit_range.0,
            max_permits: permit_range.1,
        })
    }

    pub fn total_permits(&self) -> usize {
        self.total_permits.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn active_permits(&self) -> usize {
        // The number of total permits minus the available permits, not counting for the enqueud permit decreases.
        (self.total_permits() + self.enqueued_permit_decreases.load(std::sync::atomic::Ordering::Relaxed))
            .saturating_sub(self.available_permits())
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<AdjustableSemaphorePermit, AcquireError> {
        // A few debug mode consistency checks.
        debug_assert!(self.semaphore.available_permits() <= self.max_permits);

        // To ensure that the fairness property of the enclosed tokio semaphore is respected,
        // this function must only call this class and not attempt to resolve anything else.
        //
        // As a result, any decreases are resolved by the Drop trait of this permit, which may
        // mean that permit is forgotten to resolve an outstanding decrease.
        let permit = self.semaphore.clone().acquire_owned().await?;

        Ok(AdjustableSemaphorePermit {
            permit: Some(permit),
            parent: self.clone(),
        })
    }

    /// Decrement the total number of available permits down to the minimum bound.  Note that this does not
    /// affect any permits currently issued; In the case where all permits are currently issued, no new permits will be
    /// issued until this adjustment has been resolved.
    ///
    /// Returns true if the total number of permits is greater than the minimum and the adjustment has or will be
    /// applied, otherwise returns false.
    pub fn decrement_total_permits(&self) -> bool {
        // Make sure we can do the decrease.
        if !attempt_decrement(&self.total_permits, self.min_permits) {
            return false;
        }

        if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
            permit.forget();
        } else {
            self.enqueued_permit_decreases.fetch_add(1, Ordering::Relaxed);
        }

        true
    }

    /// Increment the total number of available permits up to the maximum bound.
    ///
    /// Returns true if the total number of permits is less than the maxiumum and the adjustment has been applied.
    /// otherwise returns false.
    pub fn increment_total_permits(&self) -> bool {
        // Make sure we can do the increase.
        if !attempt_increment(&self.total_permits, self.max_permits) {
            return false;
        }

        // Are we able to do this by decrementing the decreases enqueued?
        if attempt_decrement(&self.enqueued_permit_decreases, 0) {
            return true;
        }

        // Resolve this in the semaphore itself
        self.semaphore.add_permits(1);
        true
    }
}

// Utility functions for adjusting the semaphores

/// Attempts to decrease an AtomicUsize down to a.  Returns true if successful, otherwise 1 if the value was already 0.
#[inline]
fn attempt_decrement(v: &AtomicUsize, min_value: usize) -> bool {
    v.fetch_update(SeqCst, SeqCst, |x| {
        debug_assert!(x >= min_value);
        if x > min_value {
            Some(x - 1)
        } else {
            None
        }
    })
    .is_ok()
}

/// Attempts to increase an AtomicUsize up to 0.  Returns true if successful, otherwise 1 if the value was already 0.
#[inline]
fn attempt_increment(v: &AtomicUsize, max_value: usize) -> bool {
    v.fetch_update(SeqCst, SeqCst, |x| {
        debug_assert!(x <= max_value);

        if x < max_value {
            Some(x + 1)
        } else {
            None
        }
    })
    .is_ok()
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use rand::prelude::*;
    use tokio::sync::Barrier;
    use tokio::task::JoinSet;

    use super::*;

    #[tokio::test]
    async fn test_basic_bounds() {
        let sem = AdjustableSemaphore::new(5, (2, 10));

        assert_eq!(sem.total_permits(), 5);
        assert!(sem.increment_total_permits()); // 6
        assert_eq!(sem.total_permits(), 6);

        for _ in 0..4 {
            assert!(sem.increment_total_permits());
        }
        assert_eq!(sem.total_permits(), 10);
        assert!(!sem.increment_total_permits()); // max reached
    }

    #[tokio::test]
    async fn test_decrement_behavior() {
        let sem = AdjustableSemaphore::new(3, (1, 5));

        let _p1 = sem.acquire().await;
        let _p2 = sem.acquire().await;
        let _p3 = sem.acquire().await;

        // Now all permits are issued
        assert_eq!(sem.available_permits(), 0);

        assert!(sem.decrement_total_permits()); // will enqueue decrease
        assert_eq!(sem.total_permits(), 2);
        assert_eq!(sem.available_permits(), 0);

        drop(_p1); // this should trigger the forget on drop
        assert_eq!(sem.available_permits(), 0); // Still none, as all 3 were acquired
        drop(_p2); // Second drop should finally allow a new permit

        let p4 = sem.acquire().await;
        drop(p4);
    }

    #[tokio::test]
    async fn test_increment() {
        let mut permits = Vec::new();

        let sem = AdjustableSemaphore::new(0, (0, 11));

        for i in 0..10 {
            assert_eq!(sem.available_permits(), 0);
            assert_eq!(sem.total_permits(), i);
            sem.increment_total_permits();

            let p = sem.acquire().await.unwrap();
            permits.push(p);
        }

        for i in 0..10 {
            assert_eq!(sem.available_permits(), i);
            assert_eq!(sem.total_permits(), 10);

            permits.pop();
        }
    }

    #[tokio::test]
    async fn test_rebalancing_up_down() {
        let sem = AdjustableSemaphore::new(3, (1, 5));

        for _ in 0..2 {
            assert!(sem.increment_total_permits());
        }
        assert_eq!(sem.total_permits(), 5);

        for _ in 0..4 {
            assert!(sem.decrement_total_permits());
        }
        assert_eq!(sem.total_permits(), 1);
        assert!(!sem.decrement_total_permits());
    }

    #[tokio::test]
    async fn test_permit_forget_on_drop() {
        let sem = AdjustableSemaphore::new(3, (1, 3));

        let p1 = sem.acquire().await;
        let p2 = sem.acquire().await;
        let p3 = sem.acquire().await;

        assert!(sem.decrement_total_permits()); // enqueue decrease

        assert_eq!(sem.total_permits(), 2);
        assert_eq!(sem.available_permits(), 0);

        drop(p1); // should forget one

        assert_eq!(sem.available_permits(), 0); // still 2 permits in use
        assert_eq!(sem.total_permits(), 2);

        drop(p2);
        assert_eq!(sem.available_permits(), 1); // only 2 total now, 1 should be available.

        drop(p3);
        assert_eq!(sem.available_permits(), 2); // only 2 total now, 1 should be available.
    }

    // Runs a simulation that stress tests all the increasing and decreasing to make sure nothing
    // is wrong and we're bulletproof.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_stress() {
        const TASKS: usize = 50;
        const OPS_PER_TASK: usize = 1000;

        const MIN_PERMITS: usize = 10;
        const MAX_PERMITS: usize = 50;

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
                        sem.increment_total_permits();
                    }

                    if rng.random_bool(0.1) {
                        sem.decrement_total_permits();
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

        // This should start everything going at once.
        barrier.wait().await;

        js.join_all().await;

        let final_permits = sem.total_permits();
        assert!(final_permits >= MIN_PERMITS && final_permits <= MAX_PERMITS);

        let avail_permits = sem.available_permits();
        assert_eq!(avail_permits, final_permits);
    }
}
