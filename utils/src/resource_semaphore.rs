use std::sync::Arc;

use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

/// A resource-based concurrency limiter backed by a tokio semaphore.
///
/// `ResourceSemaphore` caps the total amount of a resource (e.g. bytes) that may
/// be in use concurrently. Callers acquire and release capacity in resource
/// units; the limiter transparently maps these to semaphore permits.
///
/// On platforms where the semaphore natively supports large permit counts
/// (64-bit), each resource unit maps directly to one permit. On platforms with
/// smaller permit limits (32-bit / wasm), the limiter automatically scales
/// requests using the smallest power-of-two basis that keeps the internal
/// permit count within range.
///
/// Only owned operations are supported so the limiter can be shared across
/// tasks via `Arc` cloning.
#[derive(Debug, Clone)]
pub struct ResourceSemaphore {
    semaphore: Arc<Semaphore>,
    total_permits: u32,
    basis: u64,
}

/// Maximum number of internal permits. Capped at `u32::MAX` since
/// `acquire_many_owned` takes `u32`.
const PERMIT_LIMIT: u64 = {
    let sem_max = Semaphore::MAX_PERMITS as u64;
    let u32_max = u32::MAX as u64;
    if sem_max < u32_max {
        // Using if statements since it's a const expression.
        sem_max
    } else {
        u32_max
    }
};

impl ResourceSemaphore {
    /// Creates a new `ResourceSemaphore` with the given total capacity in
    /// resource units.
    ///
    /// The internal scaling basis is computed automatically: on 64-bit
    /// platforms it is 1 for any practical capacity; on 32-bit platforms
    /// it is the smallest power of two that fits the capacity into the
    /// semaphore's permit range.
    pub fn new(capacity: u64) -> Self {
        let basis = Self::compute_basis(capacity);
        let total_permits = capacity.div_ceil(basis) as u32;

        Self {
            semaphore: Arc::new(Semaphore::new(total_permits as usize)),
            total_permits,
            basis,
        }
    }

    /// The total capacity of the limiter in resource units.
    ///
    /// This may be slightly larger than the `capacity` passed to [`new`](Self::new)
    /// due to rounding when scaling is active.
    pub fn total_capacity(&self) -> u64 {
        self.total_permits as u64 * self.basis
    }

    /// The currently available capacity in resource units.
    pub fn available(&self) -> u64 {
        self.semaphore.available_permits() as u64 * self.basis
    }

    /// Acquires capacity for `amount` resource units (owned).
    ///
    /// The request is internally scaled and clamped to the total capacity so a
    /// single acquire can always succeed once enough capacity is freed.
    pub async fn acquire_owned(&self, amount: u64) -> Result<OwnedSemaphorePermit, AcquireError> {
        let n = self.scale(amount);
        self.semaphore.clone().acquire_many_owned(n).await
    }

    /// Computes the smallest power-of-two basis such that
    /// `ceil(capacity / basis)` fits within [`PERMIT_LIMIT`].
    fn compute_basis(capacity: u64) -> u64 {
        let mut basis: u64 = 1;
        while capacity.div_ceil(basis) > PERMIT_LIMIT {
            basis *= 2;
        }
        basis
    }

    /// Scales a raw resource amount to the number of permits to acquire,
    /// clamped to `[1, total_permits]`.
    fn scale(&self, amount: u64) -> u32 {
        amount.div_ceil(self.basis).clamp(1, self.total_permits as u64) as u32
    }

    /// Creates a `ResourceSemaphore` with an explicit basis, for testing the
    /// scaling logic on platforms where the automatic basis would be 1.
    #[cfg(test)]
    fn with_basis(capacity: u64, basis: u64) -> Self {
        assert!(basis > 0, "basis must be greater than zero");
        let total_permits = capacity.div_ceil(basis).min(PERMIT_LIMIT) as u32;

        Self {
            semaphore: Arc::new(Semaphore::new(total_permits as usize)),
            total_permits,
            basis,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Construction & capacity ───────────────────────────────────

    #[test]
    fn test_new_basic() {
        let lim = ResourceSemaphore::new(1024);
        assert_eq!(lim.total_capacity(), 1024);
        assert_eq!(lim.available(), 1024);
    }

    #[test]
    fn test_new_large_capacity() {
        let two_gb = 2 * 1024 * 1024 * 1024u64;
        let lim = ResourceSemaphore::new(two_gb);
        assert_eq!(lim.total_capacity(), two_gb);
    }

    #[test]
    fn test_new_zero_capacity() {
        let lim = ResourceSemaphore::new(0);
        assert_eq!(lim.total_capacity(), 0);
        assert_eq!(lim.available(), 0);
    }

    #[test]
    fn test_compute_basis_above_permit_limit_triggers_scaling() {
        let basis = ResourceSemaphore::compute_basis(PERMIT_LIMIT + 1);
        assert_eq!(basis, 2);
    }

    // ── Scaling logic (via with_basis) ────────────────────────────

    #[test]
    fn test_with_basis_rounds_up() {
        let lim = ResourceSemaphore::with_basis(1000, 300);
        // ceil(1000/300) = 4 permits, 4 * 300 = 1200
        assert_eq!(lim.total_capacity(), 1200);
    }

    #[test]
    fn test_with_basis_exact_division() {
        let lim = ResourceSemaphore::with_basis(900, 300);
        assert_eq!(lim.total_capacity(), 900);
    }

    #[test]
    fn test_with_basis_single_permit() {
        let lim = ResourceSemaphore::with_basis(100, 1000);
        // ceil(100/1000) = 1 permit, 1 * 1000 = 1000
        assert_eq!(lim.total_capacity(), 1000);
    }

    #[test]
    #[should_panic(expected = "basis must be greater than zero")]
    fn test_with_basis_zero_panics() {
        ResourceSemaphore::with_basis(1024, 0);
    }

    #[test]
    fn test_scale_basic() {
        let lim = ResourceSemaphore::with_basis(1024, 256);
        assert_eq!(lim.scale(256), 1);
        assert_eq!(lim.scale(512), 2);
        assert_eq!(lim.scale(1024), 4);
    }

    #[test]
    fn test_scale_rounds_up() {
        let lim = ResourceSemaphore::with_basis(1024, 256);
        assert_eq!(lim.scale(1), 1);
        assert_eq!(lim.scale(257), 2);
        assert_eq!(lim.scale(513), 3);
    }

    #[test]
    fn test_scale_clamps_to_total() {
        let lim = ResourceSemaphore::with_basis(1024, 256);
        assert_eq!(lim.scale(2000), 4);
        assert_eq!(lim.scale(u64::MAX), 4);
    }

    #[test]
    fn test_scale_minimum_is_one() {
        let lim = ResourceSemaphore::with_basis(1024, 256);
        assert_eq!(lim.scale(0), 1);
    }

    // ── compute_basis ─────────────────────────────────────────────

    #[test]
    fn test_compute_basis_small_capacity() {
        assert_eq!(ResourceSemaphore::compute_basis(1024), 1);
    }

    #[test]
    fn test_compute_basis_at_limit() {
        assert_eq!(ResourceSemaphore::compute_basis(PERMIT_LIMIT), 1);
    }

    #[test]
    fn test_compute_basis_just_above_limit() {
        let basis = ResourceSemaphore::compute_basis(PERMIT_LIMIT + 1);
        assert_eq!(basis, 2);
    }

    #[test]
    fn test_compute_basis_large_value() {
        let basis = ResourceSemaphore::compute_basis(u64::MAX);
        assert!((u64::MAX).div_ceil(basis) <= PERMIT_LIMIT);
        // Verify it's a power of two
        assert!(basis.is_power_of_two());
    }

    // ── Acquire / release ─────────────────────────────────────────

    #[tokio::test]
    async fn test_acquire_reduces_available() {
        let lim = ResourceSemaphore::new(1024);
        assert_eq!(lim.available(), 1024);

        let _permit = lim.acquire_owned(256).await.unwrap();
        assert_eq!(lim.available(), 768);

        let _permit2 = lim.acquire_owned(512).await.unwrap();
        assert_eq!(lim.available(), 256);
    }

    #[tokio::test]
    async fn test_acquire_releases_on_drop() {
        let lim = ResourceSemaphore::new(1024);

        {
            let _permit = lim.acquire_owned(1024).await.unwrap();
            assert_eq!(lim.available(), 0);
        }

        assert_eq!(lim.available(), 1024);
    }

    #[tokio::test]
    async fn test_acquire_oversized_request_clamped() {
        let lim = ResourceSemaphore::new(1024);

        // Request more than total capacity; clamped to full capacity
        let _permit = lim.acquire_owned(5000).await.unwrap();
        assert_eq!(lim.available(), 0);
    }

    #[tokio::test]
    async fn test_acquire_small_request_at_least_one_unit() {
        let lim = ResourceSemaphore::new(1024);

        // Even a 1-unit request consumes one basis unit of capacity
        let _permit = lim.acquire_owned(1).await.unwrap();
        assert_eq!(lim.available(), 1023);
    }

    #[tokio::test]
    async fn test_acquire_with_basis_rounding() {
        let lim = ResourceSemaphore::with_basis(1024, 256);

        // 1 byte rounds up to one permit (256 resource units)
        let _permit = lim.acquire_owned(1).await.unwrap();
        assert_eq!(lim.available(), 768);
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let lim = ResourceSemaphore::new(1024);
        let lim2 = lim.clone();

        let _permit = lim.acquire_owned(256).await.unwrap();
        assert_eq!(lim2.available(), 768);
    }

    #[tokio::test]
    async fn test_concurrent_acquire_respects_limit() {
        let lim = ResourceSemaphore::new(512);
        let lim2 = lim.clone();

        let _p1 = lim.acquire_owned(256).await.unwrap();
        let _p2 = lim.acquire_owned(256).await.unwrap();

        assert_eq!(lim2.available(), 0);

        // Spawning a task that tries to acquire should not complete immediately
        let handle = tokio::spawn(async move {
            let _p = lim2.acquire_owned(256).await.unwrap();
        });

        tokio::task::yield_now().await;
        assert!(!handle.is_finished());

        // Dropping a permit should unblock it
        drop(_p1);
        handle.await.unwrap();

        assert!(lim.available() <= 512);
    }
}
