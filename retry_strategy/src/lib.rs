use tokio_retry::strategy::{jitter, FibonacciBackoff};
use tokio_retry::{Action, RetryIf};

#[derive(Debug, Clone, Copy)]
pub struct RetryStrategy {
    num_retries: usize,
    base_backoff_ms: u64,
}

impl RetryStrategy {
    pub fn new(num_retries: usize, base_backoff_ms: u64) -> Self {
        Self {
            num_retries,
            base_backoff_ms,
        }
    }

    pub async fn retry<A, R, E, C: for<'r> FnMut(&'r E) -> bool>(
        &self,
        action: A,
        retryable: C,
    ) -> Result<R, E>
    where
        A: Action<Item = R, Error = E>,
    {
        let strategy = FibonacciBackoff::from_millis(self.base_backoff_ms)
            .map(jitter)
            .take(self.num_retries);
        RetryIf::spawn(strategy, action, retryable).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::sync::atomic::{AtomicU32, Ordering};
    #[tokio::test]
    async fn test_retry_failures() {
        let error_count = AtomicU32::new(0);
        let retry_count = AtomicU32::new(0);
        let strategy = RetryStrategy::new(3, 1); // 1ms
        let e = strategy
            .retry(
                || async {
                    retry_count.fetch_add(1, Ordering::Relaxed);
                    Err::<(), anyhow::Error>(anyhow!("moof"))
                },
                |_| {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    true
                },
            )
            .await;
        assert!(e.is_err());
        // retries 3 times so 4 attempts
        assert_eq!(retry_count.load(Ordering::Relaxed), 4);
        assert!(error_count.load(Ordering::Relaxed) >= 3);
    }

    #[tokio::test]
    async fn test_retry_success() {
        let error_count = AtomicU32::new(0);
        let retry_count = AtomicU32::new(0);
        let strategy = RetryStrategy::new(3, 1); // 1ms
        let e = strategy
            .retry(
                || async {
                    retry_count.fetch_add(1, Ordering::Relaxed);
                    Ok::<(), anyhow::Error>(())
                },
                |_| {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    true
                },
            )
            .await;
        assert!(e.is_ok());
        assert_eq!(retry_count.load(Ordering::Relaxed), 1);
        assert_eq!(error_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_retry_eventual_success() {
        let error_count = AtomicU32::new(0);
        let retry_count = AtomicU32::new(0);
        let strategy = RetryStrategy::new(3, 1); // 1ms
        let e = strategy
            .retry(
                || async {
                    if retry_count.fetch_add(1, Ordering::Relaxed) == 2 {
                        Ok(())
                    } else {
                        Err(anyhow!("moof"))
                    }
                },
                |_| {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    true
                },
            )
            .await;
        assert!(e.is_ok());
        assert_eq!(retry_count.load(Ordering::Relaxed), 3);
        assert!(error_count.load(Ordering::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn test_do_not_retry() {
        let error_count = AtomicU32::new(0);
        let retry_count = AtomicU32::new(0);
        let strategy = RetryStrategy::new(3, 1); // 1ms
        let e = strategy
            .retry(
                || async {
                    retry_count.fetch_add(1, Ordering::Relaxed);
                    Err::<(), anyhow::Error>(anyhow!("moof"))
                },
                |_| {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    false
                },
            )
            .await;
        assert!(e.is_err());
        assert_eq!(retry_count.load(Ordering::Relaxed), 1);
        assert!(error_count.load(Ordering::Relaxed) == 1);
    }
}
