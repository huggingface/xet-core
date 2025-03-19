use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::Semaphore;
use tokio::task::{AbortHandle, JoinError, JoinSet as TokioJoinSet};

pub struct LimitedJoinSet<T> {
    inner: TokioJoinSet<T>,
    semaphore: Arc<Semaphore>,
}

impl<T: 'static> LimitedJoinSet<T> {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            inner: TokioJoinSet::new(),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        let semaphore = self.semaphore.clone();
        self.inner.spawn(async move {
            let _permit = semaphore.acquire().await;
            task.await
        })
    }

    pub fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.inner.try_join_next()
    }

    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.inner.join_next().await
    }

    pub async fn join_all(self) -> Vec<T> {
        self.inner.join_all().await
    }

    pub fn poll_join_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, JoinError>>> {
        self.inner.poll_join_next(cx)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_joinset() {
        let mut join_set = LimitedJoinSet::new(3);

        for i in 0..4 {
            join_set.spawn(async move {
                tokio::time::sleep(Duration::from_millis(10 - i)).await;
                i
            });
        }

        let mut outs = Vec::new();
        while let Some(Ok(value)) = join_set.join_next().await {
            outs.push(value);
        }

        // expect that the task returning 3 was spawned after at least 1 other task finished
        assert_eq!(outs.len(), 4);
        for (i, out) in outs.into_iter().enumerate() {
            if out == 3 {
                assert!(i > 0);
            }
        }
    }
}
