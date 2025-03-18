use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::{AbortHandle, JoinError, JoinSet as TokioJoinSet};

struct JoinSet<T> {
    inner: TokioJoinSet<T>,
    semaphore: Arc<Semaphore>,
}

impl<T: Send + 'static> JoinSet<T> {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            inner: TokioJoinSet::new(),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T> + Unpin + Send + 'static,
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

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
