use futures::Stream;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::{JoinError, JoinSet as TokioJoinSet};

struct JoinSet<T> {
    inner: TokioJoinSet<T>,
    max_concurrent: NonZeroUsize,
    tasks_queue: VecDeque<Box<dyn Future<Output = T> + Send + 'static>>,
    reaped: VecDeque<Result<T, JoinError>>,
}

impl<T> JoinSet<T> {
    pub fn new(max_concurrent: NonZeroUsize) -> Self {
        Self {
            inner: TokioJoinSet::new(),
            max_concurrent,
            tasks_queue: VecDeque::new(),
            reaped: VecDeque::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.try_reap();
        if self.inner.len() < self.max_concurrent.get() {
            self.inner.spawn(task);
            return;
        }
        self.tasks_queue.push_back(Box::new(task));
    }

    pub fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.try_reap();
        self.reaped.pop_front()
    }

    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        if let Some(ready) = self.try_join_next() {
            // ready value
            Some(ready)
        } else {
            // none-ready to be reaped, return the next in the inner joinset
            // or a none value if it is empty
            self.inner.join_next().await
        }
    }

    pub fn try_reap(&mut self) {
        // try to retrieve any ready tasks and put them in the reaped queue
        while let Some(reaped_value) = self.inner.try_join_next() {
            self.reaped.push_back(reaped_value);
        }
        // refill inner join set
        while self.inner.len() < self.max_concurrent.get() {
            if let Some(task) = self.tasks_queue.pop_front() {
                self.inner.spawn(task);
            } else {
                break;
            }
        }
    }

    pub fn from_iter<F>(max_concurrent: NonZeroUsize, it: impl Iterator<Item = F>) -> Self
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        let mut res = Self::new(max_concurrent);
        for f in it {
            res.spawn(f);
        }
        res
    }

    pub fn len(&self) -> usize {
        self.inner.len() + self.tasks_queue.len() + self.reaped.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty() && self.reaped.is_empty()
    }
}

impl<T> Stream for JoinSet<T> {
    type Item = Result<T, JoinError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.len() == 0 {
            return Poll::Ready(None);
        }
        if let Some(result) = self.as_ref().try_join_next() {
            return Poll::Ready(Some(result));
        }
        Poll::Pending
    }
}
