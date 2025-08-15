use std::future::Future;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{AcquireError, Semaphore};
use tokio::task::{JoinError, JoinSet};
use tracing::{debug, info_span, Instrument};

use crate::threadpool::next_task_id;

#[derive(Debug, Error)]
pub enum ParutilsError<E: Send + Sync + 'static> {
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Acquire(#[from] AcquireError),
    #[error(transparent)]
    Task(E),
}

/// Runs all the futures in the provided iterator with a maximum concurrency limit.
///
/// This function requires that a permit is acquired from the provided semaphore before any work
/// is done for a future, thus limiting concurrency based on the number of permits in the semaphore.
///
/// Each future in the iterator must return a `Result<T, E>`. If any future returns an error,
/// or if there is a `JoinError` or failure to acquire a semaphore permit, the function will
/// return an error as soon as possible.
///
/// If all tasks complete successfully, the function returns a `Vec<T>` containing the results
/// of the successful futures, in the same order as they were produced by the iterator.
///
/// # Arguments
///
/// * `futures_it` - An iterator of futures, where each future resolves to a `Result<T, E>`.
/// * `max_concurrent` - An `Arc<Semaphore>` that limits the number of concurrent tasks.
///
/// # Type Parameters
///
/// * `Fut` - The type of the futures in the iterator. Each future must output a `Result<T, E>`.
/// * `T` - The type of the successful result produced by each future.
/// * `E` - The type of the error produced by each future.
///
/// # Returns
///
/// A `Result` containing:
/// * `Ok(Vec<T>)` - A vector of successful results if all tasks complete successfully.
/// * `Err(ParutilsError<E>)` - An error if any task fails, a semaphore permit cannot be acquired, or a `JoinError`
///   occurs.
///
/// # Errors
///
/// This function returns a `ParutilsError<E>` in the following cases:
/// * A task returns an error of type `E`.
/// * A semaphore permit cannot be acquired.
/// * A `JoinError` occurs while waiting for a task to complete.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
///
/// use tokio::sync::Semaphore;
/// use xet_threadpool::runner::run_limited_fold_result_with_semaphore;
///
/// #[tokio::main]
/// async fn main() {
///     let semaphore = Arc::new(Semaphore::new(2)); // Limit concurrency to 2 tasks.
///     let futures = (1..=3).map(|n| async move { Ok::<_, ()>(n) });
///
///     let results = run_limited_fold_result_with_semaphore(futures.into_iter(), semaphore).await;
///     assert_eq!(results.unwrap(), vec![1, 2, 3]);
/// }
/// ```
pub async fn run_limited_fold_result_with_semaphore<Fut, T, E>(
    futures_it: impl Iterator<Item = Fut>,
    max_concurrent: Arc<Semaphore>,
) -> Result<Vec<T>, ParutilsError<E>>
where
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    let handle = tokio::runtime::Handle::current();
    let mut js: JoinSet<Result<(usize, T), ParutilsError<E>>> = JoinSet::new();
    for (i, fut) in futures_it.enumerate() {
        let semaphore = max_concurrent.clone();
        let span = info_span!("spawn_limited_task", task_id = next_task_id());
        js.spawn_on(
            async move {
                debug!(idx = i, "acquire semaphore");
                let _permit = semaphore.acquire().await?;
                debug!(idx = i, "acquired semaphore. Running task");
                let res = fut.await.map_err(ParutilsError::Task)?;
                Ok((i, res))
            }
            .instrument(span),
            &handle,
        );
    }

    let mut results: Vec<Option<T>> = Vec::with_capacity(js.len());
    (0..js.len()).for_each(|_| results.push(None));
    while let Some(result) = js.join_next().await {
        let (i, res) = result??;
        debug_assert!(results[i].is_none());
        results[i] = Some(res);
    }
    debug_assert!(js.is_empty());
    debug_assert!(results.iter().all(|r| r.is_some()));

    Ok(results.into_iter().map(|r| r.unwrap()).collect())
}

/// Like tokio_run_max_concurrency_fold_result_with_semaphore but callers can pass in the number
/// of concurrent tasks they wish to allow and the semaphore is created inside this function scope
pub async fn run_limited_fold_result<Fut, T, E>(
    futures_it: impl Iterator<Item = Fut>,
    max_concurrent: usize,
) -> Result<Vec<T>, ParutilsError<E>>
where
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    run_limited_fold_result_with_semaphore(futures_it, semaphore).await
}

#[cfg(test)]
mod parallel_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_simple_parallel() {
        let data: Vec<String> = (0..400).map(|i| format!("Number = {}", &i)).collect();

        let data_ref: Vec<String> = data.iter().enumerate().map(|(i, s)| format!("{}{}{}", &s, ":", &i)).collect();

        let r = run_limited_fold_result(
            data.into_iter()
                .enumerate()
                .map(|(i, s)| async move { Result::<_, ()>::Ok(format!("{}{}{}", &s, ":", &i)) }),
            4,
        )
        .await
        .unwrap();

        assert_eq!(data_ref.len(), r.len());
        for i in 0..data_ref.len() {
            assert_eq!(data_ref[i], r[i]);
        }
    }
}
