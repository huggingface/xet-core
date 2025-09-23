use std::future::Future;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{AcquireError, Semaphore};
use tokio::task::{JoinError, JoinSet};

#[derive(Debug, Error)]
pub enum ParutilsError<E: Send + Sync + 'static> {
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Acquire(#[from] AcquireError),
    #[error(transparent)]
    Task(E),
    #[error("Infallible, this should not be possible: {0}")]
    Infallible(String),
}

/// Runs all the futures in the provided iterator with a maximum concurrency limit.
///
/// This function ensures that a permit is acquired from the provided semaphore before any work
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
/// use xet_runtime::utils::run_constrained_with_semaphore;
///
/// #[tokio::main]
/// async fn main() {
///     let semaphore = Arc::new(Semaphore::new(2)); // Limit concurrency to 2 tasks.
///     let futures = (1..=3).map(|n| async move { Ok::<_, ()>(n) });
///
///     let results = run_constrained_with_semaphore(futures.into_iter(), semaphore).await;
///     assert_eq!(results.unwrap(), vec![1, 2, 3]);
/// }
/// ```
pub async fn run_constrained_with_semaphore<Fut, T, E>(
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
        js.spawn_on(
            async move {
                let _permit = semaphore.acquire().await?;
                let res = fut.await.map_err(ParutilsError::Task)?;
                Ok((i, res))
            },
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

    // Convert from Vec<Option<T>> to Option<Vec<T>>
    // Should be impossible to get back a None, that would indicate the js.join_next() should have
    // more tasks
    let Some(result) = results.into_iter().collect() else {
        return Err(ParutilsError::Infallible("A task was unaccounted for when collecting result".to_string()));
    };
    Ok(result)
}

/// Like tokio_run_max_concurrency_fold_result_with_semaphore but callers can pass in the number
/// of concurrent tasks they wish to allow and the semaphore is created inside this function scope
pub async fn run_constrained<Fut, T, E>(
    futures_it: impl Iterator<Item = Fut>,
    max_concurrent: usize,
) -> Result<Vec<T>, ParutilsError<E>>
where
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    run_constrained_with_semaphore(futures_it, semaphore).await
}

#[cfg(test)]
mod parallel_tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_simple_parallel() {
        let data: Vec<String> = (0..400).map(|i| format!("Number = {}", &i)).collect();

        let data_ref: Vec<String> = data.iter().enumerate().map(|(i, s)| format!("{}{}{}", &s, ":", &i)).collect();

        let r = run_constrained(
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_parallel_with_sleeps() {
        let data: Vec<String> = (0..400).map(|i| format!("Number = {}", &i)).collect();

        let data_ref: Vec<String> = data.iter().enumerate().map(|(i, s)| format!("{}{}{}", &s, ":", &i)).collect();

        let r = run_constrained(
            data.into_iter().enumerate().map(|(i, s)| async move {
                tokio::time::sleep(std::time::Duration::from_millis(401 - i as u64)).await;
                Result::<_, ()>::Ok(format!("{}{}{}", &s, ":", &i))
            }),
            100,
        )
        .await
        .unwrap();

        assert_eq!(data_ref.len(), r.len());
        for i in 0..data_ref.len() {
            assert_eq!(data_ref[i], r[i]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_max_concurrent_constraint() {
        const NUM_TASKS: u64 = 100;
        const TASK_DURATION_BASE_MS: u64 = 100;
        const MAX_CONCURRENT: usize = 5;

        // Counters to track concurrent task execution
        let current_running = Arc::new(AtomicU32::new(0));
        let max_concurrent_observed = Arc::new(AtomicU32::new(0));

        let futures = (0..NUM_TASKS).map(|i| {
            let current_running = current_running.clone();
            let max_concurrent_observed = max_concurrent_observed.clone();

            async move {
                // Increment running counter
                let running = current_running.fetch_add(1, Ordering::SeqCst) + 1;

                // Update max observed if necessary
                max_concurrent_observed.fetch_max(running, Ordering::SeqCst);

                // Simulate work
                tokio::time::sleep(std::time::Duration::from_millis(TASK_DURATION_BASE_MS - i)).await;

                // Decrement running counter
                current_running.fetch_sub(1, Ordering::SeqCst);

                Result::<_, ()>::Ok(i)
            }
        });

        let results = run_constrained(futures, MAX_CONCURRENT).await.unwrap();

        // Verify all tasks completed successfully
        assert_eq!(results.len(), NUM_TASKS as usize);
        for i in 0..NUM_TASKS {
            assert_eq!(results[i as usize], i);
        }

        // Verify that we never exceeded the concurrency limit
        let max_observed = max_concurrent_observed.load(Ordering::SeqCst);
        assert!(
            max_observed <= MAX_CONCURRENT as u32,
            "Max concurrent tasks observed: {}, but limit was: {}",
            max_observed,
            MAX_CONCURRENT
        );

        assert_eq!(
            max_observed, MAX_CONCURRENT as u32,
            "Expected to see exactly {} concurrent tasks, but saw {}",
            MAX_CONCURRENT, max_observed
        );

        // Ensure no tasks are still running
        assert_eq!(current_running.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_returns_error() {
        let futures = (0..10).map(|i| async move {
            if i == 5 {
                Result::<_, i32>::Err(5)
            } else {
                Result::<_, i32>::Ok(i)
            }
        });

        let result = run_constrained(futures, 2).await;
        assert!(matches!(result, Err(ParutilsError::Task(5))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_returns_join_error_on_panic() {
        let futures = (0..10).map(|i| async move { if i == 5 { panic!("5") } else { Result::<_, i32>::Ok(i) } });

        let result = run_constrained(futures, 2).await;
        if let Err(ParutilsError::Join(e)) = result {
            assert!(e.is_panic());
        } else {
            assert!(false, "Expected to panic, but got {:?}", result);
        }
    }
}
