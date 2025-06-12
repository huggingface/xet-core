use std::future::Future;
use std::pin::Pin;

use futures::future;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::task::{JoinError, JoinHandle};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OnceTaskError {
    #[error(transparent)]
    JoinError(#[from] JoinError),

    #[error("BUG: repeated retrieval attempts after error.")]
    CalledAfterError,
}

enum OnceTaskState<T, E> {
    Pending(Pin<Box<dyn Future<Output = Result<T, E>> + Send>>),
    Ready(T),
    Error,
}

/// An asynchronously computed value that is evaluated at most once.
///
/// `TaskValue<T, E>` wraps a future or a ready value so that the computation
/// (e.g., background task, async function) is performed at most once, even if
/// multiple callers invoke `.get()` concurrently. After the computation,
/// the result is cached. If successful, all future `.get()` calls return a clone
/// of the stored value. If an error occurs, all subsequent calls return an error,
/// and no further attempts are made.
///
/// # Example
/// ```rust
/// use utils::{OnceTask, OnceTaskError};
///
/// #[tokio::main]
/// async fn main() -> Result<(), OnceTaskError> {
///     let cell = OnceTask::from_future(async { Ok::<_, OnceTaskError>(42) });
///     let v = cell.get().await?;
///     assert_eq!(v, 42);
///     // You can call get() again and get the same value
///     let v2 = cell.get().await?;
///     assert_eq!(v2, 42);
///     Ok(())
/// }
/// ```
pub struct OnceTask<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static + From<OnceTaskError>,
{
    state: RwLock<OnceTaskState<T, E>>,
}

impl<T, E> OnceTask<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static + From<OnceTaskError>,
{
    /// From a ready value.
    pub fn from_value(val: T) -> Self {
        Self {
            state: RwLock::new(OnceTaskState::Ready(val)),
        }
    }

    /// From a future yielding Result<T, E>.
    pub fn from_future<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        Self {
            state: RwLock::new(OnceTaskState::Pending(Box::pin(fut))),
        }
    }

    /// From a join handle, mapping join error to E.
    pub fn from_task(task: JoinHandle<Result<T, E>>) -> Self
    where
        E: From<JoinError>,
    {
        Self::from_future(async move { Ok(task.await??) })
    }

    /// Awaitable get: clones T, errors are moved/ref'ed as appropriate.
    pub async fn get(&self) -> Result<T, E> {
        // Try fast path
        {
            let state = self.state.read().await;
            match &*state {
                OnceTaskState::Ready(val) => return Ok(val.clone()),
                OnceTaskState::Error => return Err(E::from(OnceTaskError::CalledAfterError)),
                OnceTaskState::Pending(_) => {},
            }
        }

        // Initialize via write lock
        let mut state = self.state.write().await;
        match &mut *state {
            OnceTaskState::Ready(val) => Ok(val.clone()), // Now ready
            OnceTaskState::Error => Err(E::from(OnceTaskError::CalledAfterError)),
            OnceTaskState::Pending(fut) => {
                // Move out the future, replace with a pending future to maintain variant
                let fut = std::mem::replace(fut, Box::pin(future::pending()));
                match fut.await {
                    Ok(val) => {
                        *state = OnceTaskState::Ready(val.clone());
                        Ok(val)
                    },
                    Err(e) => {
                        *state = OnceTaskState::Error;
                        Err(e)
                    },
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn test_from_value() {
        let v: OnceTask<_, OnceTaskError> = OnceTask::from_value(7);
        assert_eq!(v.get().await.unwrap(), 7);
        assert_eq!(v.get().await.unwrap(), 7); // Repeatable
    }

    #[tokio::test]
    async fn test_from_future_success() {
        let cell = OnceTask::from_future(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok::<_, OnceTaskError>(999)
        });
        assert_eq!(cell.get().await.unwrap(), 999);
        assert_eq!(cell.get().await.unwrap(), 999);
    }

    #[tokio::test]
    async fn test_from_future_error() {
        let cell = OnceTask::<u8, OnceTaskError>::from_future(async { Err(OnceTaskError::CalledAfterError) });
        assert!(matches!(cell.get().await, Err(OnceTaskError::CalledAfterError)));
        assert!(matches!(cell.get().await, Err(OnceTaskError::CalledAfterError)));
    }

    #[tokio::test]
    async fn test_from_task_success() {
        let handle = task::spawn(async { Ok::<_, OnceTaskError>(88) });
        let tv = OnceTask::from_task(handle);
        assert_eq!(tv.get().await.unwrap(), 88);
        assert_eq!(tv.get().await.unwrap(), 88);
    }

    #[tokio::test]
    async fn test_from_task_join_error() {
        // Spawn a task that panics
        let handle = task::spawn(async {
            panic!("boom");
        });
        let tv = OnceTask::<u8, OnceTaskError>::from_task(handle);
        let res = tv.get().await;
        // Should be JoinError
        assert!(matches!(res, Err(OnceTaskError::JoinError(_))));
        // Second call: error should be CalledAfterError
        let res2 = tv.get().await;
        assert!(matches!(res2, Err(OnceTaskError::CalledAfterError)));
    }

    #[tokio::test]
    async fn test_concurrent_get() {
        let tv = std::sync::Arc::new(OnceTask::from_future(async {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            Ok::<_, OnceTaskError>("concurrent".to_string())
        }));
        let tv1 = tv.clone();
        let tv2 = tv.clone();
        let (a, b) = tokio::join!(tv1.get(), tv2.get());
        assert_eq!(a.unwrap(), "concurrent");
        assert_eq!(b.unwrap(), "concurrent");
    }

    #[tokio::test]
    async fn test_error_then_retrieval() {
        let tv = OnceTask::<u8, OnceTaskError>::from_future(async { Err(OnceTaskError::CalledAfterError) });
        let _ = tv.get().await;
        // After error, all further gets return CalledAfterError
        assert!(matches!(tv.get().await, Err(OnceTaskError::CalledAfterError)));
    }
}
