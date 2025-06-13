use std::future::Future;
use std::mem::replace;
use std::ops::Deref;

use thiserror::Error;
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::task::{JoinError, JoinHandle};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RwTaskLockError {
    #[error(transparent)]
    JoinError(#[from] JoinError),

    #[error("BUG: repeated retrieval attempts after error.")]
    CalledAfterError,
}

enum RwTaskLockState<T, E> {
    Pending(JoinHandle<Result<T, E>>),
    Ready(T),
    Error,
}

/// Custom read guard: keeps the RwLockReadGuard alive, exposes &T.
pub struct RwTaskLockReadGuard<'a, T, E> {
    guard: RwLockReadGuard<'a, RwTaskLockState<T, E>>,
}

impl<T, E> Deref for RwTaskLockReadGuard<'_, T, E> {
    type Target = T;
    fn deref(&self) -> &T {
        match &*self.guard {
            RwTaskLockState::Ready(ref val) => val,
            _ => unreachable!("Read guard is only constructed for Ready state"),
        }
    }
}

/// A one-time async-initialized, lockable value that yields a read guard after initialization.
///
/// `RwTaskLock<T, E>` allows you to wrap a future or ready value so the computation
/// (e.g., background task, async function) is performed at most once, even if
/// multiple callers invoke `.read()` concurrently. After the computation,
/// the result is cached. If successful, all future `.read()` calls yield a
/// read guard on the stored value. If an error occurs, all subsequent calls
/// return the error (error value must be `Clone`).
///
/// # Example
/// ```
/// use tokio::time;
/// use utils::{RwTaskLock, RwTaskLockError};
/// #[tokio::main]
/// async fn main() -> Result<(), RwTaskLockError> {
///     let lock = RwTaskLock::from_task(async {
///         time::sleep(std::time::Duration::from_millis(50)).await;
///         Ok::<_, RwTaskLockError>(vec![1, 2, 3])
///     });
///     let guard = lock.read().await?;
///     assert_eq!(&*guard, &[1, 2, 3]);
///     Ok(())
/// }
/// ```
pub struct RwTaskLock<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static + From<RwTaskLockError>,
{
    state: RwLock<RwTaskLockState<T, E>>,
}

impl<T, E> RwTaskLock<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static + From<RwTaskLockError>,
{
    /// From a ready value.
    pub fn from_value(val: T) -> Self {
        Self {
            state: RwLock::new(RwTaskLockState::Ready(val)),
        }
    }

    /// From a future yielding Result<T, E>.
    pub fn from_task<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let task = tokio::spawn(fut);

        Self {
            state: RwLock::new(RwTaskLockState::Pending(task)),
        }
    }

    /// Awaitable read: yields a custom read guard or error.
    pub async fn read(&self) -> Result<RwTaskLockReadGuard<'_, T, E>, E> {
        // Fast path
        {
            let state = self.state.read().await;
            match &*state {
                RwTaskLockState::Ready(_) => {
                    return Ok(RwTaskLockReadGuard { guard: state });
                },
                RwTaskLockState::Error => return Err(E::from(RwTaskLockError::CalledAfterError)),
                RwTaskLockState::Pending(_) => {},
            }
        }
        // Acquire write lock to initialize if necessary
        let mut state = self.state.write().await;

        match replace(&mut *state, RwTaskLockState::Error) {
            RwTaskLockState::Ready(v) => {
                *state = RwTaskLockState::Ready(v);
            },
            RwTaskLockState::Error => {
                return Err(E::from(RwTaskLockError::CalledAfterError));
            },
            RwTaskLockState::Pending(jh) => {
                match jh.await.map_err(RwTaskLockError::JoinError)? {
                    Ok(v) => {
                        *state = RwTaskLockState::Ready(v);
                    },
                    Err(e) => {
                        *state = RwTaskLockState::Error;
                        return Err(e);
                    },
                };
            },
        };

        Ok(RwTaskLockReadGuard {
            guard: state.downgrade(),
        })
    }
    /// Update the current value by applying an async function to it, storing the result as the new value.
    ///
    /// - If the current value is in the `Ready` state, the function is immediately scheduled as a background task with
    ///   the current value, and the state becomes `Pending` until completion.
    /// - If the value is in the `Pending` state, this chains the update: when the background task completes, the
    ///   updater will be called on the resulting value.
    /// - If the value is in the `Error` state, returns an error and does nothing.
    ///
    /// Returns `Ok(())` if the update is scheduled. Errors if the value is already in an error state.
    ///
    /// # Example: Chaining updates
    /// ```
    /// use tokio::time;
    /// use utils::{RwTaskLock, RwTaskLockError};
    /// #[tokio::main]
    /// async fn main() -> Result<(), RwTaskLockError> {
    ///     let lock = RwTaskLock::from_value(10);
    ///     lock.update(|v| async move { Ok::<_, RwTaskLockError>(v * 2) }).await?;
    ///     assert_eq!(*lock.read().await?, 20);
    ///
    ///     lock.update(|v| async move { Ok::<_, RwTaskLockError>(v + 5) }).await?;
    ///     assert_eq!(*lock.read().await?, 25);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Example: Chained with pending state
    /// ```
    /// use std::sync::Arc;
    ///
    /// use tokio::time;
    /// use utils::{RwTaskLock, RwTaskLockError};
    /// #[tokio::main]
    /// async fn main() -> Result<(), RwTaskLockError> {
    ///     let lock = Arc::new(RwTaskLock::from_task(async {
    ///         time::sleep(std::time::Duration::from_millis(10)).await;
    ///         Ok::<_, RwTaskLockError>(10)
    ///     }));
    ///     let lock2 = lock.clone();
    ///
    ///     // Chain update while value is still pending
    ///     lock2.update(|v| async move { Ok::<_, RwTaskLockError>(v + 10) }).await?;
    ///     assert_eq!(*lock.read().await?, 20);
    ///     Ok(())
    /// }
    /// ```
    pub async fn update<Fut, Updater>(&self, updater: Updater) -> Result<(), RwTaskLockError>
    where
        Updater: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        use RwTaskLockState::*;

        let mut state_lg = self.state.write().await;

        let state = replace(&mut *state_lg, RwTaskLockState::Error);

        match state {
            Pending(jh) => {
                // Chain the old pending future, then the updater.
                let new_task = tokio::spawn(async move {
                    let current = jh.await.map_err(RwTaskLockError::JoinError)??;
                    updater(current).await
                });
                *state_lg = Pending(new_task);
                Ok(())
            },
            Ready(v) => {
                // Start new computation from current value.
                *state_lg = Pending(tokio::spawn(updater(v)));
                Ok(())
            },
            Error => {
                // Can't update if in error.
                *state_lg = Error;
                Err(RwTaskLockError::CalledAfterError)
            },
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_from_value() {
        let lock: RwTaskLock<_, RwTaskLockError> = RwTaskLock::from_value(7);
        let guard = lock.read().await.unwrap();
        assert_eq!(*guard, 7);
        let guard2 = lock.read().await.unwrap();
        assert_eq!(*guard2, 7);
    }

    #[tokio::test]
    async fn test_from_future_success() {
        let lock = RwTaskLock::from_task(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok::<_, RwTaskLockError>(999)
        });
        let guard = lock.read().await.unwrap();
        assert_eq!(*guard, 999);
        let guard2 = lock.read().await.unwrap();
        assert_eq!(*guard2, 999);
    }

    #[tokio::test]
    async fn test_from_future_error() {
        let lock = RwTaskLock::<u8, RwTaskLockError>::from_task(async { Err(RwTaskLockError::CalledAfterError) });
        let result = lock.read().await;
        assert!(matches!(result, Err(RwTaskLockError::CalledAfterError)));
        let result2 = lock.read().await;
        assert!(matches!(result2, Err(RwTaskLockError::CalledAfterError)));
    }

    #[tokio::test]
    async fn test_concurrent_read() {
        use std::sync::Arc;
        let lock = Arc::new(RwTaskLock::from_task(async {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            Ok::<_, RwTaskLockError>("concurrent".to_string())
        }));
        let lock1 = lock.clone();
        let lock2 = lock.clone();
        let (a, b) = tokio::join!(lock1.read(), lock2.read());
        assert_eq!(*a.unwrap(), "concurrent");
        assert_eq!(*b.unwrap(), "concurrent");
    }

    #[tokio::test]
    async fn test_error_then_retrieval() {
        let lock = RwTaskLock::<u8, RwTaskLockError>::from_task(async { Err(RwTaskLockError::CalledAfterError) });
        let _ = lock.read().await;
        let result = lock.read().await;
        assert!(matches!(result, Err(RwTaskLockError::CalledAfterError)));
    }

    #[tokio::test]
    async fn test_update_from_ready() {
        let lock = RwTaskLock::from_value(100);
        lock.update(|v| async move { Ok::<_, RwTaskLockError>(v + 1) }).await.unwrap();
        let guard = lock.read().await.unwrap();
        assert_eq!(*guard, 101);
    }

    #[tokio::test]
    async fn test_update_chained_pending() {
        use std::sync::Arc;
        let lock = Arc::new(RwTaskLock::from_task(async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            Ok::<_, RwTaskLockError>(5)
        }));
        let lock2 = lock.clone();
        // Schedule update before initial value is ready
        lock2.update(|v| async move { Ok::<_, RwTaskLockError>(v * 3) }).await.unwrap();
        let guard = lock.read().await.unwrap();
        assert_eq!(*guard, 15);
    }

    #[tokio::test]
    async fn test_update_error_state() {
        let lock = RwTaskLock::<i32, RwTaskLockError>::from_task(async { Err(RwTaskLockError::CalledAfterError) });
        let _ = lock.read().await;
        let result = lock.update(|v| async move { Ok::<_, RwTaskLockError>(v + 1) }).await;
        assert!(matches!(result, Err(RwTaskLockError::CalledAfterError)));
    }

    #[tokio::test]
    async fn test_update_to_error() {
        let lock = RwTaskLock::from_value(123);
        // Updater produces an error
        lock.update(|_v| async move { Err(RwTaskLockError::CalledAfterError) })
            .await
            .unwrap();
        let result = lock.read().await;
        assert!(matches!(result, Err(RwTaskLockError::CalledAfterError)));
    }

    #[tokio::test]
    async fn test_multiple_updates() {
        let lock = RwTaskLock::from_value(1);
        lock.update(|v| async move { Ok::<_, RwTaskLockError>(v + 10) }).await.unwrap();
        lock.update(|v| async move { Ok::<_, RwTaskLockError>(v * 2) }).await.unwrap();
        let guard = lock.read().await.unwrap();
        assert_eq!(*guard, 22);
    }
}
