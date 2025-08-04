use error_printer::ErrorPrinter;

use crate::errors::{MultithreadedRuntimeError, Result};

/// Join handle for a task on the compute runtime.  
pub struct SyncJoinHandle<T: Send + Sync + 'static> {
    task_result: oneshot::Receiver<Result<T>>, /* Use the other join handle to figure out when the previous job is
                                                * done. */
}

pub fn spawn_os_thread<T: Send + Sync + 'static>(task: impl FnOnce() -> T + Send + 'static) -> SyncJoinHandle<T> {
    let (jh, tx) = SyncJoinHandle::create();

    std::thread::spawn(move || {
        // Catch panics and convert to an error we can send over the channel.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)).map_err(|payload| {
            // Try to extract a useful panic message.
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "panic with non-string payload".to_string()
            };
            MultithreadedRuntimeError::TaskPanic(msg)
        });

        // Possibly happens during runtime shutdown, so only do this at the info level.
        let _ = tx
            .send(outcome)
            .info_error("Return result on join handle encountered error; possible out-of-order shutdown.");
    });

    jh
}

impl<T: Send + Sync + 'static> SyncJoinHandle<T> {
    fn create() -> (Self, oneshot::Sender<Result<T>>) {
        let (sender, task_result) = oneshot::channel::<Result<T>>();
        (Self { task_result }, sender)
    }

    /// Blocks the current thread until the other os thread has finished.
    /// Use this only in synchronous code.  In async code, use tokio's spawn_blocking.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying task panicked.  
    ///
    /// # Examples
    ///
    /// ```
    /// use xet_threadpool::spawn_os_thread;
    /// let handle = spawn_os_thread(|| 42);
    /// let result = handle.join().unwrap();
    /// assert_eq!(result, 42);
    /// ```
    pub fn join(self) -> Result<T> {
        self.task_result
            .recv()
            .map_err(|e| MultithreadedRuntimeError::Other(format!("SyncJoinHandle: {e:?}")))?
    }

    /// Attempts to retrieve the result without blocking.  
    ///
    /// - Returns `Ok(Some(value))` if the task is complete.
    /// - Returns `Ok(None)` if the task is still running.
    /// - Returns an `Err(...)` variant if
    ///
    /// # Examples
    ///
    /// ```
    /// use xet_threadpool::{spawn_os_thread, SyncJoinHandle};
    /// let handle: SyncJoinHandle<_> = spawn_os_thread(|| 42);
    ///
    /// // Possibly do some work here...
    /// match handle.try_join() {
    ///     Ok(Some(value)) => println!("Value is ready: {}", value),
    ///     Ok(None) => println!("Still running"),
    ///     Err(e) => eprintln!("Error: {:?}", e),
    /// }
    /// ```    
    pub fn try_join(&self) -> Result<Option<T>> {
        match self.task_result.try_recv() {
            Err(oneshot::TryRecvError::Empty) => Ok(None),
            Err(e) => Err(MultithreadedRuntimeError::Other(format!("SyncJoinHandle: {e:?}"))),
            Ok(r) => Ok(Some(r?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    /// Helper: poll `try_join()` until it returns `Some` or we time out.
    fn wait_for_value<T: Send + Sync + 'static>(h: &SyncJoinHandle<T>, timeout: Duration) -> Result<T> {
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(MultithreadedRuntimeError::Other(
                    "timed out waiting for try_join() to become ready".into(),
                ));
            }
            match h.try_join()? {
                Some(v) => return Ok(v),
                None => thread::sleep(Duration::from_millis(10)),
            }
        }
    }

    #[test]
    fn join_returns_value() {
        let handle = spawn_os_thread(|| 40 + 2);
        let v = handle.join().expect("join should succeed");
        assert_eq!(v, 42);
    }

    #[test]
    fn try_join_is_non_blocking_then_ready() {
        let handle = spawn_os_thread(|| {
            // Simulate work
            thread::sleep(Duration::from_millis(100));
            1234
        });

        // Immediately after spawn, it shouldn't be ready.
        let early = handle.try_join().expect("try_join should not error");
        assert!(early.is_none(), "try_join should be non-blocking and return None while running");

        // Wait until value becomes available via try_join.
        let v = wait_for_value(&handle, Duration::from_secs(5)).expect("value should arrive");
        assert_eq!(v, 1234);

        // Note: After taking the value via try_join(), calling `join()` would
        // understandably error, since the single-shot value was already received.
    }

    #[test]
    fn join_propagates_panic_as_error() {
        let handle = spawn_os_thread(|| -> usize {
            // Panic before sending a result; receiver's `recv()` should error.
            panic!("intentional panic in worker")
        });

        let err = handle.join().expect_err("join should report an error on panic");
        // Minimal assertion: we got our domain error variant.
        match err {
            MultithreadedRuntimeError::TaskPanic(msg) => {
                // Keep it loose; don't depend on exact wording.
                assert!(msg.contains("panic"))
            },
            _ => panic!("unexpected error variant: {err:?}"),
        }
    }

    #[test]
    fn dropping_handle_before_completion_is_harmless() {
        // This covers the sender `.send(...)` failure path: if the receiver is dropped
        // before the worker completes, `.send()` will fail; the code logs at info level
        // and ignores the error.
        //
        // We can't observe the log here; this test ensures the process doesn't panic/crash.
        let handle = spawn_os_thread(|| {
            thread::sleep(Duration::from_millis(200));
            7usize
        });

        // Drop the receiver without joining.
        drop(handle);

        // Give the worker time to attempt send and exit.
        thread::sleep(Duration::from_millis(300));

        // If we reached here without panic, behavior is as intended.
        assert!(true);
    }

    #[test]
    fn try_join_then_join_errors_after_value_taken() {
        // Validate that once the oneshot value is taken via try_join(),
        // a subsequent blocking join (which consumes the handle) errors cleanly.
        let handle = spawn_os_thread(|| {
            thread::sleep(Duration::from_millis(50));
            555u32
        });

        let v = wait_for_value(&handle, Duration::from_secs(5)).expect("should get value");
        assert_eq!(v, 555);

        // Now that the value is already received, consuming `join()` should error.
        let err = handle.join().expect_err("join after value taken should error");
        matches!(err, MultithreadedRuntimeError::Other(_));
    }

    #[test]
    fn try_join_immediate_none_for_long_task() {
        let handle = spawn_os_thread(|| {
            thread::sleep(Duration::from_secs(1));
            1usize
        });

        // Quick check: `try_join` should not block and should report None right away.
        let t0 = Instant::now();
        let r = handle.try_join().expect("try_join should not error");
        let elapsed = t0.elapsed();
        assert!(elapsed < Duration::from_millis(20), "try_join should be quick");
        assert!(r.is_none(), "value should not be ready yet");
    }
}
