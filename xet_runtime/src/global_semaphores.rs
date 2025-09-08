use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

/// Identifies a process-wide semaphore and its initial size.
///
/// Create one using global_semaphore_handle!() and pass it to
/// `ThreadPool::current().global_semaphore(handle)` to obtain a global semaphore
/// `Arc<Semaphore>`
///
/// The `initial_value` is applied only when the semaphore is first created for
/// this handle; later lookups return the existing semaphore.
///
/// # Typical usage
///
/// ```ignore
/// use lazy_static::lazy_static;
///
/// lazy_static! {
///     static ref UPLOAD_LIMITER: GlobalSemaphoreHandle = global_semaphore_handle!(32);
/// }
///
/// // Acquire a permit
/// let permit = ThreadPool::current()
///     .global_semaphore(*UPLOAD_LIMITER)
///     .acquire_owned()
///     .await?;
/// ```
#[derive(Copy, Clone)]
pub struct GlobalSemaphoreHandle {
    pub handle: &'static str,
    pub initial_value: usize,
}

impl AsRef<GlobalSemaphoreHandle> for GlobalSemaphoreHandle {
    fn as_ref(&self) -> &GlobalSemaphoreHandle {
        self
    }
}

/// Creates a GlobalSemaphoreHandle instance with a compile-time unique handle and
/// initial value.   
///
/// # Example usage
///
/// ```ignore
/// use lazy_static::lazy_static;
///
/// lazy_static! {
///     static ref UPLOAD_LIMITER: GlobalSemaphoreHandle = global_semaphore_handle!(32);
/// }
///
/// // Acquire a permit
/// let permit = ThreadPool::current()
///     .global_semaphore(*UPLOAD_LIMITER)
///     .acquire_owned()
///     .await?;
/// ```
#[macro_export]
macro_rules! global_semaphore_handle {
    // Expression form: returns a GlobalSemaphoreHandle
    ($perm:expr) => {{
        // A compile-time unique &'static str using module, file, line, and column.
        const __HANDLE: &str = concat!(module_path!(), "::", file!(), ":", line!(), ":", column!());

        $crate::GlobalSemaphoreHandle {
            handle: __HANDLE,
            initial_value: ($perm).into(),
        }
    }};
}

#[derive(Debug, Default)]
pub(crate) struct GlobalSemaphoreLookup {
    lookup: Mutex<HashMap<&'static str, Arc<Semaphore>>>,
}

impl GlobalSemaphoreLookup {
    pub(crate) fn get(&self, handle: impl Into<GlobalSemaphoreHandle>) -> Arc<Semaphore> {
        let handle = handle.into();

        let mut sl = self.lookup.lock().expect("Recursive lock; bug");

        sl.entry(handle.handle)
            .or_insert_with(|| Arc::new(Semaphore::new(handle.initial_value)))
            .clone()
    }
}
