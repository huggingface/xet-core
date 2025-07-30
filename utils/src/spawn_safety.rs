use std::sync::Arc;

use tokio::sync::{RwLock, Semaphore};

/// Provides a class that resets a value from an initializer function when a spawn is detected.  
/// Intended for use with static semaphores.
pub struct ResetOnSpawn<V>
where
    V: Send,
{
    value: RwLock<(u32, Arc<V>)>,
    init_function: Box<dyn Fn() -> V + Send + Sync + 'static>,
}

impl<V> ResetOnSpawn<V>
where
    V: Send,
{
    pub fn new(init_function: impl Fn() -> V + Send + Sync + 'static) -> Self {
        Self {
            value: RwLock::new((std::process::id(), Arc::new(init_function()))),
            init_function: Box::new(init_function),
        }
    }

    pub async fn get(&self) -> Arc<V> {
        let pid = std::process::id();

        {
            let (saved_pid, ref v) = *self.value.read().await;

            if saved_pid == pid {
                return v.clone();
            }
        }

        // Have to reset it, but just forget the value in there;
        let mut write_guard = self.value.write().await;

        let mut v = Arc::new((self.init_function)());
        let ret_v = v.clone();

        // Set this to the new value, pulling out the old one.
        write_guard.0 = pid;
        std::mem::swap(&mut write_guard.1, &mut v);

        // Forget the old value to keep destructors from running on the poisoned value.
        std::mem::forget(v);

        ret_v
    }
}

pub struct SpawnSafeStaticSemaphore {
    inner: ResetOnSpawn<Semaphore>,
}

impl SpawnSafeStaticSemaphore {
    pub fn new(permits: usize) -> Self {
        Self {
            inner: ResetOnSpawn::new(move || Semaphore::new(permits)),
        }
    }

    // Checks and returns the inner semaphore.
    pub async fn get(&self) -> Arc<Semaphore> {
        self.inner.get().await
    }
}
