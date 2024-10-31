use tokio;


pub struct ThreadPool {
    inner: tokio::runtime::Runtime,
}

impl ThreadPool {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            inner: new_threadpool()?,
        })
    }

    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.inner.block_on(f)
    }

    pub fn spawn<F>(&self, f: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.inner.spawn(f);
    }

    pub fn get_handle(&self) -> tokio::runtime::Handle {
        self.inner.handle().clone()
    }
}

fn new_threadpool() -> std::io::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)            // 4 active threads
        .thread_name("hf_xet-")       // thread names will be hf_xet-1, hf_xet-2, etc.
        .thread_stack_size(8_000_000) // 8MB stack size, default is 2MB
        .max_blocking_threads(100)    // max 100 threads can block IO
        .enable_all()                 // enable all features, including IO/Timer/Signal/Reactor
        .build()?)
}