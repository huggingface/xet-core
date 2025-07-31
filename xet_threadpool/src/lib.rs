pub mod errors;
pub mod exports;

pub mod threadpool;

pub use threadpool::ThreadPool;

#[macro_use]
mod global_semaphores;

pub use global_semaphores::GlobalSemaphoreHandle;

/// Log name=value pairs to stderr, prefixed by file:line.
/// Each argument is evaluated once; names come from `stringify!(arg)`.
/// Values are formatted with `Debug` (use `{:?}`).
#[macro_export]
macro_rules! kvlog {
    ($($val:expr),+ $(,)?) => {{
        // Build a single string to reduce interleaving on multi-threaded stderr.
        let mut __s = ::std::string::String::new();
        use ::std::fmt::Write as _;

        let _ = write!(&mut __s, "KVLOG: PID={}:{}:{}:", std::process::id(), file!(), line!());
        $(
            let _ = write!(&mut __s, " {}={:?}", stringify!($val), &$val);
        )+
        eprintln!("{}", __s);
    }};
    () => {
        eprintln!(concat!("KVLOG: PID={}", file!(), ":", line!(), ":"), std::process::id());
    };
}
