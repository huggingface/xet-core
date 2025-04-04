/// A small marker struct so you can write `release_fixed(1234)`.
/// In debug builds, we allow env override; in release, we ignore env.
pub enum GlobalConfigMode<T> {
    ReleaseFixed(T),
    EnvConfigurable(T),
}

#[allow(dead_code)]
pub fn release_fixed<T>(t: T) -> GlobalConfigMode<T> {
    GlobalConfigMode::ReleaseFixed(t)
}

// Make env_configurable the default
impl<T> From<T> for GlobalConfigMode<T> {
    fn from(value: T) -> Self {
        GlobalConfigMode::EnvConfigurable(value)
    }
}

// Reexport this so that dependencies don't have weird other dependencies
pub use lazy_static::lazy_static;

#[macro_export]
macro_rules! configurable_constants {
    ($(
        $(#[$meta:meta])*
        ref $name:ident : $type:ty = $value:expr;
    )+) => {
        $(
            #[allow(unused_imports)]
            use utils::constant_declarations::*;
            lazy_static! {
                $(#[$meta])*
                pub static ref $name: $type = {
                    let v : GlobalConfigMode<$type> = ($value).into();
                    let try_load_from_env = |v_| {
                        std::env::var(concat!("HF_XET_",stringify!($name)))
                            .ok()
                            .and_then(|s| s.parse::<$type>().ok())
                            .unwrap_or(v_)
                    };

                    match (v, cfg!(debug_assertions)) {
                        (GlobalConfigMode::ReleaseFixed(v), false) => v,
                        (GlobalConfigMode::ReleaseFixed(v), true) => try_load_from_env(v),
                        (GlobalConfigMode::EnvConfigurable(v), _) => try_load_from_env(v),
                    }
                };
            }
        )+
    };
}

pub use ctor as ctor_reexport;

#[cfg(not(doctest))]
/// A macro for **tests** that sets `HF_XET_<GLOBAL_NAME>` to `$value` **before**
/// the global is initialized, and then checks that the global actually picks up
/// that value. If the global was already accessed (thus initialized), or if it
/// doesn't match after being set, this macro panics.
///
/// Typically you would document *the macro itself* here, rather than placing
/// doc comments above each call to `test_set_global!`, because it doesn't
/// define a new item.
///
/// # Example
/// ```rust
/// use utils::{configurable_constants, test_set_global};
/// configurable_constants! {
///    /// Target chunk size
///    ref CHUNK_TARGET_SIZE: u64 = 1024;
///
///    /// Max Chunk size, only adjustable in testing mode.
///    ref MAX_CHUNK_SIZE: u64 = release_fixed(4096);
/// }
///
/// test_set_global!(CHUNK_TARGET_SIZE, 2048);
/// assert_eq!(*CHUNK_TARGET_SIZE, 2048);
/// ```
#[macro_export]
macro_rules! test_set_globals {
    ($(
        $var_name:ident = $val:expr;
    )+) => {
        use utils::constant_declarations::ctor_reexport as ctor;

        #[ctor::ctor]
        fn set_globals_on_load() {
            $(
                let val = $val;

                // Construct the environment variable name, e.g. "HF_XET_MAX_NUM_CHUNKS"
                let env_name = concat!("HF_XET_", stringify!($var_name));
                // Convert the $val to a string and set it
                std::env::set_var(env_name, val.to_string());

                // Force lazy_static to be read now:
                let actual_value = *$var_name;

                if actual_value != val {
                    panic!(
                        "test_set_global! failed: wanted {} to be {:?}, but got {:?}",
                        stringify!($var_name),
                        val,
                        actual_value
                    );
                }
                eprintln!("> Set {} to {:?}",
                        stringify!($var_name),
                        val);
            )+
        }
    }
}
