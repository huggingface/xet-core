use std::fmt::Debug;
use std::str::FromStr;
use std::time::Duration;

use tracing::{info, warn};

/// A trait to control how a value is parsed from an environment string or other config source
/// if it's present.
///
/// The main reason to do things like this is to
pub trait ParsableConfigValue: Debug + Sized {
    fn parse_user_value(value: &str) -> Option<Self>;

    /// Parse the value, returning the default if it can't be parsed or the string is empty.  
    /// Issue a warning if it can't be parsed.
    fn parse(variable_name: &str, value: Option<String>, default: Self) -> Self {
        match value {
            Some(v) => match Self::parse_user_value(&v) {
                Some(v) => {
                    info!("Config: {variable_name} = {v:?} (user set)");
                    v
                },
                None => {
                    warn!(
                        "Configuration value {v} for {variable_name} cannot be parsed into correct type; reverting to default."
                    );
                    info!("Config: {variable_name} = {default:?} (default due to parse error)");
                    default
                },
            },
            None => {
                info!("Config: {variable_name} = {default:?} (default)");
                default
            },
        }
    }
}

/// Most values work with the FromStr implementation, but we want to override the behavior for some types
/// (e.g. Option<T> and bool) to have custom parsing behavior.
pub trait FromStrParseable: FromStr + Debug {}

impl<T: FromStrParseable> ParsableConfigValue for T {
    fn parse_user_value(value: &str) -> Option<Self> {
        // Just wrap the base FromStr parser.
        value.parse::<T>().ok()
    }
}

// Implement FromStrParseable for all the base types where the FromStr parsing method just works.
impl FromStrParseable for usize {}
impl FromStrParseable for u8 {}
impl FromStrParseable for u16 {}
impl FromStrParseable for u32 {}
impl FromStrParseable for u64 {}
impl FromStrParseable for isize {}
impl FromStrParseable for i8 {}
impl FromStrParseable for i16 {}
impl FromStrParseable for i32 {}
impl FromStrParseable for i64 {}
impl FromStrParseable for f32 {}
impl FromStrParseable for f64 {}
impl FromStrParseable for String {}
impl FromStrParseable for ByteSize {}

/// Special handling for bool:
/// - true: "1","true","yes","y","on"  -> true
/// - false: "0","false","no","n","off","" -> false
fn parse_bool_value(value: &str) -> Option<bool> {
    let t = value.trim().to_ascii_lowercase();

    match t.as_str() {
        "0" | "false" | "no" | "n" | "off" => Some(false),
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        _ => None,
    }
}

impl ParsableConfigValue for bool {
    fn parse_user_value(value: &str) -> Option<Self> {
        parse_bool_value(value)
    }
}

/// Enable Option<T> to allow the default value to be None if nothing is set and appear as
/// Some(Value) if the user specifies the value.
impl<T: ParsableConfigValue> ParsableConfigValue for Option<T> {
    fn parse_user_value(value: &str) -> Option<Self> {
        T::parse_user_value(value).map(Some)
    }
}

/// Implement proper parsing for Duration types as well.
///
/// Now the following suffixes are supported [y, mon, d, h, m, s, ms];
/// see the duration_str crate for the full list.
impl ParsableConfigValue for Duration {
    fn parse_user_value(value: &str) -> Option<Self> {
        duration_str::parse(value).ok()
    }
}

/// A small marker struct so you can write `release_fixed(1234)`.
/// In debug builds, we allow env override; in release, we ignore env.
pub enum GlobalConfigMode<T> {
    ReleaseFixed(T),
    EnvConfigurable(T),
    HighPerformanceOption { standard: T, high_performance: T },
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
                        let maybe_env_value = std::env::var(concat!("HF_XET_",stringify!($name))).ok();
                        <$type>::parse(stringify!($name), maybe_env_value, v_)
                    };

                    match (v, cfg!(debug_assertions)) {
                        (GlobalConfigMode::ReleaseFixed(v), false) => v,
                        (GlobalConfigMode::ReleaseFixed(v), true) => try_load_from_env(v),
                        (GlobalConfigMode::EnvConfigurable(v), _) => try_load_from_env(v),
                        (GlobalConfigMode::HighPerformanceOption { standard, high_performance }, _) => try_load_from_env(if is_high_performance() { high_performance } else { standard }),
                    }
                };
            }
        )+
    };
}

pub use ctor as ctor_reexport;

use crate::ByteSize;

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
/// use utils::{configurable_constants, test_set_globals};
/// configurable_constants! {
///    /// Target chunk size
///    ref CHUNK_TARGET_SIZE: u64 = 1024;
///
///    /// Max Chunk size, only adjustable in testing mode.
///    ref MAX_CHUNK_SIZE: u64 = release_fixed(4096);
/// }
///
/// test_set_globals! {
///    CHUNK_TARGET_SIZE = 2048;
/// }
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
                let val_str = format!("{val:?}");

                // Construct the environment variable name, e.g. "HF_XET_MAX_NUM_CHUNKS"
                let env_name = concat!("HF_XET_", stringify!($var_name));

                // Set the environment
                unsafe {
                    std::env::set_var(env_name, &val_str);
                }

                // Force lazy_static to be read now:
                let actual_value = *$var_name;

                if format!("{actual_value:?}") != val_str {
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

fn get_high_performance_flag() -> bool {
    if let Ok(val) = std::env::var("HF_XET_HIGH_PERFORMANCE") {
        parse_bool_value(&val).unwrap_or(false)
    } else if let Ok(val) = std::env::var("HF_XET_HP") {
        parse_bool_value(&val).unwrap_or(false)
    } else {
        false
    }
}

lazy_static! {
    /// To set the high performance mode to true, set either of the following environment variables to 1 or true:
    ///  - HF_XET_HIGH_PERFORMANCE
    ///  - HF_XET_HP
    pub static ref HIGH_PERFORMANCE: bool = get_high_performance_flag();
}

#[inline]
pub fn is_high_performance() -> bool {
    *HIGH_PERFORMANCE
}
