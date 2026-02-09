use std::str::FromStr;

use tracing::{Level, event, info, warn};

use crate::{ByteSize, TemplatedPathBuf};

#[cfg(not(feature = "elevated_information_level"))]
pub const INFORMATION_LOG_LEVEL: Level = Level::DEBUG;
#[cfg(feature = "elevated_information_level")]
pub const INFORMATION_LOG_LEVEL: Level = Level::INFO;

/// A trait to control how a value is parsed from an environment string or other config source
/// if it's present.
///
/// The main reason to do things like this is to
pub trait ParsableConfigValue: std::fmt::Debug + Sized {
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
                event!(INFORMATION_LOG_LEVEL, "Config: {variable_name} = {default:?} (default)");
                default
            },
        }
    }
}

/// Most values work with the FromStr implementation, but we want to override the behavior for some types
/// (e.g. Option<T> and bool) to have custom parsing behavior.
pub trait FromStrParseable: FromStr + std::fmt::Debug {}

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
impl ParsableConfigValue for std::time::Duration {
    fn parse_user_value(value: &str) -> Option<Self> {
        duration_str::parse(value).ok()
    }
}

impl ParsableConfigValue for TemplatedPathBuf {
    fn parse_user_value(value: &str) -> Option<Self> {
        Some(Self::new(value))
    }
}

// Reexport this so that dependencies don't have weird other dependencies
pub use lazy_static::lazy_static;

#[macro_export]
macro_rules! test_configurable_constants {
    ($(
        $(#[$meta:meta])*
        ref $name:ident : $type:ty = $value:expr;
    )+) => {
        $(
            #[allow(unused_imports)]
            use $crate::configuration_utils::*;

            lazy_static! {
                $(#[$meta])*
                pub static ref $name: $type = {
                    #[cfg(debug_assertions)]
                    {
                        let default_value = $value;
                        let maybe_env_value = std::env::var(concat!("HF_XET_",stringify!($name))).ok();
                        <$type>::parse(stringify!($name), maybe_env_value, default_value)
                    }
                    #[cfg(not(debug_assertions))]
                    {
                        $value
                    }
                };
            }
        )+
    };
}

pub use ctor as ctor_reexport;

#[cfg(not(doctest))]
/// A macro for **tests** that sets `HF_XET_<CONSTANT_NAME>` to `$value` **before**
/// the constant is initialized, and then checks that the constant actually picks up
/// that value. If the constant was already accessed (thus initialized), or if it
/// doesn't match after being set, this macro panics.
///
/// Typically you would document *the macro itself* here, rather than placing
/// doc comments above each call to `test_set_constants!`, because it doesn't
/// define a new item.
///
/// # Example
/// ```rust
/// use utils::{test_configurable_constants, test_set_constants};
/// test_configurable_constants! {
///    /// Target chunk size
///    ref CHUNK_TARGET_SIZE: u64 = 1024;
///
///    /// Max Chunk size, only adjustable in testing mode.
///    ref MAX_CHUNK_SIZE: u64 = 4096;
/// }
///
/// test_set_constants! {
///    CHUNK_TARGET_SIZE = 2048;
/// }
/// assert_eq!(*CHUNK_TARGET_SIZE, 2048);
/// ```
#[macro_export]
macro_rules! test_set_constants {
    ($(
        $var_name:ident = $val:expr;
    )+) => {
        use $crate::configuration_utils::ctor_reexport as ctor;

        #[ctor::ctor]
        fn set_constants_on_load() {
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
                        "test_set_constants! failed: wanted {} to be {:?}, but got {:?}",
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

#[cfg(not(doctest))]
/// A macro for **tests** that sets config group environment variables **before**
/// XetRuntime is initialized. The environment variables follow the pattern
/// `HF_XET_{GROUP_NAME}_{FIELD_NAME}`.
///
/// This macro uses `ctor` to run on module load, ensuring environment variables
/// are set before any config values are read.
///
/// # Example
/// ```rust
/// use config::XetConfig;
/// use utils::test_set_config;
///
/// test_set_config! {
///     data {
///         max_concurrent_uploads = 16;
///         max_concurrent_downloads = 20;
///     }
///     client {
///         upload_reporting_block_size = 1024000;
///     }
/// }
///
/// // Now XetConfig::new() will pick up these values
/// let config = XetConfig::new();
/// assert_eq!(config.data.max_concurrent_uploads, 16);
/// ```
#[macro_export]
macro_rules! test_set_config {
    ($(
        $group_name:ident {
            $(
                $field_name:ident = $val:expr;
            )+
        }
    )+) => {
        use $crate::configuration_utils::ctor_reexport as config_ctor;

        #[config_ctor::ctor]
        fn set_config_on_load() {
            $(
                let group_name_upper = stringify!($group_name).to_uppercase();
                $(
                    let val = $val;
                    let val_str = format!("{val:?}");
                    let field_name_upper = stringify!($field_name).to_uppercase();

                    // Construct the environment variable name: HF_XET_{GROUP_NAME}_{FIELD_NAME}
                    let env_name = format!("HF_XET_{}_{}", group_name_upper, field_name_upper);

                    // Set the environment
                    unsafe {
                        std::env::set_var(&env_name, &val_str);
                    }

                    eprintln!("> Set config {}.{} to {:?} (env: {})",
                            stringify!($group_name),
                            stringify!($field_name),
                            val,
                            env_name);
                )+
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
