use std::str::FromStr;
use std::sync::LazyLock;

use tracing::{Level, event, info, warn};

use super::ByteSize;
#[cfg(not(target_family = "wasm"))]
use super::TemplatedPathBuf;

#[cfg(not(feature = "elevated_information_level"))]
pub const INFORMATION_LOG_LEVEL: Level = Level::DEBUG;
#[cfg(feature = "elevated_information_level")]
pub const INFORMATION_LOG_LEVEL: Level = Level::INFO;

/// A trait to control how a value is parsed from an environment string or other config source
/// if it's present. Also provides serialization back to a string representation that
/// roundtrips with `parse_user_value`.
pub trait ParsableConfigValue: std::fmt::Debug + Sized {
    fn parse_user_value(value: &str) -> Option<Self>;

    /// Serialize this value to a string that can be parsed back via `parse_user_value`.
    fn to_config_string(&self) -> String;

    /// Try to update this value in place from a string. Returns true on success.
    /// The default implementation delegates to `parse_user_value`, but types like
    /// `ConfigEnum` override this to use context-aware validation.
    fn try_update_in_place(&mut self, value: &str) -> bool {
        if let Some(v) = Self::parse_user_value(value) {
            *self = v;
            true
        } else {
            false
        }
    }

    /// Parse the value, returning the default if it can't be parsed or the string is empty.  
    /// Issue a warning if it can't be parsed.
    fn parse_config_value(variable_name: &str, value: Option<String>, default: Self) -> Self {
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
pub trait FromStrParseable: FromStr + std::fmt::Debug + std::fmt::Display {}

impl<T: FromStrParseable> ParsableConfigValue for T {
    fn parse_user_value(value: &str) -> Option<Self> {
        value.parse::<T>().ok()
    }

    fn to_config_string(&self) -> String {
        self.to_string()
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
/// - false: "0","false","no","n","off" -> false
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

    fn to_config_string(&self) -> String {
        if *self { "true" } else { "false" }.to_owned()
    }
}

/// Enable Option<T> to allow the default value to be None if nothing is set and appear as
/// Some(Value) if the user specifies the value.
impl<T: ParsableConfigValue> ParsableConfigValue for Option<T> {
    fn parse_user_value(value: &str) -> Option<Self> {
        if value.trim().is_empty() {
            return Some(None);
        }
        T::parse_user_value(value).map(Some)
    }

    fn to_config_string(&self) -> String {
        match self {
            Some(v) => v.to_config_string(),
            None => String::new(),
        }
    }
}

/// Implement proper parsing for Duration types as well.
///
/// Now the following suffixes are supported: s, ms, us, ns, m, h, d, etc.;
/// see the humantime crate for the full list.
impl ParsableConfigValue for std::time::Duration {
    fn parse_user_value(value: &str) -> Option<Self> {
        humantime::parse_duration(value).ok()
    }

    fn to_config_string(&self) -> String {
        let total_ms = self.as_millis();
        if self.subsec_nanos() == 0 {
            format!("{}s", self.as_secs())
        } else if self.subsec_nanos().is_multiple_of(1_000_000) {
            format!("{total_ms}ms")
        } else if self.subsec_nanos().is_multiple_of(1_000) {
            format!("{}us", self.as_micros())
        } else {
            format!("{}ns", self.as_nanos())
        }
    }
}

#[cfg(not(target_family = "wasm"))]
impl ParsableConfigValue for TemplatedPathBuf {
    fn parse_user_value(value: &str) -> Option<Self> {
        Some(Self::new(value))
    }

    fn to_config_string(&self) -> String {
        self.template_string()
    }
}

#[macro_export]
macro_rules! test_configurable_constants {
    ($(
        $(#[$meta:meta])*
        ref $name:ident : $type:ty = $value:expr;
    )+) => {
        $(
            #[allow(unused_imports)]
            use $crate::configuration_utils::*;

            $(#[$meta])*
            pub static $name: std::sync::LazyLock<$type> = std::sync::LazyLock::new(|| {
                #[cfg(debug_assertions)]
                {
                    let default_value = $value;
                    let maybe_env_value = std::env::var(concat!("HF_XET_",stringify!($name))).ok();
                    <$type>::parse_config_value(stringify!($name), maybe_env_value, default_value)
                }
                #[cfg(not(debug_assertions))]
                {
                    $value
                }
            });
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
/// use xet_runtime::{test_configurable_constants, test_set_constants};
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

                // Force LazyLock to initialize now:
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
/// use xet_runtime::config::XetConfig;
/// use xet_runtime::test_set_config;
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

/// To set the high performance mode to true, set either of the following environment variables to 1 or true:
///  - HF_XET_HIGH_PERFORMANCE
///  - HF_XET_HP
pub static HIGH_PERFORMANCE: LazyLock<bool> = LazyLock::new(get_high_performance_flag);

#[inline]
pub fn is_high_performance() -> bool {
    *HIGH_PERFORMANCE
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn assert_roundtrip<T: ParsableConfigValue + PartialEq + std::fmt::Debug>(value: T) {
        let s = value.to_config_string();
        let restored = T::parse_user_value(&s).unwrap_or_else(|| {
            panic!("Failed to parse config string '{s}' back into {:?}", std::any::type_name::<T>())
        });
        assert_eq!(value, restored);
    }

    macro_rules! assert_roundtrips {
        ($($value:expr),+ $(,)?) => {
            $(assert_roundtrip($value);)+
        };
    }

    #[test]
    fn test_roundtrip_numeric_primitives() {
        assert_roundtrips!(
            0usize,
            42usize,
            usize::MAX,
            0u8,
            255u8,
            0u16,
            65535u16,
            0u32,
            123456u32,
            0u64,
            u64::MAX,
            0isize,
            -42isize,
            isize::MAX,
            -128i8,
            127i8,
            -32768i16,
            32767i16,
            0i32,
            -123456i32,
            0i64,
            i64::MIN,
            0.0f32,
            3.14f32,
            -1.5f32,
            0.0f64,
            std::f64::consts::PI,
            -1e10f64
        );
    }

    #[test]
    fn test_roundtrip_bool_and_string() {
        assert_roundtrips!(true, false, String::new(), "hello world".to_owned(), "http://localhost:8080".to_owned());
    }

    #[test]
    fn test_roundtrip_byte_size() {
        assert_roundtrips!(
            ByteSize::new(0),
            ByteSize::new(1000),
            ByteSize::new(1_000_000),
            ByteSize::new(8_000_000),
            ByteSize::new(10_000_000_000)
        );
    }

    #[test]
    fn test_roundtrip_duration() {
        assert_roundtrips!(
            Duration::from_secs(0),
            Duration::from_secs(60),
            Duration::from_secs(120),
            Duration::from_millis(200),
            Duration::from_millis(3000),
            Duration::from_secs(360),
            Duration::from_micros(123),
            Duration::from_nanos(123)
        );
    }

    #[test]
    fn test_roundtrip_option_some() {
        assert_roundtrips!(Some(42usize), Some("hello".to_owned()));
    }

    #[test]
    fn test_roundtrip_option_none_string() {
        let none_val: Option<String> = None;
        let s = none_val.to_config_string();
        assert_eq!(s, "");
    }

    #[test]
    fn test_parse_option_empty_string_as_none() {
        assert_eq!(Option::<String>::parse_user_value(""), Some(None));
        assert_eq!(Option::<String>::parse_user_value("   "), Some(None));
        assert_eq!(Option::<usize>::parse_user_value(""), Some(None));
    }

    #[test]
    fn test_parse_bool_empty_string_as_none() {
        assert_eq!(bool::parse_user_value(""), None);
        assert_eq!(bool::parse_user_value("   "), None);
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn test_roundtrip_templated_path_buf() {
        let path = TemplatedPathBuf::new("/some/simple/path");
        let s = path.to_config_string();
        assert_eq!(s, "/some/simple/path");
        let restored = TemplatedPathBuf::parse_user_value(&s).unwrap();
        assert_eq!(path.template_string(), restored.template_string());
    }
}
