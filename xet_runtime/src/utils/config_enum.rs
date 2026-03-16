use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use tracing::{event, info, warn};

use super::configuration_utils::{INFORMATION_LOG_LEVEL, ParsableConfigValue};

/// A config value restricted to a fixed set of valid lowercase string options.
///
/// Stores the normalized (lowercased) value and validates against `valid_values`
/// at parse time. If the user provides an invalid value via environment variable,
/// a warning is logged and the default is used instead.
///
/// # Usage in `config_group!`
///
/// ```rust,ignore
/// use crate::utils::ConfigEnum;
///
/// crate::config_group!({
///     ref compression_policy: ConfigEnum = ConfigEnum::new("auto", &["", "auto", "none", "lz4"]);
/// });
/// ```
#[derive(Clone)]
pub struct ConfigEnum {
    value: String,
    valid_values: &'static [&'static str],
}

impl ConfigEnum {
    pub fn new(default: &str, valid_values: &'static [&'static str]) -> Self {
        let lower = default.to_lowercase();
        debug_assert!(
            valid_values.iter().any(|v| v.to_lowercase() == lower),
            "Default value \"{default}\" is not in the valid values list: {valid_values:?}"
        );
        ConfigEnum {
            value: lower,
            valid_values,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn valid_values(&self) -> &'static [&'static str] {
        self.valid_values
    }

    /// Set the value if it is valid (case-insensitive), otherwise return an error.
    pub fn try_set(&mut self, value: &str) -> Result<(), String> {
        let lower = value.to_lowercase();
        if self.valid_values.iter().any(|v| v.to_lowercase() == lower) {
            self.value = lower;
            Ok(())
        } else {
            Err(format!("\"{value}\" is not a valid value. Valid values are: {:?}", self.valid_values))
        }
    }

    /// Parse the stored value into a target type via `FromStr`, matching the
    /// familiar `str::parse::<T>()` signature.
    ///
    /// In debug builds, asserts that *every* entry in `valid_values` can be
    /// successfully parsed into `T`, catching mismatches between the config's
    /// allowed strings and the target type's parser at development time.
    pub fn parse<T>(&self) -> Result<T, T::Err>
    where
        T: FromStr,
        T::Err: fmt::Debug + fmt::Display,
    {
        #[cfg(debug_assertions)]
        for v in self.valid_values {
            if let Err(e) = v.parse::<T>() {
                panic!("ConfigEnum valid value \"{v}\" cannot be parsed into {}: {e}", std::any::type_name::<T>());
            }
        }
        self.value.parse::<T>()
    }
}

impl fmt::Debug for ConfigEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}

impl fmt::Display for ConfigEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Deref for ConfigEnum {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl AsRef<str> for ConfigEnum {
    fn as_ref(&self) -> &str {
        &self.value
    }
}

impl PartialEq<str> for ConfigEnum {
    fn eq(&self, other: &str) -> bool {
        self.value == other.to_lowercase()
    }
}

impl PartialEq<&str> for ConfigEnum {
    fn eq(&self, other: &&str) -> bool {
        self.value == other.to_lowercase()
    }
}

impl PartialEq for ConfigEnum {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for ConfigEnum {}

impl ParsableConfigValue for ConfigEnum {
    fn parse_user_value(_value: &str) -> Option<Self> {
        None
    }

    fn parse_config_value(variable_name: &str, value: Option<String>, default: Self) -> Self {
        match value {
            Some(v) => {
                let lower = v.to_lowercase();
                if default.valid_values.iter().any(|valid| valid.to_lowercase() == lower) {
                    info!("Config: {variable_name} = {lower:?} (user set)");
                    ConfigEnum {
                        value: lower,
                        valid_values: default.valid_values,
                    }
                } else {
                    warn!(
                        "Configuration value \"{v}\" for {variable_name} is not valid. \
                         Valid values are: {:?}. Reverting to default \"{}\".",
                        default.valid_values, default.value
                    );
                    info!("Config: {variable_name} = {:?} (default due to invalid value)", default.value);
                    default
                }
            },
            None => {
                event!(INFORMATION_LOG_LEVEL, "Config: {variable_name} = {:?} (default)", default.value);
                default
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::ParseIntError;

    use super::*;

    const VALID: &[&str] = &["", "auto", "none", "lz4", "bg4-lz4"];
    const INT_VALID: &[&str] = &["1", "2", "3"];

    #[test]
    fn test_new_default_value() {
        let ce = ConfigEnum::new("auto", VALID);
        assert_eq!(ce.as_str(), "auto");
    }

    #[test]
    fn test_new_normalizes_to_lowercase() {
        let ce = ConfigEnum::new("AUTO", VALID);
        assert_eq!(ce.as_str(), "auto");
    }

    #[test]
    fn test_deref_and_asref() {
        let ce = ConfigEnum::new("lz4", VALID);
        let s: &str = &ce;
        assert_eq!(s, "lz4");
        assert_eq!(ce.as_ref(), "lz4");
    }

    #[test]
    fn test_partial_eq_str() {
        let ce = ConfigEnum::new("lz4", VALID);
        assert_eq!(ce, "lz4");
        assert_eq!(ce, "LZ4");
        assert_ne!(ce, "auto");
    }

    #[test]
    fn test_partial_eq_str_ref() {
        let ce = ConfigEnum::new("lz4", VALID);
        assert_eq!(ce, "lz4");
    }

    #[test]
    fn test_partial_eq_self() {
        let a = ConfigEnum::new("lz4", VALID);
        let b = ConfigEnum::new("lz4", VALID);
        assert_eq!(a, b);
    }

    #[test]
    fn test_display() {
        let ce = ConfigEnum::new("bg4-lz4", VALID);
        assert_eq!(format!("{ce}"), "bg4-lz4");
    }

    #[test]
    fn test_debug() {
        let ce = ConfigEnum::new("bg4-lz4", VALID);
        assert_eq!(format!("{ce:?}"), "\"bg4-lz4\"");
    }

    #[test]
    fn test_parse_valid_value() {
        let default = ConfigEnum::new("auto", VALID);
        let result = ConfigEnum::parse_config_value("test", Some("LZ4".to_string()), default);
        assert_eq!(result.as_str(), "lz4");
    }

    #[test]
    fn test_parse_invalid_value_returns_default() {
        let default = ConfigEnum::new("auto", VALID);
        let result = ConfigEnum::parse_config_value("test", Some("zstd".to_string()), default);
        assert_eq!(result.as_str(), "auto");
    }

    #[test]
    fn test_parse_none_returns_default() {
        let default = ConfigEnum::new("auto", VALID);
        let result = ConfigEnum::parse_config_value("test", None, default);
        assert_eq!(result.as_str(), "auto");
    }

    #[test]
    fn test_parse_empty_string_valid() {
        let default = ConfigEnum::new("auto", VALID);
        let result = ConfigEnum::parse_config_value("test", Some("".to_string()), default);
        assert_eq!(result.as_str(), "");
    }

    #[test]
    #[should_panic(expected = "not in the valid values list")]
    #[cfg(debug_assertions)]
    fn test_new_invalid_default_panics() {
        let _ = ConfigEnum::new("invalid", VALID);
    }

    #[test]
    fn test_valid_values_accessor() {
        let ce = ConfigEnum::new("auto", VALID);
        assert_eq!(ce.valid_values(), VALID);
    }

    #[test]
    fn test_try_set_valid() {
        let mut ce = ConfigEnum::new("auto", VALID);
        assert!(ce.try_set("lz4").is_ok());
        assert_eq!(ce.as_str(), "lz4");
    }

    #[test]
    fn test_try_set_case_insensitive() {
        let mut ce = ConfigEnum::new("auto", VALID);
        assert!(ce.try_set("LZ4").is_ok());
        assert_eq!(ce.as_str(), "lz4");
    }

    #[test]
    fn test_try_set_invalid() {
        let mut ce = ConfigEnum::new("auto", VALID);
        assert!(ce.try_set("zstd").is_err());
        assert_eq!(ce.as_str(), "auto");
    }

    #[test]
    fn test_try_set_empty_string() {
        let mut ce = ConfigEnum::new("auto", VALID);
        assert!(ce.try_set("").is_ok());
        assert_eq!(ce.as_str(), "");
    }

    #[test]
    fn test_parse_success() {
        let ce = ConfigEnum::new("2", INT_VALID);
        let val: Result<u32, ParseIntError> = ce.parse();
        assert_eq!(val.unwrap(), 2);
    }

    #[test]
    fn test_parse_all_values_parseable() {
        let ce = ConfigEnum::new("1", INT_VALID);
        let _: u32 = ce.parse().unwrap();
    }

    #[test]
    #[should_panic(expected = "cannot be parsed")]
    #[cfg(debug_assertions)]
    fn test_parse_panics_on_unparseable_valid_value() {
        let ce = ConfigEnum::new("auto", VALID);
        let _: Result<u32, ParseIntError> = ce.parse();
    }
}
