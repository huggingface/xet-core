/// Macro to create a configuration value group struct.
///
/// Usage:
/// ```rust
/// use xet_config::config_group;
///
/// config_group!({
///     ref TEST_INT: usize = 42;
///     ref TEST_STRING: String = "default".to_string();
/// });
/// ```
///
/// This creates a `ConfigValueGroup` struct with the specified fields and a `new(group_name: &str)` method
/// that loads values from environment variables prefixed with the group name.
#[macro_export]
macro_rules! config_group {
    ({
        $(
            $(#[$meta:meta])*
            ref $name:ident : $type:ty = $value:expr;
        )+
    }) => {
        #[allow(unused_imports)]
        use $crate::ParsableConfigValue;

        /// Name of this configuration struct, accessible for macro generation
        pub const CONFIG_VALUES_NAME: &str = "ConfigValueGroup";

        /// ConfigValueGroup struct containing all configurable values
        #[derive(Debug, Clone)]
        pub struct ConfigValueGroup {
            $(
                $(#[$meta])*
                #[allow(non_snake_case)]
                pub $name: $type,
            )+
        }

        impl Default for ConfigValueGroup {
            /// Create a new instance with default values only (no environment variable overrides).
            fn default() -> Self {
                Self {
                    $(
                        $name: {
                            let v: $type = $value;
                            v
                        },
                    )+
                }
            }
        }

        impl AsRef<ConfigValueGroup> for ConfigValueGroup {
            fn as_ref(&self) -> &ConfigValueGroup {
                self
            }
        }

        impl ConfigValueGroup {
            /// Create a new instance with default values only (no environment variable overrides).
            /// This is an alias for `Default::default()`.
            pub fn new() -> Self {
                Self::default()
            }

            /// Apply environment variable overrides to this configuration group.
            ///
            /// The group name is derived from the module path. For example, in module `xet_config::groups::data`,
            /// the env var for TEST_INT would be HF_XET_DATA_TEST_INT.
            pub fn apply_env_overrides(&mut self) {

                $(

                    {
                    // Get module name at compile time using konst and build env var name in one line
                    const ENV_VAR_NAME: &str = const_str::concat!(
                        "HF_XET_",
                        const_str::convert_ascii_case!(upper, konst::string::rsplit_once(module_path!(), "::").unwrap().1),
                        "_",
                        const_str::convert_ascii_case!(upper, stringify!($name)));

                    // Check the primary environment variable first
                    let mut maybe_env_value = std::env::var(ENV_VAR_NAME).ok();

                    // If not found, check aliases
                    // The compiler should optimize this loop since ENVIRONMENT_NAME_ALIASES is a const
                    if maybe_env_value.is_none() {
                            for &(primary_name, alias_name) in $crate::ENVIRONMENT_NAME_ALIASES {
                                if primary_name == ENV_VAR_NAME {
                                    let alt_env_value = std::env::var(alias_name).ok();
                                    if alt_env_value.is_some() {
                                        maybe_env_value = alt_env_value;
                                        break;
                                    }
                                }
                            }
                    }

                    let default_value: $type = $value;
                    self.$name = <$type>::parse(stringify!($name), maybe_env_value, default_value);
                }
                )+
            }
        }

        /// Type alias for easier reference in config aggregation macros
        pub(crate) type ConfigValues = ConfigValueGroup;
    };
}
