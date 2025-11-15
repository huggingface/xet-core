/// Macro to create a configuration value group struct.
///
/// Usage:
/// ```rust
/// use config::config_group;
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

        impl ConfigValueGroup {
            /// Create a new instance with default values only (no environment variable overrides).
            /// This is an alias for `Default::default()`.
            pub fn new() -> Self {
                Self::default()
            }

            /// Apply environment variable overrides to this configuration group.
            ///
            /// The group name is derived from the module path. For example, in module `config::groups::data`,
            /// the env var for TEST_INT would be HF_XET_DATA_TEST_INT.
            pub fn apply_env_overrides(&mut self) {
                // Get the module name from the module path
                // This works by getting the last segment of the module path
                let module_path = module_path!();
                let group_name = module_path
                    .split("::")
                    .last()
                    .unwrap_or("unknown");

                $(
                    let field_name_upper = stringify!($name).to_uppercase();
                    let env_var_name = format!("HF_XET_{}_{}", group_name.to_uppercase(), field_name_upper);
                    let maybe_env_value = std::env::var(&env_var_name).ok();
                    let default_value: $type = $value;
                    self.$name = <$type>::parse(stringify!($name), maybe_env_value, default_value);
                )+
            }
        }

        /// Type alias for easier reference in config aggregation macros
        pub(crate) type ConfigValues = ConfigValueGroup;
    };
}
