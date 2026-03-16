/// Single source of truth for all platform-independent config groups.
/// Invokes the given callback macro with the list of group names.
///
/// The `system_monitor` group is excluded because it is `#[cfg(not(target_family = "wasm"))]`;
/// each consumer appends it separately with the appropriate cfg gate.
#[macro_export]
macro_rules! all_config_groups {
    ($mac:ident) => {
        $mac!(data, shard, deduplication, chunk_cache, client, log, reconstruction, xorb, session);
    };
}

/// Macro to create a configuration value group struct.
///
/// Usage:
/// ```rust,no_run
/// use xet_runtime::config_group;
///
/// mod basic_group {
///     use xet_runtime::config_group;
///
///     config_group!({
///         ref TEST_INT: usize = 42;
///         ref TEST_STRING: String = "default".to_string();
///     });
/// }
/// ```
///
/// This creates a `ConfigValueGroup` struct with the specified fields, default values,
/// environment variable overrides, and string-based accessors.
///
/// Python bindings are provided at the `XetConfig` level, not per-group.
#[macro_export]
macro_rules! config_group {
    ({
        $(
            $(#[$meta:meta])*
            ref $name:ident : $type:ty = $value:expr;
        )+
    }) => {
        #[allow(unused_imports)]
        use $crate::config::ParsableConfigValue;

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
                    const ENV_VAR_NAME: &str = const_str::concat!(
                        "HF_XET_",
                        const_str::convert_ascii_case!(upper, konst::string::rsplit_once(module_path!(), "::").unwrap().1),
                        "_",
                        const_str::convert_ascii_case!(upper, stringify!($name)));

                    let mut maybe_env_value = std::env::var(ENV_VAR_NAME).ok();

                    if maybe_env_value.is_none() {
                            for &(primary_name, alias_name) in $crate::config::ENVIRONMENT_NAME_ALIASES {
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
                    self.$name = <$type>::parse_config_value(stringify!($name), maybe_env_value, default_value);
                }
                )+
            }

            /// Returns the list of field names in this configuration group.
            pub fn field_names() -> &'static [&'static str] {
                &[$(stringify!($name)),+]
            }

            // Internal: used by XetConfig::with_config to apply field updates by name.
            pub(crate) fn update_field(&mut self, name: &str, value: impl ToString) -> Result<(), $crate::config::ConfigError> {
                let value_string = value.to_string();
                match name {
                    $(
                        stringify!($name) => {
                            if !self.$name.try_update_in_place(&value_string) {
                                return Err($crate::config::ConfigError::ParseError {
                                    field: name.to_owned(),
                                    value: value_string,
                                });
                            }
                            Ok(())
                        }
                    )+
                    _ => Err($crate::config::ConfigError::UnknownField(name.to_owned())),
                }
            }

            /// Get a configuration field's string representation by name.
            pub fn get(&self, name: &str) -> Result<String, $crate::config::ConfigError> {
                match name {
                    $(
                        stringify!($name) => Ok(self.$name.to_config_string()),
                    )+
                    _ => Err($crate::config::ConfigError::UnknownField(name.to_owned())),
                }
            }

            // Internal: used by PyXetConfig::with_config to apply field updates from Python values.
            #[cfg(feature = "python")]
            pub(crate) fn update_field_from_python(
                &mut self,
                name: &str,
                value: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<()> {
                match name {
                    $(
                        stringify!($name) => {
                            <$type as $crate::config::python::PythonConfigValue>::update_from_python(&mut self.$name, value)?;
                            Ok(())
                        }
                    )+
                    _ => Err(pyo3::exceptions::PyValueError::new_err(format!("Unknown config field: '{name}'"))),
                }
            }

            // Internal: used by PyXetConfig to return field values as native Python objects.
            #[cfg(feature = "python")]
            pub(crate) fn get_to_python(
                &self,
                name: &str,
                py: pyo3::Python<'_>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                match name {
                    $(
                        stringify!($name) => {
                            <$type as $crate::config::python::PythonConfigValue>::to_python(&self.$name, py)
                        }
                    )+
                    _ => Err(pyo3::exceptions::PyValueError::new_err(format!("Unknown config field: '{name}'"))),
                }
            }

            // Internal: used by PyXetConfig iteration to yield all (field, value) pairs.
            #[cfg(feature = "python")]
            pub(crate) fn items_to_python(
                &self,
                py: pyo3::Python<'_>,
            ) -> pyo3::PyResult<Vec<(&'static str, pyo3::Py<pyo3::PyAny>)>> {
                Ok(vec![
                    $(
                        (stringify!($name),
                         <$type as $crate::config::python::PythonConfigValue>::to_python(&self.$name, py)?),
                    )+
                ])
            }
        }

        /// Type alias for easier reference in config aggregation macros
        pub(crate) type ConfigValues = ConfigValueGroup;
    };
}
