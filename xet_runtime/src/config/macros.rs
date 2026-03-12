/// Macro to create a configuration value group struct.
///
/// Usage:
/// ```rust,no_run
/// use xet_runtime::config_group;
///
/// // Without Python class name:
/// mod basic_group {
///     use xet_runtime::config_group;
///
///     config_group!({
///         ref TEST_INT: usize = 42;
///         ref TEST_STRING: String = "default".to_string();
///     });
/// }
///
/// // With Python class name (enables PyO3 bindings when "python" feature is active):
/// mod python_named_group {
///     use xet_runtime::config_group;
///
///     config_group!("MyConfig", {
///         ref TEST_INT: usize = 42;
///         ref TEST_STRING: String = "default".to_string();
///     });
/// }
/// ```
///
/// This creates a `ConfigValueGroup` struct with the specified fields, default values,
/// environment variable overrides, and string-based accessors.
///
/// When a Python class name is provided and the `python` feature is enabled, the macro
/// also generates `#[pyclass]` and `#[pymethods]` implementations with typed getters
/// for each field. Group structs are read-only from Python; use `XetConfig.with_config()`
/// to create modified copies.
#[macro_export]
macro_rules! config_group {
    // Arm without Python class name -- no PyO3 bindings
    ({
        $(
            $(#[$meta:meta])*
            ref $name:ident : $type:ty = $value:expr;
        )+
    }) => {
        $crate::config_group!(@impl, {
            $(
                $(#[$meta])*
                ref $name : $type = $value;
            )+
        });
    };

    // Arm with Python class name -- generates PyO3 bindings when "python" feature is active
    ($py_name:literal, {
        $(
            $(#[$meta:meta])*
            ref $name:ident : $type:ty = $value:expr;
        )+
    }) => {
        $crate::config_group!(@impl, {
            $(
                $(#[$meta])*
                ref $name : $type = $value;
            )+
        });

        #[cfg(feature = "python")]
        $crate::config_group!(@python_impl $py_name, {
            $(
                ref $name : $type;
            )+
        });
    };

    // Internal arm: core struct definition, Default, set/get, etc.
    (@impl, {
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
                    self.$name = <$type>::parse(stringify!($name), maybe_env_value, default_value);
                }
                )+
            }

            /// Returns the list of field names in this configuration group.
            pub fn field_names() -> &'static [&'static str] {
                &[$(stringify!($name)),+]
            }

            /// Set a configuration field by name, accepting any value that implements `ToString`.
            pub fn set(&mut self, name: &str, value: impl ToString) -> Result<(), $crate::config::ConfigError> {
                let value_string = value.to_string();
                match name {
                    $(
                        stringify!($name) => {
                            self.$name = <$type>::parse_user_value(&value_string)
                                .ok_or_else(|| $crate::config::ConfigError::ParseError {
                                    field: name.to_owned(),
                                    value: value_string,
                                })?;
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

            /// Set a configuration field by name from a Python value.
            #[cfg(feature = "python")]
            pub fn set_from_python(
                &mut self,
                name: &str,
                value: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<()> {
                match name {
                    $(
                        stringify!($name) => {
                            self.$name = <$type as $crate::config::python::PythonConfigValue>::from_python(value)?;
                            Ok(())
                        }
                    )+
                    _ => Err(pyo3::exceptions::PyValueError::new_err(format!("Unknown config field: '{name}'"))),
                }
            }
        }

        /// Type alias for easier reference in config aggregation macros
        pub(crate) type ConfigValues = ConfigValueGroup;
    };

    // Internal arm: Python bindings generation
    (@python_impl $py_name:literal, {
        $(
            ref $name:ident : $type:ty;
        )+
    }) => {
        use $crate::config::python::PythonConfigValue;

        #[pyo3::pyclass(name = $py_name)]
        pub struct PyConfigValueGroup {
            inner: ConfigValueGroup,
        }

        impl From<ConfigValueGroup> for PyConfigValueGroup {
            fn from(inner: ConfigValueGroup) -> Self {
                Self { inner }
            }
        }

        impl From<PyConfigValueGroup> for ConfigValueGroup {
            fn from(wrapper: PyConfigValueGroup) -> Self {
                wrapper.inner
            }
        }

        impl PyConfigValueGroup {
            pub fn inner(&self) -> &ConfigValueGroup {
                &self.inner
            }
        }

        #[pyo3::pymethods]
        impl PyConfigValueGroup {
            /// Get a field's string representation by name.
            #[pyo3(name = "get")]
            fn py_get(&self, name: &str) -> pyo3::PyResult<String> {
                self.inner.get(name)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
            }

            /// Return the list of field names.
            #[staticmethod]
            fn field_names() -> Vec<&'static str> {
                ConfigValueGroup::field_names().to_vec()
            }

            fn __repr__(&self) -> String {
                format!("{}({:?})", $py_name, self.inner)
            }

            fn __str__(&self) -> String {
                format!("{:?}", self.inner)
            }

            $(
                #[getter]
                fn $name(&self, py: pyo3::Python<'_>) -> pyo3::Py<pyo3::PyAny> {
                    <$type as PythonConfigValue>::to_python(&self.inner.$name, py)
                }
            )+
        }

        pub type PyConfigValues = PyConfigValueGroup;
    };
}
