use super::{ConfigError, groups};
use crate::utils::ByteSize;

macro_rules! define_xet_config {
    ($($group:ident),*) => {
        /// Primary configuration struct containing all config sections
        #[derive(Debug, Clone, Default)]
        pub struct XetConfig {
            $(pub $group: groups::$group::ConfigValues,)*
            #[cfg(not(target_family = "wasm"))]
            pub system_monitor: groups::system_monitor::ConfigValues,
        }
    };
}
crate::all_config_groups!(define_xet_config);

macro_rules! impl_xet_config_group_dispatch {
    ($($group:ident),*) => {
        impl XetConfig {
            /// Apply environment variable overrides to all configuration sections.
            /// Returns a new `XetConfig` instance with overrides applied.
            /// The group name for each section is derived from its module name.
            /// Environment variables follow the pattern: HF_XET_{GROUP_NAME}_{FIELD_NAME}
            pub fn with_env_overrides(mut self) -> Self {
                $(self.$group.apply_env_overrides();)*
                #[cfg(not(target_family = "wasm"))]
                self.system_monitor.apply_env_overrides();
                self
            }

            // Internal: dispatches with_config field updates to the correct group.
            fn update_field(&mut self, path: &str, value: impl ToString) -> Result<(), ConfigError> {
                let (group, field) =
                    path.split_once('.').ok_or_else(|| ConfigError::InvalidPath(path.to_owned()))?;
                match group {
                    $(stringify!($group) => self.$group.update_field(field, value),)*
                    #[cfg(not(target_family = "wasm"))]
                    "system_monitor" => self.system_monitor.update_field(field, value),
                    _ => Err(ConfigError::UnknownGroup(group.to_owned())),
                }
            }

            // Internal: dispatches Python with_config field updates to the correct group.
            #[cfg(feature = "python")]
            fn update_field_from_python(
                &mut self,
                path: &str,
                value: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<()> {
                let (group, field) = path.split_once('.').ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        ConfigError::InvalidPath(path.to_owned()).to_string(),
                    )
                })?;
                match group {
                    $(stringify!($group) => self.$group.update_field_from_python(field, value),)*
                    #[cfg(not(target_family = "wasm"))]
                    "system_monitor" => self.system_monitor.update_field_from_python(field, value),
                    _ => Err(pyo3::exceptions::PyValueError::new_err(
                        ConfigError::UnknownGroup(group.to_owned()).to_string(),
                    )),
                }
            }

            // Internal: dispatches Python get/item access to the correct group.
            #[cfg(feature = "python")]
            fn get_field_to_python(
                &self,
                path: &str,
                py: pyo3::Python<'_>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                let (group, field) = path.split_once('.').ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        ConfigError::InvalidPath(path.to_owned()).to_string(),
                    )
                })?;
                match group {
                    $(stringify!($group) => self.$group.get_to_python(field, py),)*
                    #[cfg(not(target_family = "wasm"))]
                    "system_monitor" => self.system_monitor.get_to_python(field, py),
                    _ => Err(pyo3::exceptions::PyValueError::new_err(
                        ConfigError::UnknownGroup(group.to_owned()).to_string(),
                    )),
                }
            }

            /// Get a configuration value's string representation by dotted path
            /// (e.g. "data.max_concurrent_file_ingestion").
            pub fn get(&self, path: &str) -> Result<String, ConfigError> {
                let (group, field) =
                    path.split_once('.').ok_or_else(|| ConfigError::InvalidPath(path.to_owned()))?;
                match group {
                    $(stringify!($group) => self.$group.get(field),)*
                    #[cfg(not(target_family = "wasm"))]
                    "system_monitor" => self.system_monitor.get(field),
                    _ => Err(ConfigError::UnknownGroup(group.to_owned())),
                }
            }

            /// Return all dotted-path keys across all groups.
            pub fn all_keys(&self) -> Vec<String> {
                let mut keys = Vec::new();
                $(
                    for &field in groups::$group::ConfigValueGroup::field_names() {
                        keys.push(format!("{}.{field}", stringify!($group)));
                    }
                )*
                #[cfg(not(target_family = "wasm"))]
                for &field in groups::system_monitor::ConfigValueGroup::field_names() {
                    keys.push(format!("system_monitor.{field}"));
                }
                keys
            }

            // Internal: collects all (key, value) pairs for Python iteration.
            #[cfg(feature = "python")]
            fn all_items_to_python(
                &self,
                py: pyo3::Python<'_>,
            ) -> pyo3::PyResult<Vec<(String, pyo3::Py<pyo3::PyAny>)>> {
                let mut items = Vec::new();
                $(
                    for (field, val) in self.$group.items_to_python(py)? {
                        items.push((format!("{}.{field}", stringify!($group)), val));
                    }
                )*
                #[cfg(not(target_family = "wasm"))]
                for (field, val) in self.system_monitor.items_to_python(py)? {
                    items.push((format!("system_monitor.{field}"), val));
                }
                Ok(items)
            }
        }
    };
}
crate::all_config_groups!(impl_xet_config_group_dispatch);

impl XetConfig {
    /// Create a new XetConfig instance with default values and apply environment variable overrides.
    /// If high performance mode is enabled (via environment variables HF_XET_HIGH_PERFORMANCE or HF_XET_HP),
    /// also applies high performance settings automatically.
    /// This is equivalent to `XetConfig::default().with_env_overrides()`, with `with_high_performance()`
    /// called conditionally if high performance mode is enabled.
    pub fn new() -> Self {
        let mut config = Self::default().with_env_overrides();
        if crate::utils::is_high_performance() {
            config = config.with_high_performance();
        }
        config
    }

    /// Apply high performance mode settings to this configuration.
    /// Returns a new `XetConfig` instance with high performance values applied.
    ///
    /// High performance mode can be enabled by setting either of these environment variables:
    /// - HF_XET_HIGH_PERFORMANCE
    /// - HF_XET_HP
    ///
    /// This method sets the following values to their high performance defaults:
    /// - data.max_concurrent_file_ingestion: 8 -> 100
    ///
    /// Note: This method is automatically called by `XetConfig::new()` if high performance mode is enabled.
    pub fn with_high_performance(mut self) -> Self {
        self.data.max_concurrent_file_ingestion = 100;

        // Adjustments to the adaptive concurrency control.
        self.client.ac_max_upload_concurrency = 124;
        self.client.ac_max_download_concurrency = 124;
        self.client.ac_min_upload_concurrency = 4;
        self.client.ac_min_download_concurrency = 4;
        self.client.ac_initial_upload_concurrency = 16;
        self.client.ac_initial_download_concurrency = 16;

        self.reconstruction.min_reconstruction_fetch_size = ByteSize::from("1gb");
        self.reconstruction.max_reconstruction_fetch_size = ByteSize::from("16gb");
        self.reconstruction.download_buffer_size = ByteSize::from("16gb");
        self.reconstruction.download_buffer_perfile_size = ByteSize::from("2gb");
        self.reconstruction.download_buffer_limit = ByteSize::from("64gb");

        self
    }

    /// Return a new config with the value at the given dotted path updated
    /// (e.g. "data.max_concurrent_file_ingestion").
    pub fn with_config(mut self, path: &str, value: impl ToString) -> Result<Self, ConfigError> {
        self.update_field(path, value)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_config_and_get() {
        let config = XetConfig::default()
            .with_config("data.max_concurrent_file_ingestion", "16")
            .unwrap();
        assert_eq!(config.data.max_concurrent_file_ingestion, 16);
        assert_eq!(config.get("data.max_concurrent_file_ingestion").unwrap(), "16");
    }

    #[test]
    fn test_with_config_sets_multiple_field_types() {
        let config = XetConfig::default()
            .with_config("data.max_concurrent_file_ingestion", 64)
            .unwrap()
            .with_config("data.progress_update_interval", "500ms")
            .unwrap()
            .with_config("data.ingestion_block_size", "16MB")
            .unwrap()
            .with_config("client.enable_adaptive_concurrency", "false")
            .unwrap()
            .with_config("data.default_cas_endpoint", "http://example.com:9090")
            .unwrap();

        assert_eq!(config.data.max_concurrent_file_ingestion, 64);
        assert_eq!(config.data.progress_update_interval, std::time::Duration::from_millis(500));
        assert_eq!(config.data.ingestion_block_size.as_u64(), 16_000_000);
        assert!(!config.client.enable_adaptive_concurrency);
        assert_eq!(config.data.default_cas_endpoint, "http://example.com:9090");
    }

    #[test]
    fn test_with_config_chained() {
        let config = XetConfig::default()
            .with_config("data.max_concurrent_file_ingestion", 16)
            .unwrap()
            .with_config("client.retry_max_attempts", 10)
            .unwrap();
        assert_eq!(config.data.max_concurrent_file_ingestion, 16);
        assert_eq!(config.client.retry_max_attempts, 10);
    }

    #[test]
    fn test_unknown_field_error() {
        let result = XetConfig::default().with_config("data.nonexistent_field", "42");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::UnknownField(name) => assert_eq!(name, "nonexistent_field"),
            e => panic!("Expected UnknownField error, got: {e:?}"),
        }
    }

    #[test]
    fn test_unknown_group_error() {
        let result = XetConfig::default().with_config("nonexistent_group.field", "42");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::UnknownGroup(name) => assert_eq!(name, "nonexistent_group"),
            e => panic!("Expected UnknownGroup error, got: {e:?}"),
        }
    }

    #[test]
    fn test_invalid_path_error() {
        let result = XetConfig::default().with_config("no_dot_in_path", "42");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::InvalidPath(path) => assert_eq!(path, "no_dot_in_path"),
            e => panic!("Expected InvalidPath error, got: {e:?}"),
        }
    }

    #[test]
    fn test_parse_error() {
        let result = XetConfig::default().with_config("data.max_concurrent_file_ingestion", "not_a_number");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::ParseError { field, value } => {
                assert_eq!(field, "max_concurrent_file_ingestion");
                assert_eq!(value, "not_a_number");
            },
            e => panic!("Expected ParseError, got: {e:?}"),
        }
    }

    #[test]
    fn test_field_names() {
        let names = crate::config::groups::data::ConfigValueGroup::field_names();
        assert!(names.contains(&"max_concurrent_file_ingestion"));
        assert!(names.contains(&"ingestion_block_size"));
        assert!(names.contains(&"progress_update_interval"));
    }

    #[test]
    fn test_get_unknown_field_error() {
        let config = XetConfig::default();
        let result = config.get("data.nonexistent");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::UnknownField(name) => assert_eq!(name, "nonexistent"),
            e => panic!("Expected UnknownField error, got: {e:?}"),
        }
    }

    #[test]
    fn test_with_config_roundtrip_all_groups() {
        let config = XetConfig::default().with_config("shard.target_size", "2048").unwrap();
        assert_eq!(config.get("shard.target_size").unwrap(), "2048");

        let config = XetConfig::default()
            .with_config("deduplication.min_n_chunks_per_range", "4")
            .unwrap();
        assert_eq!(config.get("deduplication.min_n_chunks_per_range").unwrap(), "4");

        let config = XetConfig::default()
            .with_config("chunk_cache.size_bytes", "5000000000")
            .unwrap();
        assert_eq!(config.get("chunk_cache.size_bytes").unwrap(), "5000000000");

        let config = XetConfig::default().with_config("client.retry_max_attempts", "10").unwrap();
        assert_eq!(config.get("client.retry_max_attempts").unwrap(), "10");

        let config = XetConfig::default()
            .with_config("reconstruction.target_block_completion_time", "30s")
            .unwrap();
        assert_eq!(config.get("reconstruction.target_block_completion_time").unwrap(), "30s");

        let config = XetConfig::default()
            .with_config("xorb.compression_scheme_retest_interval", "64")
            .unwrap();
        assert_eq!(config.get("xorb.compression_scheme_retest_interval").unwrap(), "64");
    }

    #[test]
    fn test_with_config_option_empty_string_sets_none() {
        let config = XetConfig::default().with_config("log.dest", "").unwrap();
        assert_eq!(config.log.dest, None);
        assert_eq!(config.get("log.dest").unwrap(), "");
    }

    #[test]
    fn test_with_config_option_can_transition_some_to_none() {
        let config = XetConfig::default()
            .with_config("log.dest", "path/to/log")
            .unwrap()
            .with_config("log.dest", "")
            .unwrap();
        assert_eq!(config.log.dest, None);
    }
}

#[cfg(feature = "python")]
pub mod py_xet_config {
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    use super::*;

    #[pyclass(name = "XetConfig")]
    pub struct PyXetConfig {
        inner: XetConfig,
    }

    impl From<XetConfig> for PyXetConfig {
        fn from(inner: XetConfig) -> Self {
            Self { inner }
        }
    }

    impl PyXetConfig {
        pub fn inner(&self) -> &XetConfig {
            &self.inner
        }
    }

    #[pymethods]
    impl PyXetConfig {
        #[new]
        fn py_new() -> Self {
            Self {
                inner: XetConfig::new(),
            }
        }

        /// Return a new XetConfig with one or more values updated.
        ///
        /// Can be called in two ways:
        ///   config.with_config("group.field", value)          -- single update
        ///   config.with_config({"group.field": value, ...})   -- batch update
        #[pyo3(name = "with_config")]
        #[pyo3(signature = (name_or_dict, value=None))]
        fn py_with_config(&self, name_or_dict: &Bound<'_, PyAny>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
            let mut new_inner = self.inner.clone();

            if let Ok(dict) = name_or_dict.downcast::<PyDict>() {
                if value.is_some() {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "with_config(dict) does not accept a second argument",
                    ));
                }
                for (key, val) in dict.iter() {
                    let key_str: String = key.extract()?;
                    new_inner.update_field_from_python(&key_str, &val)?;
                }
            } else {
                let name: String = name_or_dict.extract()?;
                let val = value.ok_or_else(|| {
                    pyo3::exceptions::PyTypeError::new_err("with_config(name, value) requires a value argument")
                })?;
                new_inner.update_field_from_python(&name, val)?;
            }

            Ok(Self { inner: new_inner })
        }

        /// Get a configuration value as its native Python type by dotted path
        /// (e.g. "data.max_concurrent_file_ingestion").
        #[pyo3(name = "get")]
        fn py_get(&self, py: Python<'_>, path: &str) -> PyResult<Py<PyAny>> {
            self.inner.get_field_to_python(path, py)
        }

        fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
            self.inner
                .get_field_to_python(key, py)
                .map_err(|_| pyo3::exceptions::PyKeyError::new_err(key.to_owned()))
        }

        /// Return all (key, value) pairs as a list of tuples.
        /// Keys are dotted paths like "data.max_concurrent_file_ingestion".
        fn items(&self, py: Python<'_>) -> PyResult<Vec<(String, Py<PyAny>)>> {
            self.inner.all_items_to_python(py)
        }

        /// Return all dotted-path keys.
        fn keys(&self) -> Vec<String> {
            self.inner.all_keys()
        }

        fn __len__(&self) -> usize {
            self.inner.all_keys().len()
        }

        fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyXetConfigIter>> {
            let items = slf.inner.all_items_to_python(py)?;
            Py::new(py, PyXetConfigIter { items, index: 0 })
        }

        fn __repr__(&self) -> String {
            format!("XetConfig({:?})", self.inner)
        }

        fn __str__(&self) -> String {
            format!("{:?}", self.inner)
        }
    }

    #[pyclass]
    struct PyXetConfigIter {
        items: Vec<(String, Py<PyAny>)>,
        index: usize,
    }

    #[pymethods]
    impl PyXetConfigIter {
        fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
            slf
        }

        fn __next__(&mut self) -> Option<(String, Py<PyAny>)> {
            if self.index < self.items.len() {
                let item = self.items[self.index].clone();
                self.index += 1;
                Some(item)
            } else {
                None
            }
        }
    }
}
