use super::{ConfigError, groups};
use crate::utils::ByteSize;

/// Primary configuration struct containing all config sections
#[derive(Debug, Clone, Default)]
pub struct XetConfig {
    pub data: groups::data::ConfigValues,
    pub shard: groups::shard::ConfigValues,
    pub deduplication: groups::deduplication::ConfigValues,
    pub chunk_cache: groups::chunk_cache::ConfigValues,
    pub client: groups::client::ConfigValues,
    pub log: groups::log::ConfigValues,
    pub reconstruction: groups::reconstruction::ConfigValues,
    pub xorb: groups::xorb::ConfigValues,
    #[cfg(not(target_family = "wasm"))]
    pub system_monitor: groups::system_monitor::ConfigValues,
}

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

    /// Apply environment variable overrides to all configuration sections.
    /// Returns a new `XetConfig` instance with overrides applied.
    /// The group name for each section is derived from its module name.
    /// Environment variables follow the pattern: HF_XET_{GROUP_NAME}_{FIELD_NAME}
    pub fn with_env_overrides(mut self) -> Self {
        self.data.apply_env_overrides();
        self.shard.apply_env_overrides();
        self.deduplication.apply_env_overrides();
        self.chunk_cache.apply_env_overrides();
        self.client.apply_env_overrides();
        self.log.apply_env_overrides();
        self.reconstruction.apply_env_overrides();
        self.xorb.apply_env_overrides();
        #[cfg(not(target_family = "wasm"))]
        self.system_monitor.apply_env_overrides();
        self
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

    /// Set a configuration value by dotted path (e.g. "data.max_concurrent_file_ingestion").
    pub fn set(&mut self, path: &str, value: impl ToString) -> Result<(), ConfigError> {
        let (group, field) = path.split_once('.').ok_or_else(|| ConfigError::InvalidPath(path.to_owned()))?;

        match group {
            "data" => self.data.set(field, value),
            "shard" => self.shard.set(field, value),
            "deduplication" => self.deduplication.set(field, value),
            "chunk_cache" => self.chunk_cache.set(field, value),
            "client" => self.client.set(field, value),
            "log" => self.log.set(field, value),
            "reconstruction" => self.reconstruction.set(field, value),
            "xorb" => self.xorb.set(field, value),
            #[cfg(not(target_family = "wasm"))]
            "system_monitor" => self.system_monitor.set(field, value),
            _ => Err(ConfigError::UnknownGroup(group.to_owned())),
        }
    }

    /// Get a configuration value's string representation by dotted path
    /// (e.g. "data.max_concurrent_file_ingestion").
    pub fn get(&self, path: &str) -> Result<String, ConfigError> {
        let (group, field) = path.split_once('.').ok_or_else(|| ConfigError::InvalidPath(path.to_owned()))?;

        match group {
            "data" => self.data.get(field),
            "shard" => self.shard.get(field),
            "deduplication" => self.deduplication.get(field),
            "chunk_cache" => self.chunk_cache.get(field),
            "client" => self.client.get(field),
            "log" => self.log.get(field),
            "reconstruction" => self.reconstruction.get(field),
            "xorb" => self.xorb.get(field),
            #[cfg(not(target_family = "wasm"))]
            "system_monitor" => self.system_monitor.get(field),
            _ => Err(ConfigError::UnknownGroup(group.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_via_group() {
        let mut config = XetConfig::default();
        config.data.set("max_concurrent_file_ingestion", "16").unwrap();
        assert_eq!(config.data.max_concurrent_file_ingestion, 16);
        assert_eq!(config.data.get("max_concurrent_file_ingestion").unwrap(), "16");
    }

    #[test]
    fn test_set_with_integer() {
        let mut config = XetConfig::default();
        config.data.set("max_concurrent_file_ingestion", 32).unwrap();
        assert_eq!(config.data.max_concurrent_file_ingestion, 32);
    }

    #[test]
    fn test_set_and_get_via_dotted_path() {
        let mut config = XetConfig::default();
        config.set("data.max_concurrent_file_ingestion", "16").unwrap();
        assert_eq!(config.get("data.max_concurrent_file_ingestion").unwrap(), "16");
        assert_eq!(config.data.max_concurrent_file_ingestion, 16);
    }

    #[test]
    fn test_set_dotted_path_with_integer() {
        let mut config = XetConfig::default();
        config.set("data.max_concurrent_file_ingestion", 64).unwrap();
        assert_eq!(config.data.max_concurrent_file_ingestion, 64);
    }

    #[test]
    fn test_set_duration_field() {
        let mut config = XetConfig::default();
        config.set("data.progress_update_interval", "500ms").unwrap();
        assert_eq!(config.data.progress_update_interval, std::time::Duration::from_millis(500));
    }

    #[test]
    fn test_set_byte_size_field() {
        let mut config = XetConfig::default();
        config.set("data.ingestion_block_size", "16MB").unwrap();
        assert_eq!(config.data.ingestion_block_size.as_u64(), 16_000_000);
    }

    #[test]
    fn test_set_bool_field() {
        let mut config = XetConfig::default();
        config.set("client.enable_adaptive_concurrency", "false").unwrap();
        assert!(!config.client.enable_adaptive_concurrency);
    }

    #[test]
    fn test_set_string_field() {
        let mut config = XetConfig::default();
        config.set("data.default_cas_endpoint", "http://example.com:9090").unwrap();
        assert_eq!(config.data.default_cas_endpoint, "http://example.com:9090");
    }

    #[test]
    fn test_unknown_field_error() {
        let mut config = XetConfig::default();
        let result = config.set("data.nonexistent_field", "42");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::UnknownField(name) => assert_eq!(name, "nonexistent_field"),
            e => panic!("Expected UnknownField error, got: {e:?}"),
        }
    }

    #[test]
    fn test_unknown_group_error() {
        let mut config = XetConfig::default();
        let result = config.set("nonexistent_group.field", "42");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::UnknownGroup(name) => assert_eq!(name, "nonexistent_group"),
            e => panic!("Expected UnknownGroup error, got: {e:?}"),
        }
    }

    #[test]
    fn test_invalid_path_error() {
        let mut config = XetConfig::default();
        let result = config.set("no_dot_in_path", "42");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::InvalidPath(path) => assert_eq!(path, "no_dot_in_path"),
            e => panic!("Expected InvalidPath error, got: {e:?}"),
        }
    }

    #[test]
    fn test_parse_error() {
        let mut config = XetConfig::default();
        let result = config.set("data.max_concurrent_file_ingestion", "not_a_number");
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
    fn test_set_get_roundtrip_all_groups() {
        let mut config = XetConfig::default();

        config.set("shard.target_size", "2048").unwrap();
        assert_eq!(config.get("shard.target_size").unwrap(), "2048");

        config.set("deduplication.min_n_chunks_per_range", "4").unwrap();
        assert_eq!(config.get("deduplication.min_n_chunks_per_range").unwrap(), "4");

        config.set("chunk_cache.size_bytes", "5000000000").unwrap();
        assert_eq!(config.get("chunk_cache.size_bytes").unwrap(), "5000000000");

        config.set("client.retry_max_attempts", "10").unwrap();
        assert_eq!(config.get("client.retry_max_attempts").unwrap(), "10");

        config.set("reconstruction.target_block_completion_time", "30s").unwrap();
        assert_eq!(config.get("reconstruction.target_block_completion_time").unwrap(), "30s");

        config.set("xorb.compression_scheme_retest_interval", "64").unwrap();
        assert_eq!(config.get("xorb.compression_scheme_retest_interval").unwrap(), "64");
    }
}

#[cfg(feature = "python")]
pub mod py_xet_config {
    use pyo3::prelude::*;
    use pyo3::types::PyAnyMethods;

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

        pub fn inner_mut(&mut self) -> &mut XetConfig {
            &mut self.inner
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

        /// Set a configuration value by dotted path (e.g. "data.max_concurrent_file_ingestion").
        #[pyo3(name = "set")]
        fn py_set(&mut self, path: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
            let value_str: String = value.str()?.extract()?;
            self.inner
                .set(path, value_str)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
        }

        /// Get a configuration value's string representation by dotted path.
        #[pyo3(name = "get")]
        fn py_get(&self, path: &str) -> PyResult<String> {
            self.inner
                .get(path)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
        }

        #[getter]
        fn data(&self) -> groups::data::PyConfigValueGroup {
            self.inner.data.clone().into()
        }

        #[getter]
        fn shard(&self) -> groups::shard::PyConfigValueGroup {
            self.inner.shard.clone().into()
        }

        #[getter]
        fn deduplication(&self) -> groups::deduplication::PyConfigValueGroup {
            self.inner.deduplication.clone().into()
        }

        #[getter]
        fn chunk_cache(&self) -> groups::chunk_cache::PyConfigValueGroup {
            self.inner.chunk_cache.clone().into()
        }

        #[getter]
        fn client(&self) -> groups::client::PyConfigValueGroup {
            self.inner.client.clone().into()
        }

        #[getter]
        fn log(&self) -> groups::log::PyConfigValueGroup {
            self.inner.log.clone().into()
        }

        #[getter]
        fn reconstruction(&self) -> groups::reconstruction::PyConfigValueGroup {
            self.inner.reconstruction.clone().into()
        }

        #[getter]
        fn xorb(&self) -> groups::xorb::PyConfigValueGroup {
            self.inner.xorb.clone().into()
        }

        #[cfg(not(target_family = "wasm"))]
        #[getter]
        fn system_monitor(&self) -> groups::system_monitor::PyConfigValueGroup {
            self.inner.system_monitor.clone().into()
        }

        fn __repr__(&self) -> String {
            format!("XetConfig({:?})", self.inner)
        }

        fn __str__(&self) -> String {
            format!("{:?}", self.inner)
        }
    }
}
