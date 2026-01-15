use utils::ByteSize;

use crate::groups;

/// Primary configuration struct containing all config sections
#[derive(Debug, Clone, Default)]
pub struct XetConfig {
    pub data: groups::data::ConfigValues,
    pub mdb_shard: groups::mdb_shard::ConfigValues,
    pub deduplication: groups::deduplication::ConfigValues,
    pub chunk_cache: groups::chunk_cache::ConfigValues,
    pub client: groups::client::ConfigValues,
    pub log: groups::log::ConfigValues,
    pub reconstruction: groups::reconstruction::ConfigValues,
    pub xorb: groups::xorb::ConfigValues,
}

impl XetConfig {
    /// Create a new XetConfig instance with default values and apply environment variable overrides.
    /// If high performance mode is enabled (via environment variables HF_XET_HIGH_PERFORMANCE or HF_XET_HP),
    /// also applies high performance settings automatically.
    /// This is equivalent to `XetConfig::default().with_env_overrides()`, with `with_high_performance()`
    /// called conditionally if high performance mode is enabled.
    pub fn new() -> Self {
        let mut config = Self::default().with_env_overrides();
        if utils::is_high_performance() {
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
        self.mdb_shard.apply_env_overrides();
        self.deduplication.apply_env_overrides();
        self.chunk_cache.apply_env_overrides();
        self.client.apply_env_overrides();
        self.log.apply_env_overrides();
        self.reconstruction.apply_env_overrides();
        self.xorb.apply_env_overrides();
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
    /// - data.max_concurrent_uploads: 8 -> 100
    /// - data.max_concurrent_file_ingestion: 8 -> 100
    /// - data.max_concurrent_downloads: 8 -> 100
    /// - cas_client.num_range_in_segment_base: 16 -> 128
    ///
    /// Note: This method is automatically called by `XetConfig::new()` if high performance mode is enabled.
    pub fn with_high_performance(mut self) -> Self {
        self.client.fixed_concurrency_max_uploads = 100;
        self.data.max_concurrent_file_ingestion = 100;
        self.client.fixed_concurrency_max_downloads = 100;
        self.client.num_range_in_segment_base = 128;

        // Adjustments to the adaptive concurrency control.
        self.client.ac_max_upload_concurrency = 124;
        self.client.ac_max_download_concurrency = 124;
        self.client.ac_min_upload_concurrency = 4;
        self.client.ac_min_download_concurrency = 4;
        self.client.ac_initial_upload_concurrency = 16;
        self.client.ac_initial_download_concurrency = 16;

        self.reconstruction.min_reconstruction_fetch_size = ByteSize::from("1gb");
        self.reconstruction.max_reconstruction_fetch_size = ByteSize::from("16gb");
        self.reconstruction.download_buffer_size = ByteSize::from("64gb");

        self
    }
}
