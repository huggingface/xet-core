use std::time::Duration;

use utils::ByteSize;

crate::config_group!({

    /// Gives the minimum spacing in number of chunks between global dedup queries
    /// sent to the server to limit the number of simultaneous queries.
    ///
    /// The default value is 256, which means that the server will receive a query at most
    /// for every 256 chunks or 4MB of data.
    ///
    /// Use the environment variable `HF_XET_DATA_MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES` to set this value.
    ref min_spacing_between_global_dedup_queries: usize = 256;

    /// scheme for a local filesystem based CAS server
    ///
    /// The default value is "local://".
    ///
    /// Use the environment variable `HF_XET_DATA_LOCAL_CAS_SCHEME` to set this value.
    ref local_cas_scheme: String = "local://".to_owned();

    /// The maximum number of files to ingest at once on the upload path.
    /// High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 100 via XetConfig::with_high_performance().
    ///
    /// The default value is 8.
    ///
    /// Use the environment variable `HF_XET_DATA_MAX_CONCURRENT_FILE_INGESTION` to set this value.
    ref max_concurrent_file_ingestion: usize = 8;

    /// The maximum number of files to ingest at once on the download path.
    /// High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 100 via XetConfig::with_high_performance().
    ///
    /// The default value is 8.
    ///
    /// Use the environment variable `HF_XET_DATA_MAX_CONCURRENT_FILE_DOWNLOADS` to set this value.
    ref max_concurrent_file_downloads: usize = 8;

    /// The maximum block size from a file to process at once.
    ///
    /// The default value is 8mb.
    ///
    /// Use the environment variable `HF_XET_DATA_INGESTION_BLOCK_SIZE` to set this value.
    ref ingestion_block_size : ByteSize = ByteSize::from("8mb");

    /// How often to send updates on file progress, in milliseconds.  Disables batching
    /// if set to 0.
    ///
    /// The default value is 200ms.
    ///
    /// Use the environment variable `HF_XET_DATA_PROGRESS_UPDATE_INTERVAL` to set this value.
    ref progress_update_interval : Duration = Duration::from_millis(200);

    /// How large of a time window to use for aggregating the progress speed results.
    ///
    /// The default value is 10sec.
    ///
    /// Use the environment variable `HF_XET_DATA_PROGRESS_UPDATE_SPEED_SAMPLING_WINDOW` to set this value.
    ref progress_update_speed_sampling_window: Duration = Duration::from_secs(10);

    /// How often do we flush new xorb data to disk on a long running upload session?
    ///
    /// The default value is 20sec.
    ///
    /// Use the environment variable `HF_XET_DATA_SESSION_XORB_METADATA_FLUSH_INTERVAL` to set this value.
    ref session_xorb_metadata_flush_interval : Duration = Duration::from_secs(20);

    /// Force a flush of the xorb metadata every this many xorbs, if more are created
    /// in this time window.
    ///
    /// The default value is 64.
    ///
    /// Use the environment variable `HF_XET_DATA_SESSION_XORB_METADATA_FLUSH_MAX_COUNT` to set this value.
    ref session_xorb_metadata_flush_max_count : usize = 64;

    /// Default CAS endpoint
    ///
    /// The default value is "http://localhost:8080".
    ///
    /// Use the environment variable `HF_XET_DATA_DEFAULT_CAS_ENDPOINT` to set this value.
    ref default_cas_endpoint: String = "http://localhost:8080".to_string();

    /// Whether to aggregate progress updates before sending them.
    /// When enabled, progress updates are batched and sent at regular intervals
    /// to reduce overhead.
    ///
    /// The default value is true.
    ///
    /// Use the environment variable `HF_XET_DATA_AGGREGATE_PROGRESS` to set this value.
    ref aggregate_progress: bool = true;

    /// Default prefix used for CAS and shard operations.
    ///
    /// The default value is "default".
    ///
    /// Use the environment variable `HF_XET_DATA_DEFAULT_PREFIX` to set this value.
    ref default_prefix: String = "default".to_string();

    /// Subdirectory name for staging data within the endpoint cache directory.
    ///
    /// The default value is "staging".
    ///
    /// Use the environment variable `HF_XET_DATA_STAGING_SUBDIR` to set this value.
    ref staging_subdir: String = "staging".to_string();

});
