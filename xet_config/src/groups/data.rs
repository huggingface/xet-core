use std::time::Duration;

crate::config_group!({

    // Approximately 4 MB min spacing between global dedup queries.  Calculated by 4MB / TARGET_CHUNK_SIZE
    ref min_spacing_between_global_dedup_queries: usize = 256;

    /// scheme for a local filesystem based CAS server
    ref local_cas_scheme: String = "local://".to_owned();

    /// The maximum number of simultaneous xorb upload streams.
    /// Can be overwritten by environment variable "HF_XET_DATA_MAX_CONCURRENT_UPLOADS".
    /// Default value: 8. High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 100 via XetConfig::with_high_performance().
    ref max_concurrent_uploads: usize = 8;

    /// The maximum number of files to ingest at once on the upload path.
    /// Can be overwritten by environment variable "HF_XET_DATA_MAX_CONCURRENT_FILE_INGESTION".
    /// Default value: 8. High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 100 via XetConfig::with_high_performance().
    ref max_concurrent_file_ingestion: usize = 8;

    /// The maximum number of files to download at one time.
    /// Can be overwritten by environment variable "HF_XET_DATA_MAX_CONCURRENT_DOWNLOADS".
    /// Default value: 8. High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 100 via XetConfig::with_high_performance().
    ref max_concurrent_downloads : usize = 8;

    /// The maximum block size from a file to process at once.
    ref ingestion_block_size : usize = 8 * 1024 * 1024;

    /// How often to send updates on file progress, in milliseconds.  Disables batching
    /// if set to 0.
    ref progress_update_interval : Duration = Duration::from_millis(200);

    /// How large of a time window to use for aggregating the progress speed results.
    ref progress_update_speed_sampling_window: Duration = Duration::from_millis(10 * 1000);

    /// How often do we flush new xorb data to disk on a long running upload session?
    ref session_xorb_metadata_flush_interval : Duration = Duration::from_secs(20);

    /// Force a flush of the xorb metadata every this many xorbs, if more are created
    /// in this time window.
    ref session_xorb_metadata_flush_max_count : usize = 64;

    /// Default CAS endpoint
    ref default_cas_endpoint: String = "http://localhost:8080".to_string();

});
