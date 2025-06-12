utils::configurable_constants! {

    // Approximately 4 MB min spacing between global dedup queries.  Calculated by 4MB / TARGET_CHUNK_SIZE
    ref MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES: usize = 256;

    /// scheme for a local filesystem based CAS server
    ref LOCAL_CAS_SCHEME: String = "local://".to_owned();

    /// The current version
    ref CURRENT_VERSION: String = release_fixed( env!("CARGO_PKG_VERSION").to_owned());

    /// The expiration time of a local shard when first placed in the local shard cache.  Currently
    /// set to 3 weeks.
    ref MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS: u64 = 3 * 7 * 24 * 3600;

    /// The maximum number of simultaneous xorb upload streams.
    /// can be overwritten by environment variable "HF_XET_MAX_CONCURRENT_UPLOADS".
    /// The default value changes from 8 to 100 when "High Performance Mode" is enabled
    ref MAX_CONCURRENT_UPLOADS: usize = GlobalConfigMode::HighPerformanceOption {
        standard: 8,
        high_performance: 100,
    };

    /// The maximum number of files to ingest at once on the upload path
    ref MAX_CONCURRENT_FILE_INGESTION: usize =  GlobalConfigMode::HighPerformanceOption {
        standard: 8,
        high_performance: 100,
    };

    /// The maximum number of files to download at one time.
    ref MAX_CONCURRENT_DOWNLOADS : usize = GlobalConfigMode::HighPerformanceOption {
        standard: 8,
        high_performance: 100,
    };

    /// The maximum block size from a file to process at once.
    ref INGESTION_BLOCK_SIZE : usize = 8 * 1024 * 1024;

    /// How often to send updates on file progress, in milliseconds.  Disables batching
    /// if set to 0.
    ref PROGRESS_UPDATE_INTERVAL_MS : u64 = 200;

    /// How large of a time window to use for aggregating the progress speed results.
    ref PROGRESS_UPDATE_SPEED_SAMPLING_WINDOW_MS: u64 = 10 * 1000;


    /// How often do we flush new xorb data to disk on a long running upload session?
    ref SESSION_XORB_METADATA_FLUSH_INTERVAL_SECS : u64 = 20;

    /// Force a flush of the xorb metadata every this many xorbs, if more are created
    /// in this time window.
    ref SESSION_XORB_METADATA_FLUSH_MAX_COUNT : usize = 64;


}
