utils::configurable_constants! {


    // Approximately 4 MB min spacing between global dedup queries.  Calculated by 4MB / TARGET_CHUNK_SIZE
    ref MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES: usize = 256;

    /// scheme for a local filesystem based CAS server
    ref LOCAL_CAS_SCHEME: String = "local://".to_owned();

    /// The current version
    ref CURRENT_VERSION: String = release_fixed( env!("CARGO_PKG_VERSION").to_owned());

    /// Number of ranges to use when estimating fragmentation
    ref NRANGES_IN_STREAMING_FRAGMENTATION_ESTIMATOR: usize = 128;

    /// Minimum number of chunks per range. Used to control fragmentation
    /// This targets an average of 1MB per range.
    /// The hysteresis factor multiplied by the target Chunks Per Range (CPR) controls
    /// the low end of the hysteresis range. Basically, dedupe will stop
    /// when CPR drops below hysteresis * target_cpr, and will start again when
    /// CPR increases above target CPR.
    ref MIN_N_CHUNKS_PER_RANGE_HYSTERESIS_FACTOR: f32 = 0.5;

    ref DEFAULT_MIN_N_CHUNKS_PER_RANGE: f32 = 8.0;

    /// The expiration time of a local shard when first placed in the local shard cache.  Currently
    /// set to 3 weeks.
    ref MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS: u64 = 3 * 7 * 24 * 3600;

    /// The maximum number of simultaneous xorb upload streams.
    /// The default value is 8 and can be overwritten by environment variable "XET_CONCURRENT_XORB_UPLOADS".
    ref MAX_CONCURRENT_XORB_UPLOADS: usize = 8;

    /// The amount of data to process at once while chunking through files and incoming data
    ref DATA_INGESTION_BUFFER_SIZE : usize = 16 * 1024 * 1024;

    /// This is the target memory usage of chunks within an upload session.  This should be enough to
    /// ensure that buffers are filled while uploading new xorbs, assuming that up to MAX_XORB_BYTES
    /// are tied up in an intermediate xorb.
    ///
    /// As soon as new xorbs get queued in the
    /// MAX_CONCURRENT_XORB_UPLOADS path, the memory is put to that process and those chunks are released.
    ///
    /// This is tracked on a per-session basis to avoid a potential deadlock where all chunk permits
    /// are tied up in a multiplicity of intermediate xorb buffers.
    ref CHUNK_MEMORY_USAGE_PER_UPLOAD_SESSION: usize = 512 * 1024 * 1024;

}
