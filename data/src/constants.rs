use lazy_static::lazy_static;

lazy_static! {
    /// The xet library version.
    pub static ref XET_VERSION: String =
        std::env::var("XET_VERSION").unwrap_or_else(|_| CURRENT_VERSION.to_string());
    /// The maximum number of simultaneous xorb upload streams.
    /// The default value is 8 and can be overwritten by environment variable "XET_CONCURRENT_XORB_UPLOADS".
    pub static ref MAX_CONCURRENT_XORB_UPLOADS: usize = std::env::var("XET_CONCURRENT_XORB_UPLOADS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
}

/// The maximum git filter protocol packet size
pub const GIT_MAX_PACKET_SIZE: usize = 65516;

/// We put a limit on the pointer file size so that
/// we don't ever try to read a whole giant blob into memory when
/// trying to clean or smudge.
/// See gitxetcore::data::pointer_file for the explanation for this limit.
pub const POINTER_FILE_LIMIT: usize = 150;

// Salt is 256-bit in length.
pub const REPO_SALT_LEN: usize = 32;

// Approximately 4 MB min spacing between global dedup queries.  Calculated by 4MB / TARGET_CHUNK_SIZE
pub const MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES: usize = 256;

/// scheme for a local filesystem based CAS server
pub const LOCAL_CAS_SCHEME: &str = "local://";

/// The current version
pub const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Number of ranges to use when estimating fragmentation
pub const NRANGES_IN_STREAMING_FRAGMENTATION_ESTIMATOR: usize = 128;

/// Minimum number of chunks per range. Used to control fragmentation
/// This targets an average of 1MB per range.
/// The hysteresis factor multiplied by the target Chunks Per Range (CPR) controls
/// the low end of the hysteresis range. Basically, dedupe will stop
/// when CPR drops below hysteresis * target_cpr, and will start again when
/// CPR increases above target CPR.
pub const MIN_N_CHUNKS_PER_RANGE_HYSTERESIS_FACTOR: f32 = 0.5;
pub const DEFAULT_MIN_N_CHUNKS_PER_RANGE: f32 = 8.0;
