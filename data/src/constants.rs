use lazy_static::lazy_static;

lazy_static! {
    // The xet library version.
    pub static ref XET_VERSION: String =
        std::env::var("XET_VERSION").unwrap_or_else(|_| CURRENT_VERSION.to_string());
    /// The maximum number of simultaneous download streams
    pub static ref MAX_CONCURRENT_DOWNLOADS: usize = std::env::var("XET_CONCURRENT_DOWNLOADS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    /// The maximum number of simultaneous upload streams
    pub static ref MAX_CONCURRENT_UPLOADS: usize = std::env::var("XET_CONCURRENT_UPLOADS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
}

/// The maximum git filter protocol packet size
pub const GIT_MAX_PACKET_SIZE: usize = 65516;

/// We put a limit on the pointer file size so that
/// we don't ever try to read a whole giant blob into memory when
/// trying to clean or smudge.
/// See gitxetcore::data::pointer_file for the explanation for this limit.
pub const POINTER_FILE_LIMIT: usize = 150;

/// If a file has size smaller than this threshold, AND if it "looks-like"
/// text, we interpret this as a text file and passthrough the file, letting
/// git handle it. See `small_file_determination.rs` for details.
///
/// We set this to be 1 less than a constant multiple of the GIT_MAX_PACKET_SIZE
/// so we can read exactly up to that multiple of packets to determine if it
/// is a small file.
pub const SMALL_FILE_THRESHOLD: usize = 4 * GIT_MAX_PACKET_SIZE - 1;

// Salt is 256-bit in length.
pub const REPO_SALT_LEN: usize = 32;

// Approximately 4 MB min spacing between global dedup queries.  Calculated by 4MB / TARGET_CHUNK_SIZE
pub const MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES: usize = 256;

/// scheme for a local filesystem based CAS server
pub const LOCAL_CAS_SCHEME: &str = "local://";

/// The current version
pub const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maximum number of entries in the file construction cache
/// which stores File Hash -> reconstruction instructions
pub const FILE_RECONSTRUCTION_CACHE_SIZE: usize = 65536;
