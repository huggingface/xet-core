/// Target 1024 chunks per CAS block
pub const TARGET_CDC_CHUNK_SIZE: usize = 64 * 1024;

/// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
pub const MINIMUM_CHUNK_DIVISOR: usize = 8;

/// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
pub const MAXIMUM_CHUNK_MULTIPLIER: usize = 2;

/// The maximum number of bytes to go in a single xorb.
pub const MAX_XORB_BYTES: usize = 64 * 1024 * 1024;

/// The maximum number of chunks to go in a single xorb.  
/// Chunks are targeted at 64K, for ~1024 chunks per xorb, but
/// can be much higher when there are a lot of small files.
pub const MAX_XORB_CHUNKS: usize = 8 * 1024;
