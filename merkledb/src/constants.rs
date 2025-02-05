pub const MEAN_TREE_BRANCHING_FACTOR: u64 = 4;
pub const N_LOW_VARIANCE_CDC_CHUNKERS: usize = 8;

/// Target 1024 chunks per CAS block
pub const TARGET_CDC_CHUNK_SIZE: usize = 64 * 1024;
pub const IDEAL_CAS_BLOCK_SIZE: usize = 64 * 1024 * 1024;

/// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
pub const MINIMUM_CHUNK_DIVISOR: usize = 8;
/// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
pub const MAXIMUM_CHUNK_MULTIPLIER: usize = 2;

/// no chunk may be larger than MAXIMUM_CHUNK_SIZE bytes
pub const MAXIMUM_CHUNK_SIZE: usize = TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER;

/// Produce a CAS block when accumulated chunks exceeds TARGET_CAS_BLOCK_SIZE,
/// this ensures that block sizes are always less than IDEAL_CAS_BLOCK_SIZE.
pub const TARGET_CAS_BLOCK_SIZE: usize = IDEAL_CAS_BLOCK_SIZE - TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER;
