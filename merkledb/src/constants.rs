pub const MEAN_TREE_BRANCHING_FACTOR: u64 = 4;
pub const TARGET_CAS_BLOCK_SIZE: usize = 15 * 1024 * 1024;
pub const IDEAL_CAS_BLOCK_SIZE: usize = 16 * 1024 * 1024;
pub const TARGET_CDC_CHUNK_SIZE: usize = 16384;
pub const N_LOW_VARIANCE_CDC_CHUNKERS: usize = 8;

/// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
pub const MINIMUM_CHUNK_DIVISOR: usize = 4;
/// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
pub const MAXIMUM_CHUNK_MULTIPLIER: usize = 8;
