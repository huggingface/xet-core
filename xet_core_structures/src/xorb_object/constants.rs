xet_runtime::test_configurable_constants! {
    /// This will target 1024 chunks per Xorb / CAS block
    ref TARGET_CHUNK_SIZE: usize = 64 * 1024;

    /// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
    /// Note that this is not a threshold but a recommendation.
    /// Smaller chunks can be produced if size of a file is smaller than this number.
    ref MINIMUM_CHUNK_DIVISOR: usize = 8;

    /// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
    /// Note that this is a limit.
    ref MAXIMUM_CHUNK_MULTIPLIER: usize = 2;

    /// The maximum number of bytes to go in a single xorb.
    ref MAX_XORB_BYTES: usize = 64 * 1024 * 1024;

    /// The maximum number of chunks to go in a single xorb.
    /// Chunks are targeted at 64K, for ~1024 chunks per xorb, but
    /// can be much higher when there are a lot of small files.
    ref MAX_XORB_CHUNKS: usize = 8 * 1024;

    /// Target 1024 chunks per XORB block
    ref XORB_BLOCK_SIZE: usize = 64 * 1024 * 1024;
}

lazy_static::lazy_static! {
    /// The maximum chunk size, calculated from the configurable constants above
    pub static ref MAX_CHUNK_SIZE: usize = (*TARGET_CHUNK_SIZE) * (*MAXIMUM_CHUNK_MULTIPLIER);
}

/// Given a list of chunk boundaries in a file and an arbitrary reference position,
/// returns the next stable chunk boundary at or after that position.
///
/// `starting_position` may be any byte offset in the file; it does not need to
/// be an existing chunk boundary. The search starts at the first chunk boundary
/// `>= starting_position`.
///
/// A stable chunk boundary is defined such that any possible changes in the data
/// before `starting_position` would produce the same chunk boundaries at the
/// stable boundary and later. The fixed data between `starting_position` and
/// the returned stable boundary is always sufficient to restore the chunker to
/// its original chunk boundaries.
///
/// The stability condition requires two consecutive chunks after `starting_position`,
/// both with sizes in `[2 * min_chunk, max_chunk - min_chunk)`. The boundary
/// at the end of the second such chunk is the stable chunk boundary.
///
/// The lower bound is `2 * min_chunk` rather than `min_chunk` (as used in
/// `find_partitions` in the chunking module) because this function operates on
/// existing chunk boundaries without data access, and cannot verify the absence
/// of hidden hash triggers in the `[c_k, c_k + min_chunk)` skip zone. A
/// shadow-zone trigger can advance a modified chunker by up to `min_chunk`, so
/// the next chunk must be at least `2 * min_chunk` to remain reachable.
///
/// See `parallel chunking.lyx` for the full proof and `find_stable_start` in
/// `merkle_hash_subtree.rs` for the analogous construction in merkle hashing.
pub fn next_stable_chunk_boundary(starting_position: usize, chunk_boundaries: &[usize]) -> Option<usize> {
    let start_idx = chunk_boundaries.partition_point(|&x| x < starting_position);

    for i in start_idx..chunk_boundaries.len().saturating_sub(2) {
        let size_a = chunk_boundaries[i + 1] - chunk_boundaries[i];
        let size_b = chunk_boundaries[i + 2] - chunk_boundaries[i + 1];

        if is_stable_chunk_size(size_a) && is_stable_chunk_size(size_b) {
            return Some(chunk_boundaries[i + 2]);
        }
    }
    None
}

/// True when a chunk's size satisfies the CDC stability condition: `[2 * min_chunk,
/// max_chunk - min_chunk)`. Two consecutive chunks both meeting this condition mark a
/// stable boundary at the end of the second chunk (see [`next_stable_chunk_boundary`]).
pub fn is_stable_chunk_size(size: usize) -> bool {
    let minimum_chunk = *TARGET_CHUNK_SIZE / *MINIMUM_CHUNK_DIVISOR;
    let maximum_chunk = *TARGET_CHUNK_SIZE * *MAXIMUM_CHUNK_MULTIPLIER;
    size >= 2 * minimum_chunk && size < maximum_chunk - minimum_chunk
}
