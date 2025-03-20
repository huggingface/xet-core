utils::configurable_constants! {

    /// This will target 1024 chunks per Xorb / CAS block
    ref TARGET_CHUNK_SIZE: usize = release_fixed(64 * 1024);

    /// TARGET_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
    ref MINIMUM_CHUNK_DIVISOR: usize = release_fixed(8);

    /// TARGET_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
    ref MAXIMUM_CHUNK_MULTIPLIER: usize = release_fixed(2);
}

lazy_static::lazy_static! {
    static ref MAX_CHUNK_SIZE : usize = (*TARGET_CHUNK_SIZE) * (*MAXIMUM_CHUNK_MULTIPLIER);
    static ref MIN_CHUNK_SIZE : usize = (*TARGET_CHUNK_SIZE) / (*MINIMUM_CHUNK_DIVISOR);
}
