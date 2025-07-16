use utils::configurable_constants;

configurable_constants! {
    /// How often should we retest the compression scheme?
    /// Determining the optimal compression scheme takes time, but
    /// it also minimizes the storage costs of the data.
    ///
    /// If set to zero, it's set once per file block per xorb.
    ref CAS_OBJECT_COMPRESSION_SCHEME_RETEST_INTERVAL : usize = 32;

    /// Target 1024 chunks per CAS block
    ref IDEAL_CAS_BLOCK_SIZE: usize = release_fixed(64 * 1024 * 1024);
}
