crate::config_group!({
    /// How often should we retest the compression scheme?
    /// Determining the optimal compression scheme takes time, but
    /// it also minimizes the storage costs of the data.
    ///
    /// If set to zero, it's set once per file block per xorb.
    ref compression_scheme_retest_interval : usize = 32;
});
