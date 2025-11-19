crate::config_group!({
    /// How often should we retest the compression scheme?
    /// Determining the optimal compression scheme takes time, but
    /// it also minimizes the storage costs of the data.
    ///
    /// If set to zero, it's set once per file block per xorb.
    ///
    /// The default value is 32.
    ///
    /// Use the environment variable `HF_XET_XORB_COMPRESSION_SCHEME_RETEST_INTERVAL` to set this value.
    ref compression_scheme_retest_interval : usize = 32;
});
