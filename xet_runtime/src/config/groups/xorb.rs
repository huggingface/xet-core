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

    /// Compression policy for xorb data.
    /// Valid values: "" or "auto" for automatic detection, "none", "lz4", "bg4-lz4".
    /// When set to "" or "auto", the best compression scheme is chosen based on data analysis.
    ///
    /// Ideally this would be typed as `Option<CompressionScheme>` for early validation,
    /// but `CompressionScheme` lives in `xet_core_structures` which depends on `xet_runtime`,
    /// creating a circular dependency. Validation happens at use time via
    /// `CompressionScheme::from_policy_str()`.
    ///
    /// The default value is "auto" (auto-detect).
    ///
    /// Use the environment variable `HF_XET_XORB_COMPRESSION_POLICY` to set this value.
    ref compression_policy: String = "auto".to_string();
});
