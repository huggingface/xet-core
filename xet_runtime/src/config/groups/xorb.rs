use crate::utils::{ByteSize, ConfigEnum};

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
    /// The default value is "auto" (auto-detect).
    ///
    /// Use the environment variable `HF_XET_XORB_COMPRESSION_POLICY` to set this value.
    ref compression_policy: ConfigEnum = ConfigEnum::new("auto", &["", "auto", "none", "lz4", "bg4-lz4"]);

    /// Override the maximum xorb size in bytes for simulation mode.
    /// When set to Some(value), this overrides the hard-coded MAX_XORB_BYTES
    /// cutting threshold in simulation builds. When None (default), the
    /// standard constant is used.
    ///
    /// Only effective when the `simulation` feature is enabled.
    ///
    /// Use the environment variable `HF_XET_XORB_SIMULATION_MAX_BYTES` to set this value.
    ref simulation_max_bytes: Option<ByteSize> = None;

    /// Override the maximum xorb chunk count for simulation mode.
    /// When set to Some(value), this overrides the hard-coded MAX_XORB_CHUNKS
    /// cutting threshold in simulation builds. When None (default), the
    /// standard constant is used.
    ///
    /// Only effective when the `simulation` feature is enabled.
    ///
    /// Use the environment variable `HF_XET_XORB_SIMULATION_MAX_CHUNKS` to set this value.
    ref simulation_max_chunks: Option<usize> = None;
});
