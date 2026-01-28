use std::time::Duration;

crate::config_group!({
    /// The interval at which to sample system statistics.
    ///
    /// The default value is 5 seconds.
    ///
    /// Use the environment variable `HF_XET_SYSTEM_MONITOR_SAMPLE_INTERVAL` to set this value.
    ref sample_interval: Duration = Duration::from_secs(5);

    /// The path to write the system monitor output to.
    /// If not set, the output will be written to tracing log at "Error" level.
    /// If the pattern "#PID#" is present in the given value, it will be replaced
    /// by the id of the current process.
    ///
    /// The default value is None.
    ///
    /// Use the environment variable `HF_XET_SYSTEM_MONITOR_OUTPUT_PATH` to set this value.
    ref output_path: Option<String> = None;
});
