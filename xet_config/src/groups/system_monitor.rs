use std::time::Duration;

use utils::TemplatedPathBuf;

crate::config_group!({
    /// Whether to enable system resource monitoring.
    ///
    /// When enabled, the system monitor will periodically sample and log system statistics
    /// such as CPU usage, memory usage, and other resource metrics.
    ///
    /// The default value is false.
    ///
    /// Use the environment variable `HF_XET_SYSTEM_MONITOR_ENABLED` to set this value.
    ref enabled: bool = false;

    /// The interval at which to sample system statistics.
    ///
    /// The default value is 5 seconds.
    ///
    /// Use the environment variable `HF_XET_SYSTEM_MONITOR_SAMPLE_INTERVAL` to set this value.
    ref sample_interval: Duration = Duration::from_secs(5);

    /// The path to write the system monitor output to.
    ///
    /// If not set, the output will be written to tracing log at "INFO" level.
    ///
    /// Supports template variables (case-insensitive):
    /// - `{PID}` - Replaced with the current process ID
    /// - `{TIMESTAMP}` - Replaced with ISO 8601 timestamp in local timezone with offset
    ///   (e.g., `2024-02-05T14-30-45-0500`)
    ///
    /// Example: `~/logs/monitor_{PID}_{TIMESTAMP}.log`
    ///
    /// The default value is None.
    ///
    /// Use the environment variable `HF_XET_SYSTEM_MONITOR_LOG_PATH` to set this value.
    ref log_path: Option<TemplatedPathBuf> = None;
});
