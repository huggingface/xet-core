use std::time::Duration;

use utils::ByteSize;

crate::config_group!({

    /// The log destination.  By default, logs to the logs/ subdirectory in the huggingface xet cache directory.
    ///
    /// If this path exists as a directory or the path ends with a /, then logs will be dumped into to that directory.
    /// Dy default, logs older than LOG_DIR_MAX_RETENTION_AGE in the directory are deleted, and old logs are deleted to
    /// keep the total size of files present below LOG_DIR_MAX_SIZE.
    ///
    /// If LOG_DEST is given but empty, then logs are dumped to the console.
    ///
    /// The default value is None.
    ///
    /// Use the environment variable `HF_XET_LOG_DEST` to set this value.
    ref dest : Option<String> = None;

    /// The format the logs are printed in. If "json", then logs are dumped as json blobs; otherwise they
    /// are treated as text.  By default logging to files is done in json and console logging is done with text.
    ///
    /// The default value is None.
    ///
    /// Use the environment variable `HF_XET_LOG_FORMAT` to set this value.
    ref format : Option<String> = None;

    /// The base name for a log file when logging to a directory.  The timestamp and pid are appended to this name to form the log
    /// file.
    ///
    /// The default value is "xet".
    ///
    /// Use the environment variable `HF_XET_LOG_PREFIX` to set this value.
    ref prefix : String = "xet".to_string();

    /// If given, disable cleaning up old files in the log directory.
    ///
    /// The default value is false.
    ///
    /// Use the environment variable `HF_XET_LOG_DIR_DISABLE_CLEANUP` to set this value.
    ref dir_disable_cleanup : bool = false;

    /// If given, prune old log files in the directory to keep the directory size under this many bytes.
    ///
    /// Note that the directory may exceed this size as pruning is done only on files without an associated active process
    /// and older than LOG_DIR_MIN_DELETION_AGE.
    ///
    /// The default value is 250mb.
    ///
    /// Use the environment variable `HF_XET_LOG_DIR_MAX_SIZE` to set this value.
    ref dir_max_size: ByteSize = ByteSize::from("250mb");

    /// Do not delete any files younger than this.
    ///
    /// The default value is 1day.
    ///
    /// Use the environment variable `HF_XET_LOG_DIR_MIN_DELETION_AGE` to set this value.
    ref dir_min_deletion_age: Duration = Duration::from_secs(24 * 3600); // 1 day

    /// Delete all files older than this.
    ///
    /// The default value is 14day.
    ///
    /// Use the environment variable `HF_XET_LOG_DIR_MAX_RETENTION_AGE` to set this value.
    ref dir_max_retention_age: Duration = Duration::from_secs(14 * 24 * 3600); // 2 weeks

});
