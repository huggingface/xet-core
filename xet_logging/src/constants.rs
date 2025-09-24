use std::time::Duration;

use utils::ByteSize;

utils::configurable_constants! {

    /// The log destination.  By default, logs to the logs/ subdirectory in the huggingface xet cache directory.
    ///
    /// If this path exists as a directory or the path ends with a /, then logs will be dumped into to that directory.
    /// Dy default, logs older than LOG_DIR_MAX_RETENTION_AGE in the directory are deleted, and old logs are deleted to
    /// keep the total size of files present below LOG_DIR_MAX_SIZE.
    ///
    /// If LOG_DEST is given but empty, then logs are dumped to the console.
    ref LOG_DEST : Option<String> = None;

    /// The format the logs are printed in. If "json", then logs are dumped as json blobs; otherwise they
    /// are treated as text.  By default logging to files is done in json and console logging is done with text.
    ref LOG_FORMAT : Option<String> = None;

    /// The base name for a log file when logging to a directory.  The timestamp and pid are appended to this name to form the log
    /// file.
    ref LOG_PREFIX : String = "xet";

    /// If given, disable cleaning up old files in the log directory.
    ref LOG_DIR_DISABLE_CLEANUP : bool = false;

    /// If given, prune old log files in the directory to keep the directory size under this many bytes.
    ///
    /// Note that the directory may exceed this size as pruning is done only on files without an associated active process
    /// and older than LOG_DIR_MIN_DELETION_AGE.
    ref LOG_DIR_MAX_SIZE: ByteSize = ByteSize::from("250mb");

    /// Do not delete any files younger than this.
    ref LOG_DIR_MIN_DELETION_AGE: Duration = Duration::from_secs(24 * 3600); // 1 day

    /// Delete all files older than this.
    ref LOG_DIR_MAX_RETENTION_AGE: Duration = Duration::from_secs(14 * 24 * 3600); // 2 weeks

}

/// Default log level for the library to use. Override using the `RUST_LOG` env variable.
pub(crate) const DEFAULT_LOG_LEVEL_FILE: &str = "info";
pub(crate) const DEFAULT_LOG_LEVEL_CONSOLE: &str = "warn";
