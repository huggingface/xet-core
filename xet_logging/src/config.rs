use std::path::{Path, PathBuf};
use std::time::Duration;

use utils::normalized_path_from_user_string;
use xet_runtime::xet_config;

#[derive(Clone, Debug, PartialEq)]
pub enum LoggingMode {
    Directory(PathBuf),
    File(PathBuf),
    Console,
}

/// The log directory cleanup configuration.  By default, the values
/// are loaded from environment variables.

#[derive(Clone, Debug, PartialEq)]
pub struct LogDirConfig {
    pub min_deletion_age: Duration,
    pub max_retention_age: Duration,
    pub size_limit: u64,
    pub filename_prefix: String,
}

impl Default for LogDirConfig {
    fn default() -> Self {
        // Load the defaults from the environmental config.
        Self {
            min_deletion_age: xet_config().log.dir_min_deletion_age,
            max_retention_age: xet_config().log.dir_max_retention_age,
            size_limit: xet_config().log.dir_max_size.as_u64(),
            filename_prefix: xet_config().log.prefix.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LoggingConfig {
    pub logging_mode: LoggingMode,
    pub use_json: bool,
    pub enable_log_dir_cleanup: bool,
    pub version: String,
    pub log_dir_config: LogDirConfig,
}

impl LoggingConfig {
    /// Set up logging to a directory.  Note that this can be overwritten by environmental
    pub fn default_to_directory(version: String, log_directory: impl AsRef<Path>) -> LoggingConfig {
        // Choose the logging mode.
        let logging_mode = {
            if let Some(log_dest) = &xet_config().log.dest {
                if log_dest.as_str().is_empty() {
                    LoggingMode::Console
                } else {
                    let path = normalized_path_from_user_string(log_dest);

                    if log_dest.ends_with('/')
                        || (cfg!(windows) && log_dest.ends_with('\\'))
                        || (path.exists() && path.is_dir())
                    {
                        LoggingMode::Directory(path)
                    } else {
                        LoggingMode::File(path)
                    }
                }
            } else {
                LoggingMode::Directory(log_directory.as_ref().to_path_buf())
            }
        };

        let use_json = {
            if let Some(format) = &xet_config().log.format {
                format.as_str().to_ascii_lowercase().trim() == "json"
            } else {
                logging_mode != LoggingMode::Console
            }
        };

        let enable_log_dir_cleanup =
            matches!(logging_mode, LoggingMode::Directory(_)) && !xet_config().log.dir_disable_cleanup;

        Self {
            logging_mode,
            use_json,
            enable_log_dir_cleanup,
            version,
            log_dir_config: LogDirConfig::default(),
        }
    }
}
