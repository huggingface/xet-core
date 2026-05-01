use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::XetConfig;
use crate::utils::TemplatedPathBuf;

#[derive(Clone, Debug, PartialEq)]
pub enum LoggingMode {
    Directory(PathBuf),
    File(PathBuf),
    Console,
}

/// The log directory cleanup configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct LogDirConfig {
    pub min_deletion_age: Duration,
    pub max_retention_age: Duration,
    pub size_limit: u64,
    pub filename_prefix: String,
}

impl LogDirConfig {
    pub fn from_config(config: &XetConfig) -> Self {
        Self {
            min_deletion_age: config.log.dir_min_deletion_age,
            max_retention_age: config.log.dir_max_retention_age,
            size_limit: config.log.dir_max_size.as_u64(),
            filename_prefix: config.log.prefix.to_string(),
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
    /// Set up logging to a directory using the given config.
    pub fn from_directory(config: &XetConfig, version: String, log_directory: impl AsRef<Path>) -> LoggingConfig {
        let logging_mode = {
            if let Some(log_dest) = &config.log.dest {
                if log_dest.as_str().is_empty() {
                    LoggingMode::Console
                } else {
                    let path = TemplatedPathBuf::evaluate(log_dest);

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
            if let Some(format) = &config.log.format {
                format.as_str().to_ascii_lowercase().trim() == "json"
            } else {
                logging_mode != LoggingMode::Console
            }
        };

        let enable_log_dir_cleanup =
            matches!(logging_mode, LoggingMode::Directory(_)) && !config.log.dir_disable_cleanup;

        Self {
            logging_mode,
            use_json,
            enable_log_dir_cleanup,
            version,
            log_dir_config: LogDirConfig::from_config(config),
        }
    }
}
