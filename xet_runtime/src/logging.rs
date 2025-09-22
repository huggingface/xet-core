use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, Local, Utc};
use sysinfo::{Pid, ProcessRefreshKind, RefreshKind, System};
use tracing::{debug, error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use utils::{ByteSize, normalized_path_from_user_string};

use crate::xet_cache_root;

utils::configurable_constants! {

    /// The log destination.  By default, logs to the logs/ subdirectory in the huggingface cache directory.
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
    ref LOG_DIR_MAX_SIZE: ByteSize = ByteSize::from("1gb");

    /// Do not delete any files younger than this.
    ref LOG_DIR_MIN_DELETION_AGE: Duration = Duration::from_secs(24 * 3600); // 1 day

    /// Delete all files older than this.
    ref LOG_DIR_MAX_RETENTION_AGE: Duration = Duration::from_secs(14 * 24 * 3600); // 2 weeks

}

/// Default log level for the library to use. Override using the `RUST_LOG` env variable.
const DEFAULT_LOG_LEVEL_FILE: &str = "info";
const DEFAULT_LOG_LEVEL_CONSOLE: &str = "warn";

#[derive(Clone, Debug, PartialEq)]
pub enum LoggingMode {
    Directory(PathBuf),
    File(PathBuf),
    Console,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LoggingConfig {
    pub logging_mode: LoggingMode,
    pub use_json: bool,
    pub enable_log_dir_cleanup: bool,
    pub version: String,
}

impl LoggingConfig {
    pub fn new(version: String) -> LoggingConfig {
        // Choose the logging mode.
        let logging_mode = {
            if let Some(log_dest) = &*LOG_DEST {
                if log_dest.is_empty() {
                    LoggingMode::Console
                } else {
                    let path = normalized_path_from_user_string(log_dest);

                    if log_dest.ends_with('/') || log_dest.ends_with('\\') || (path.exists() && path.is_dir()) {
                        LoggingMode::Directory(path)
                    } else {
                        LoggingMode::File(path)
                    }
                }
            } else {
                let default_logging_directory = xet_cache_root().join("logs");
                LoggingMode::Directory(default_logging_directory)
            }
        };

        let use_json = {
            if let Some(format) = &*LOG_FORMAT {
                format.to_ascii_lowercase().trim() == "json"
            } else {
                logging_mode != LoggingMode::Console
            }
        };

        let enable_log_dir_cleanup = matches!(logging_mode, LoggingMode::Directory(_)) && !*LOG_DIR_DISABLE_CLEANUP;

        Self {
            logging_mode,
            use_json,
            enable_log_dir_cleanup,
            version,
        }
    }
}

/// The main entry point to set up logging.  Should only be called once.
pub fn init_logging(cfg: LoggingConfig) {
    let maybe_log_file: Option<PathBuf> = {
        match &cfg.logging_mode {
            LoggingMode::Directory(log_dir) => {
                if cfg.enable_log_dir_cleanup && log_dir.exists() && log_dir.is_dir() {
                    run_log_directory_cleanup_background(log_dir);
                }

                Some(log_file_in_dir(log_dir))
            },
            LoggingMode::File(path_buf) => Some(path_buf.clone()),
            LoggingMode::Console => None,
        }
    };

    // Set up either logging to console or to a log file.
    if let Some(log_file) = maybe_log_file {
        // Attempt logging to a file, but fallback to console logging on error.
        if let Err(e) = init_logging_to_file(&log_file, cfg.use_json) {
            init_logging_to_console(&cfg);
            error!("Error logging to file {log_file:?} ({e}); falling back to console logging.");
        }
    } else {
        init_logging_to_console(&cfg);
    }

    // Log the version information.
    info!("{}, xet-core revision {}", &cfg.version, git_version::git_version!());
}

fn init_logging_to_console(cfg: &LoggingConfig) {
    // Now, just use basic console logging.
    let registry = tracing_subscriber::registry();

    #[cfg(feature = "tokio-console")]
    let registry = {
        // Console subscriber layer for tokio-console, custom filter for tokio trace level events
        let console_layer = console_subscriber::spawn().with_filter(EnvFilter::new("tokio=trace,runtime=trace"));
        registry.with(console_layer)
    };

    let fmt_layer_base = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false);
    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL_CONSOLE))
        .unwrap_or_default();

    if cfg.use_json {
        let filtered_fmt_layer = fmt_layer_base.json().with_filter(fmt_filter);
        registry.with(filtered_fmt_layer).init();
    } else {
        let filtered_fmt_layer = fmt_layer_base.pretty().with_filter(fmt_filter);
        registry.with(filtered_fmt_layer).init();
    }
}

fn init_logging_to_file(path: &Path, use_json: bool) -> Result<(), std::io::Error> {
    // Set up logging to a file.
    use std::ffi::OsStr;

    use tracing_appender::{non_blocking, rolling};

    let (path, file_name) = match path.file_name() {
        Some(name) => (path.to_path_buf(), name),
        None => (path.join("xet.log"), OsStr::new("xet.log")),
    };

    let log_directory = match path.parent() {
        Some(parent) => {
            std::fs::create_dir_all(parent)?;
            parent
        },
        None => Path::new("."),
    };

    // Make sure the log location is writeable so we error early here and dump to stderr on failure.
    std::fs::write(&path, [])?;

    // Build a non‑blocking file appender. • `rolling::never` = one static file, no rotation. • Keep the
    // `WorkerGuard` alive so the background thread doesn’t shut down and drop messages.
    let file_appender = rolling::never(log_directory, file_name);

    let (writer, guard) = non_blocking(file_appender);

    // Store the guard globally so it isn’t dropped.
    static FILE_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();
    let _ = FILE_GUARD.set(guard); // ignore error if already initialised

    let registry = tracing_subscriber::registry();
    #[cfg(feature = "tokio-console")]
    let registry = {
        // Console subscriber layer for tokio-console, custom filter for tokio trace level events
        let console_layer = console_subscriber::spawn().with_filter(EnvFilter::new("tokio=trace,runtime=trace"));
        registry.with(console_layer)
    };

    // Build the fmt layer.
    let fmt_layer_base = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .with_writer(writer);
    // Standard filter layer: RUST_LOG env var or DEFAULT_LOG_LEVEL fallback.
    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL_FILE))
        .unwrap_or_default();

    if use_json {
        registry.with(fmt_layer_base.json().with_filter(fmt_filter)).init();
    } else {
        registry.with(fmt_layer_base.pretty().with_filter(fmt_filter)).init();
    };

    Ok(())
}

/// Build `<prefix>_<YYYYMMDD>T<HHMMSS><mmm><+/-HHMM>_<pid>.log` in `dir`.
/// Timestamp is in *local time with numeric offset* (e.g., -0700), filename-safe.
pub fn log_file_in_dir(dir: impl AsRef<Path>) -> PathBuf {
    let now_local: DateTime<Local> = Local::now();
    let now_fixed: DateTime<FixedOffset> = now_local.with_timezone(now_local.offset());

    // ISO 8601 basic, filename-safe (no colons): 20250915T083210123-0700
    let ts = now_fixed.format("%Y%m%dT%H%M%S%3f%z"); // %z => ±HHMM

    let pid = std::process::id();
    let prefix = LOG_PREFIX.as_str();
    let filename = format!("{}_{}_{}.log", prefix, ts, pid);
    dir.as_ref().join(filename)
}

/// Parse `<prefix>_<YYYYMMDD>T<HHMMSS><mmm><+/-HHMM>_<pid>.log`
/// Works with full paths or bare filenames.
/// Returns (prefix, timestamp with fixed offset, pid).
pub fn parse_log_file_name(path: impl AsRef<Path>) -> Option<(String, DateTime<FixedOffset>, u32)> {
    let path = path.as_ref();
    let file_name = path.file_name()?.to_str()?;

    // Returns None if it doesn't end with .log.
    let file_name = file_name.strip_suffix(".log")?;

    // Split from the RIGHT so base may contain underscores.
    // Expect exactly: <base>_<timestamp>_<pid>
    let mut parts = file_name.rsplitn(3, '_');
    let pid_str = parts.next()?;
    let ts_str = parts.next()?;
    let prefix = parts.next()?; // remainder is the full base (may include underscores)

    let pid: u32 = pid_str.parse().ok()?;

    // Parse ISO 8601-basic with offset, no colons
    let ts = DateTime::parse_from_str(ts_str, "%Y%m%dT%H%M%S%3f%z").ok()?;

    Some((prefix.to_string(), ts, pid))
}

// A utility struct to help with the directory cleanup.
struct CandidateLogFile {
    path: PathBuf,
    size: u64,
    age: Duration,
}

fn run_log_directory_cleanup_background(log_dir: &Path) {
    // Spawn run_log_directory_cleanup as background thread, logging any errors as a warn!
    let log_dir = log_dir.to_path_buf();
    std::thread::spawn(move || {
        if let Err(e) = run_log_directory_cleanup(&log_dir) {
            warn!("Error during log directory cleanup in {:?}: {}", log_dir, e);
        }
    });
}

fn run_log_directory_cleanup(log_dir: &Path) -> io::Result<()> {
    let min_age = *LOG_DIR_MIN_DELETION_AGE;
    let max_retention = *LOG_DIR_MAX_RETENTION_AGE;
    let size_limit_bytes = *LOG_DIR_MAX_SIZE;
    let size_limit = size_limit_bytes.as_u64();

    info!(
        "starting log cleanup in {:?} (min_age={:?}, max_retention={:?}, max_size={} bytes)",
        log_dir, min_age, max_retention, size_limit_bytes
    );

    // Initialize sysinfo once to get a list of the active process ids.  To ensure we never delete
    // a log file associated with an active process, we preserve any log file associated with a currently
    // active PID.
    let sys = System::new_with_specifics(RefreshKind::nothing().with_processes(ProcessRefreshKind::everything()));

    // Collect candidate files.
    let mut candidates = Vec::<CandidateLogFile>::new();
    let mut total_bytes: u64 = 0;
    let mut candidate_deletion_bytes: u64 = 0;

    let now = Utc::now();
    let mut n_log_files = 0usize;

    for entry in std::fs::read_dir(log_dir)? {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                warn!("read_dir entry error: {}", e);
                continue;
            },
        };
        let path = entry.path();

        let Ok(ft) = entry.file_type() else { continue };
        if !ft.is_file() {
            continue;
        }

        let Some((prefix, timestamp, pid)) = parse_log_file_name(&path) else {
            debug!("ignoring unparseable log file {:?}", path);
            continue;
        };

        if prefix != *LOG_PREFIX {
            debug!("ignoring log file {:?} with differing prefix {prefix} (!={})", path, *LOG_PREFIX);
            continue;
        }

        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                warn!("metadata failed for {:?}: {}", path, e);

                continue;
            },
        };

        let size = meta.len();
        total_bytes += size;
        n_log_files += 1;

        let Ok(age) = (now - timestamp.to_utc()).to_std() else {
            debug!("Skipping deletion for very new log file {path:?}");
            continue;
        };

        // Skip if it's too new.
        if age < min_age {
            debug!("Skipping deletion for new log file {path:?}");
            continue;
        }

        // Skip if there is an active PID associated with the file.
        if sys.process(Pid::from_u32(pid)).is_some() {
            debug!("Skipping deletion for log file {path:?} with active associated PID.");
            continue;
        }

        // These files are available for deletion.
        candidates.push(CandidateLogFile { path, size, age });

        candidate_deletion_bytes += size;
    }

    info!(
        "Log Directory Cleanup: found {:?} of logs in {} log files, with {:?} in {} files eligible for deletion.",
        ByteSize::new(total_bytes),
        n_log_files,
        ByteSize::new(candidate_deletion_bytes),
        candidates.len()
    );

    // 1) Hard expiration pass: delete anything older than max_retention, unless protected.
    let mut deleted_bytes: u64 = 0;
    candidates.retain(|lf| {
        if lf.age > max_retention {
            let path = &lf.path;
            match std::fs::remove_file(path) {
                Ok(_) => {
                    deleted_bytes += lf.size;
                    debug!("Log Directory Cleanup: Removed old log file {path:?})");
                },
                Err(e) => {
                    info!("Log Directory Cleanup: Error removing old log file {path:?}, skipping: {e}");
                },
            };
            false
        } else {
            true
        }
    });

    // 2) Size trimming: if above the limit, delete oldest eligible (unprotected) first.
    let mut n_pruned = 0;
    if total_bytes - deleted_bytes > size_limit {
        // Sort by oldest first.
        candidates.sort_by_key(|lf| lf.age);
        for lf in &candidates {
            if total_bytes - deleted_bytes <= size_limit {
                break;
            }

            match std::fs::remove_file(&lf.path) {
                Ok(()) => {
                    deleted_bytes += lf.size;
                    n_pruned += 1;
                    debug!("Log Directory cleanup: Pruned log file {:?}.", lf.path);
                },
                Err(e) => info!("Log Directory Cleanup: Error removing size-pruned log file {:?}: {}", lf.path, e),
            }
        }
    }

    info!(
        "Log Directory Cleanup: deleted {:?} in {} files",
        ByteSize::new(deleted_bytes),
        candidates.len() - n_pruned
    );
    Ok(())
}

#[cfg(test)]
mod tests {

    use chrono::{Datelike, Timelike};

    use super::*;

    #[test]
    fn round_trip_make_and_parse() {
        let dir = Path::new("/tmp");
        let path = log_file_in_dir(dir);
        let (base, ts, pid) = parse_log_file_name(&path).expect("parse");
        assert_eq!(base, *LOG_PREFIX);
        assert!(pid > 0);

        // Verify that the timestamp string matches what's embedded in the filename
        let fname = path.file_name().unwrap().to_str().unwrap();
        let ts_part = fname
            .strip_prefix(&format!("{}_", base))
            .unwrap()
            .strip_suffix(&format!("_{}.log", pid))
            .unwrap();
        assert_eq!(ts_part, ts.format("%Y%m%dT%H%M%S%3f%z").to_string());
    }

    #[test]
    fn parse_known_file() {
        let fname = "app_base_20250915T083210123-0700_12345.log";
        let (base, ts, pid) = parse_log_file_name(fname).expect("parse");
        assert_eq!(base, "app_base");
        assert_eq!(pid, 12345);
        assert_eq!(ts.format("%Y%m%dT%H%M%S%3f%z").to_string(), "20250915T083210123-0700");
        assert_eq!(ts.year(), 2025);
        assert_eq!(ts.month(), 9);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 8);
        assert_eq!(ts.minute(), 32);
        assert_eq!(ts.second(), 10);
        assert_eq!(ts.timestamp_subsec_millis(), 123);
        assert_eq!(ts.offset().local_minus_utc(), -7 * 3600);
    }

    #[test]
    fn allows_underscores_in_base() {
        let fname = "my_cool_app_20240102T030405006+0530_999.log";
        let (base, ts, pid) = parse_log_file_name(fname).expect("parse");
        assert_eq!(base, "my_cool_app");
        assert_eq!(pid, 999);
        assert_eq!(ts.format("%Y%m%dT%H%M%S%3f%z").to_string(), "20240102T030405006+0530");
    }

    #[test]
    fn parse_with_directory_path() {
        let path = Path::new("/var/log/myprog/app_20250915T083210123-0700_12345.log");
        let (base, _, pid) = parse_log_file_name(path).expect("parse");
        assert_eq!(base, "app");
        assert_eq!(pid, 12345);
    }
}
