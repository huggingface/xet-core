use std::env;
use std::time::Duration;

use tracing::info;
use xet_logging::{LoggingConfig, LoggingMode};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <log_directory> <num_lines>", args[0]);
        std::process::exit(1);
    }

    let log_directory = &args[1];
    let num_lines: usize = args[2].parse().expect("num_lines must be a number");

    // Initialize logging to the specified directory
    let config = LoggingConfig {
        logging_mode: LoggingMode::Directory(log_directory.into()),
        use_json: true,
        enable_log_dir_cleanup: true,
        version: "test".to_string(),
        log_dir_config: xet_logging::LogDirConfig::default(),
    };

    xet_logging::init(config);

    // Generate log messages with 5ms delay between each
    for i in 0..num_lines {
        info!("Test log message number {} - this is a dummy log message for testing purposes", i + 1);

        // Wait for 50 microseconds between each log message just to spread it out a little...
        std::thread::sleep(Duration::from_micros(50));
    }

    // Wait for background cleanup to complete before exiting
    xet_logging::wait_for_log_directory_cleanup();
}
