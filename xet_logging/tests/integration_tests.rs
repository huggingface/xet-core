use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use std::{fs, thread};

use rand::prelude::*;
use tempfile::TempDir;

/// Helper function to run the test executable with environment variables
fn run_test_executable(log_dir: &Path, num_lines: usize, env_vars: &[(&str, &str)]) {
    let executable_path = PathBuf::from(env!("CARGO_BIN_EXE_log_test_executable"));

    let mut command = Command::new(&executable_path);
    command.arg(log_dir.to_string_lossy().as_ref()).arg(num_lines.to_string());

    // Set environment variables
    for (key, value) in env_vars {
        command.env(key, value);
    }

    // Use spawn() instead of output() to pipe stderr through
    let mut child = command.spawn().expect("Failed to execute test executable");
    let status = child.wait().expect("Failed to wait for test executable");

    if !status.success() {
        panic!("Test executable failed with status: {}", status);
    }
}

/// Helper function to run the test executable in parallel
fn run_test_executable_parallel(log_dir: &Path, num_lines: usize, env_vars: &[(&str, &str)]) -> thread::JoinHandle<()> {
    let executable_path = PathBuf::from(env!("CARGO_BIN_EXE_log_test_executable"));
    let log_dir = log_dir.to_path_buf();
    let env_vars: Vec<(String, String)> = env_vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

    thread::spawn(move || {
        let mut command = Command::new(&executable_path);
        command.arg(log_dir.to_string_lossy().as_ref()).arg(num_lines.to_string());

        // Set environment variables
        for (key, value) in &env_vars {
            command.env(key, value);
        }

        // Use spawn() instead of output() to pipe stderr through
        let mut child = command.spawn().expect("Failed to execute test executable");
        let status = child.wait().expect("Failed to wait for test executable");

        if !status.success() {
            panic!("Test executable failed with status: {}", status);
        }
    })
}

/// Helper function to calculate total size of directory
fn get_directory_size(dir: &Path) -> u64 {
    let mut total_size = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    total_size += metadata.len();
                }
            }
        }
    }
    total_size
}

/// Helper function to count log files in directory
fn count_log_files(dir: &Path) -> usize {
    if let Ok(entries) = fs::read_dir(dir) {
        entries
            .flatten()
            .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "log"))
            .count()
    } else {
        0
    }
}

#[test]
fn test_maximum_age_cleanup() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();

    // Set up environment variables for 500ms max age
    let env_vars = [
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "500ms"),
        ("HF_XET_LOG_DIR_MIN_DELETION_AGE", "100ms"),
        ("HF_XET_LOG_DIR_MAX_SIZE", "1gb"), // Set high to avoid size-based cleanup
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "false"),
    ];

    // Run the test executable multiple times to create enough log files
    for _ in 0..5 {
        run_test_executable(log_dir, 100, &env_vars);
    }

    run_test_executable(log_dir, 10, &env_vars);

    // Wait for files to age beyond the retention period
    std::thread::sleep(Duration::from_millis(1000));

    // Run the test executable again to trigger cleanup
    run_test_executable(log_dir, 5, &env_vars);

    // Check that old files have been cleaned up
    let log_files = count_log_files(log_dir);
    assert!(log_files <= 1, "Expected at most 1 log file after age-based cleanup, found {}", log_files);
}

#[test]
fn test_maximum_size_cleanup() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();

    // Set up environment variables for 10kb max size
    let env_vars = [
        ("HF_XET_LOG_DIR_MAX_SIZE", "10kb"),
        ("HF_XET_LOG_DIR_MIN_DELETION_AGE", "1ms"), // Disable the minimum deletion guard
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "1h"), // Set high to avoid age-based cleanup
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "false"),
    ];

    // Run the test executable multiple times to create enough log files
    for _ in 0..20 {
        run_test_executable(log_dir, 1000, &env_vars); // Small log files
    }

    // Wait for disk to be synchronized.
    std::thread::sleep(Duration::from_millis(100));

    // Run one final tiny executable to trigger cleanup of the previous file
    // This file should be small enough to stay under 10KB even if it's not cleaned up
    run_test_executable(log_dir, 1, &env_vars);

    // Wait for the final cleanup to complete
    std::thread::sleep(Duration::from_millis(100));

    // Check that directory size is within limits
    let total_size = get_directory_size(log_dir);
    assert!(total_size <= 10 * 1024, "Directory size {} exceeds 10kb limit", total_size);
}

#[test]
fn test_active_window_protection() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();

    // Set up environment variables for small size limit to trigger cleanup
    let env_vars = [
        ("HF_XET_LOG_DIR_MAX_SIZE", "1kb"),
        ("HF_XET_LOG_DIR_MIN_DELETION_AGE", "1s"),
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "1h"),
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "false"),
    ];

    // Run the test executable to create log files
    for _ in 0..3 {
        run_test_executable(log_dir, 100, &env_vars);
    }

    // Wait for disk to be synchronized.
    std::thread::sleep(Duration::from_millis(100));

    // Immediately check that the current log file still exists
    // (it should not be deleted because it's associated with an active process)
    let log_files = count_log_files(log_dir);
    assert!(log_files <= 5);

    // Directory is larger than the minimum due to the above protection.
    let log_dir_size = get_directory_size(log_dir);
    assert!(log_dir_size > 1 * 1024);

    // Wait a bit longer than the minimum deletion age and run again to create another log -- and clean the rest up.
    std::thread::sleep(Duration::from_secs(2));

    for _ in 0..2 {
        run_test_executable(log_dir, 100, &env_vars);
    }

    // Wait for disk to be synchronized.
    std::thread::sleep(Duration::from_millis(100));

    // All the previous ones should now have been cleaned up.
    let log_files = count_log_files(log_dir);
    assert_eq!(log_files, 2);
}

#[test]
fn test_cleanup_disabled() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();

    // Set up environment variables to disable cleanup
    let env_vars = [
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "true"),
        ("HF_XET_LOG_DIR_MAX_SIZE", "1kb"),
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "1s"),
    ];

    // Run the test executable multiple times
    for _ in 0..3 {
        run_test_executable(log_dir, 20, &env_vars);
        std::thread::sleep(Duration::from_millis(50));
    }

    // Wait for disk to be synchronized.
    std::thread::sleep(Duration::from_millis(100));

    // All files should still be there since cleanup is disabled
    let log_files = count_log_files(log_dir);
    assert_eq!(log_files, 3);
}

#[test]
fn test_maximum_age_cleanup_parallel() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();
    std::fs::create_dir_all(log_dir).expect("Failed to create log directory");

    let env_vars = [
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "500ms"),
        ("HF_XET_LOG_DIR_MIN_DELETION_AGE", "100ms"),
        ("HF_XET_LOG_DIR_MAX_SIZE", "1gb"),
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "false"),
    ];

    // Spawn multiple background tasks
    let mut handles = Vec::new();
    for _ in 0..5 {
        let handle = run_test_executable_parallel(log_dir, 100, &env_vars);
        handles.push(handle);
    }

    // Run one more to trigger cleanup
    run_test_executable(log_dir, 10, &env_vars);

    // Wait for all background tasks to complete
    for handle in handles {
        handle.join().expect("Background task failed");
    }

    std::thread::sleep(Duration::from_millis(1000));
    run_test_executable(log_dir, 5, &env_vars);
    std::thread::sleep(Duration::from_millis(500)); // Wait for background cleanup
    let log_files = count_log_files(log_dir);
    assert!(log_files <= 1, "Expected at most 1 log file after age-based cleanup, found {}", log_files);
}

#[test]
fn test_maximum_size_cleanup_parallel() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();
    std::fs::create_dir_all(log_dir).expect("Failed to create log directory");

    let env_vars = [
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "1h"), // Set high to avoid age-based cleanup
        ("HF_XET_LOG_DIR_MIN_DELETION_AGE", "1ms"), // Disable the minimum deletion guard
        ("HF_XET_LOG_DIR_MAX_SIZE", "10kb"),
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "false"),
    ];

    // Spawn multiple background tasks
    let mut handles = Vec::new();
    for _ in 0..20 {
        let handle = run_test_executable_parallel(log_dir, 1000, &env_vars);
        handles.push(handle);
    }

    // Wait for all background tasks to complete
    for handle in handles {
        handle.join().expect("Background task failed");
    }

    // Wait for disk to be synchronized.
    std::thread::sleep(Duration::from_millis(100));

    // Run one final tiny executable to trigger cleanup of the previous file
    // This file should be small enough to stay under 10KB even if it's not cleaned up
    run_test_executable(log_dir, 1, &env_vars);

    // Wait for the final cleanup to complete
    std::thread::sleep(Duration::from_millis(100));

    // Check that directory size is within limits
    let total_size = get_directory_size(log_dir);
    assert!(total_size <= 10 * 1024, "Directory size {} exceeds 10kb limit", total_size);
}

#[test]
fn test_active_window_protection_parallel() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();
    std::fs::create_dir_all(log_dir).expect("Failed to create log directory");

    let env_vars = [
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "1h"), // Set high to avoid age-based cleanup
        ("HF_XET_LOG_DIR_MIN_DELETION_AGE", "1ms"), // Disable the minimum deletion guard
        ("HF_XET_LOG_DIR_MAX_SIZE", "10kb"),
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "false"),
    ];

    // Spawn multiple background tasks that will run concurrently
    let mut handles = Vec::new();
    for _ in 0..5 {
        let handle = run_test_executable_parallel(log_dir, 1000, &env_vars);
        handles.push(handle);
    }

    // Wait for all background tasks to complete
    for handle in handles {
        handle.join().expect("Background task failed");
    }

    // Wait for disk to be synchronized.
    std::thread::sleep(Duration::from_millis(100));

    // Check that directory size is within limits (should be larger than 10kb due to active window protection)
    let total_size = get_directory_size(log_dir);
    assert!(
        total_size > 10 * 1024,
        "Expected directory size to exceed 10kb due to active window protection, found {}",
        total_size
    );
}

#[test]
fn test_cleanup_stress_test() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path();

    let env_vars = [
        ("HF_XET_LOG_DIR_MAX_RETENTION_AGE", "1s"), // Set to 1 sec so that files are deleted properly
        ("HF_XET_LOG_DIR_MIN_DELETION_AGE", "1ms"), // Disable the minimum deletion guard
        ("HF_XET_LOG_DIR_MAX_SIZE", "10kb"),        // Small size limit to trigger frequent cleanup
        ("HF_XET_LOG_DIR_DISABLE_CLEANUP", "false"),
    ];

    // Spawn many background tasks to try to reproduce race conditions
    let mut handles = Vec::new();
    let mut rng = StdRng::seed_from_u64(42);
    for i in 0..100 {
        //
        let log_size = rng.random_range(1..=250);
        let handle = run_test_executable_parallel(log_dir, log_size, &env_vars);
        handles.push(handle);

        // Small delay between spawns to create overlapping execution
        if i % 10 == 0 {
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    // Wait for all background tasks to complete
    for handle in handles {
        handle.join().expect("Background task failed");
    }

    // Wait for any remaining cleanup to complete
    std::thread::sleep(Duration::from_millis(100));

    // Run a few more tasks to trigger final cleanup
    let mut handles = Vec::new();
    for _ in 0..5 {
        let handle = run_test_executable_parallel(log_dir, 5, &env_vars);
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Background task failed");
    }

    // Wait for the above files to expire.
    std::thread::sleep(Duration::from_millis(2000));

    run_test_executable(log_dir, 1, &env_vars);

    // Check that directory size is within limits (should be under 50kb due to cleanup)
    let total_size = get_directory_size(log_dir);
    assert!(
        total_size <= 10 * 1024,
        "Expected directory size to be under 50kb after stress test cleanup, found {}",
        total_size
    );
}
