use std::sync::atomic::{AtomicUsize, Ordering};

static BASELINE_FD_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Returns the number of currently open file descriptors for this process.
#[cfg(target_os = "macos")]
pub fn count_open_fds() -> usize {
    std::fs::read_dir("/dev/fd").map(|d| d.count()).unwrap_or(0)
}

/// Returns the number of currently open file descriptors for this process.
#[cfg(target_os = "linux")]
pub fn count_open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
}

/// Returns the number of currently open file descriptors for this process.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn count_open_fds() -> usize {
    0
}

/// Sets the baseline FD count to the current value. Call this once at the start
/// of a test or process to establish a reference point.
pub fn set_fd_baseline() {
    BASELINE_FD_COUNT.store(count_open_fds(), Ordering::Relaxed);
}

/// Returns the number of file descriptors opened since the last call to
/// [`set_fd_baseline`]. Negative values (FDs closed) are reported as 0.
pub fn fds_since_baseline() -> usize {
    count_open_fds().saturating_sub(BASELINE_FD_COUNT.load(Ordering::Relaxed))
}

/// Logs the current FD count at `eprintln` level with a label. Useful for
/// quickly instrumenting code paths during debugging.
pub fn report_fd_count(label: &str) {
    let count = count_open_fds();
    let baseline = BASELINE_FD_COUNT.load(Ordering::Relaxed);
    if baseline > 0 {
        eprintln!("[FD] {label}: {count} open (delta from baseline: {})", count as isize - baseline as isize);
    } else {
        eprintln!("[FD] {label}: {count} open");
    }
}

/// Returns a snapshot of all currently open file descriptors and what they
/// point to (via readlink on macOS/Linux). Useful for diffing before/after
/// to identify leaked FDs.
#[cfg(target_os = "macos")]
pub fn list_open_fds() -> Vec<(i32, String)> {
    let Ok(entries) = std::fs::read_dir("/dev/fd") else {
        return Vec::new();
    };
    let mut fds: Vec<(i32, String)> = entries
        .filter_map(|e| {
            let e = e.ok()?;
            let fd: i32 = e.file_name().to_str()?.parse().ok()?;
            let target = std::fs::read_link(e.path())
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "<unknown>".into());
            Some((fd, target))
        })
        .collect();
    fds.sort_by_key(|(fd, _)| *fd);
    fds
}

/// Returns a snapshot of all currently open file descriptors and what they
/// point to (via readlink on Linux).
#[cfg(target_os = "linux")]
pub fn list_open_fds() -> Vec<(i32, String)> {
    let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
        return Vec::new();
    };
    let mut fds: Vec<(i32, String)> = entries
        .filter_map(|e| {
            let e = e.ok()?;
            let fd: i32 = e.file_name().to_str()?.parse().ok()?;
            let target = std::fs::read_link(e.path())
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "<unknown>".into());
            Some((fd, target))
        })
        .collect();
    fds.sort_by_key(|(fd, _)| *fd);
    fds
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn list_open_fds() -> Vec<(i32, String)> {
    Vec::new()
}

/// Prints FDs present in `after` but not in `before`, using `lsof` for details on macOS.
pub fn print_new_fds(label: &str, before: &[(i32, String)], after: &[(i32, String)]) {
    let before_set: std::collections::HashSet<i32> = before.iter().map(|(fd, _)| *fd).collect();
    let new_fds: Vec<_> = after.iter().filter(|(fd, _)| !before_set.contains(fd)).collect();
    if new_fds.is_empty() {
        eprintln!("[FD-DIFF] {label}: no new FDs");
    } else {
        eprintln!("[FD-DIFF] {label}: {} new FDs:", new_fds.len());
        for (fd, target) in &new_fds {
            eprintln!("  fd {fd} -> {target}");
        }
        let pid = std::process::id();
        let fd_list: String = new_fds.iter().map(|(fd, _)| fd.to_string()).collect::<Vec<_>>().join(",");
        if let Ok(output) = std::process::Command::new("lsof")
            .args(["-p", &pid.to_string(), "-a", "-d", &fd_list])
            .output()
        {
            eprintln!("[FD-DIFF] lsof output:\n{}", String::from_utf8_lossy(&output.stdout));
        }
    }
}
