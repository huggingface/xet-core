#[cfg(feature = "fd-track")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "fd-track")]
use tracing::debug;

#[cfg(feature = "fd-track")]
static BASELINE_FD_COUNT: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "fd-track")]
static PEAK_FD_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(any(target_os = "macos", target_os = "linux"))]
const PROC_FD_PATH: &str = if cfg!(target_os = "macos") {
    "/dev/fd"
} else {
    "/proc/self/fd"
};

/// Returns the number of currently open file descriptors for this process.
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub fn count_open_fds() -> usize {
    std::fs::read_dir(PROC_FD_PATH).map(|d| d.count()).unwrap_or(0)
}

/// Returns the number of currently open file descriptors for this process.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn count_open_fds() -> usize {
    0
}

#[cfg(feature = "fd-track")]
fn baseline_or_init() -> usize {
    let baseline = BASELINE_FD_COUNT.load(Ordering::Relaxed);
    if baseline == 0 {
        set_fd_baseline();
        BASELINE_FD_COUNT.load(Ordering::Relaxed)
    } else {
        baseline
    }
}

#[cfg(feature = "fd-track")]
fn update_peak(current: usize) {
    let mut observed = PEAK_FD_COUNT.load(Ordering::Relaxed);
    while current > observed
        && PEAK_FD_COUNT
            .compare_exchange_weak(observed, current, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        observed = PEAK_FD_COUNT.load(Ordering::Relaxed);
    }
}

/// Sets the baseline FD count to the current value. Call this once at the start
/// of a test or process to establish a reference point.
pub fn set_fd_baseline() {
    #[cfg(feature = "fd-track")]
    {
        let count = count_open_fds();
        BASELINE_FD_COUNT.store(count, Ordering::Relaxed);
        PEAK_FD_COUNT.store(count, Ordering::Relaxed);
        debug!(
            target: "xet_runtime::fd_track",
            fd_count = count,
            "FD baseline initialized"
        );
    }
}

/// Returns the number of file descriptors opened since the last call to
/// [`set_fd_baseline`]. Negative values (FDs closed) are reported as 0.
pub fn fds_since_baseline() -> usize {
    #[cfg(feature = "fd-track")]
    {
        let baseline = baseline_or_init();
        count_open_fds().saturating_sub(baseline)
    }
    #[cfg(not(feature = "fd-track"))]
    {
        0
    }
}

/// Logs the current FD count snapshot with a label when `fd-track` is enabled.
pub fn report_fd_count(label: &str) {
    #[cfg(feature = "fd-track")]
    {
        let count = count_open_fds();
        let baseline = baseline_or_init();
        update_peak(count);
        debug!(
            target: "xet_runtime::fd_track",
            label,
            fd_count = count,
            baseline,
            delta = count as isize - baseline as isize,
            opened_since_baseline = fds_since_baseline(),
            peak = PEAK_FD_COUNT.load(Ordering::Relaxed),
            "FD snapshot"
        );
    }
    #[cfg(not(feature = "fd-track"))]
    {
        let _ = label;
    }
}

/// RAII scope guard that records FD growth/cleanup over a code path.
/// Logs on creation/drop only when `fd-track` is enabled.
#[derive(Debug, Default)]
pub struct FdTrackGuard {
    #[cfg(feature = "fd-track")]
    label: String,
    #[cfg(feature = "fd-track")]
    start_count: usize,
    #[cfg(all(feature = "fd-track", unix))]
    start_snapshot: Vec<(i32, String)>,
}

pub fn track_fd_scope(label: impl Into<String>) -> FdTrackGuard {
    #[cfg(feature = "fd-track")]
    {
        let label = label.into();
        let start_count = count_open_fds();
        let baseline = baseline_or_init();
        update_peak(start_count);
        debug!(
            target: "xet_runtime::fd_track",
            label,
            start_fd = start_count,
            baseline,
            delta = start_count as isize - baseline as isize,
            "FD scope start"
        );
        FdTrackGuard {
            label,
            start_count,
            #[cfg(unix)]
            start_snapshot: list_open_fds(),
        }
    }
    #[cfg(not(feature = "fd-track"))]
    {
        let _ = label;
        FdTrackGuard::default()
    }
}

impl Drop for FdTrackGuard {
    fn drop(&mut self) {
        #[cfg(feature = "fd-track")]
        {
            let end_count = count_open_fds();
            let baseline = baseline_or_init();
            update_peak(end_count);
            let change = end_count as isize - self.start_count as isize;
            debug!(
                target: "xet_runtime::fd_track",
                label = self.label,
                start_fd = self.start_count,
                end_fd = end_count,
                change,
                baseline,
                baseline_delta = end_count as isize - baseline as isize,
                opened_since_baseline = fds_since_baseline(),
                peak = PEAK_FD_COUNT.load(Ordering::Relaxed),
                "FD scope end"
            );

            #[cfg(unix)]
            if change > 0 {
                let end_snapshot = list_open_fds();
                print_new_fds(&self.label, &self.start_snapshot, &end_snapshot);
            }
        }
    }
}

/// Returns a snapshot of all currently open file descriptors and what they
/// point to (via readlink on macOS/Linux). Useful for diffing before/after
/// to identify leaked FDs.
#[cfg(target_os = "macos")]
pub fn list_open_fds() -> Vec<(i32, String)> {
    #[cfg(feature = "fd-track")]
    {
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
    #[cfg(not(feature = "fd-track"))]
    {
        Vec::new()
    }
}

/// Returns a snapshot of all currently open file descriptors and what they
/// point to (via readlink on Linux).
#[cfg(target_os = "linux")]
pub fn list_open_fds() -> Vec<(i32, String)> {
    #[cfg(feature = "fd-track")]
    {
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
    #[cfg(not(feature = "fd-track"))]
    {
        Vec::new()
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn list_open_fds() -> Vec<(i32, String)> {
    Vec::new()
}

/// Prints FDs present in `after` but not in `before`.
#[cfg(unix)]
pub fn print_new_fds(label: &str, before: &[(i32, String)], after: &[(i32, String)]) {
    #[cfg(feature = "fd-track")]
    {
        let before_set: std::collections::HashSet<i32> = before.iter().map(|(fd, _)| *fd).collect();
        let new_fds: Vec<_> = after.iter().filter(|(fd, _)| !before_set.contains(fd)).collect();

        if new_fds.is_empty() {
            debug!(target: "xet_runtime::fd_track", label, "FD diff: no new descriptors");
            return;
        }

        debug!(
            target: "xet_runtime::fd_track",
            label,
            new_fd_count = new_fds.len(),
            "FD diff: detected new descriptors"
        );
        for (fd, target) in &new_fds {
            debug!(target: "xet_runtime::fd_track", label, fd = *fd, target = %target, "FD diff entry");
        }

        let pid = std::process::id();
        let fd_list: String = new_fds.iter().map(|(fd, _)| fd.to_string()).collect::<Vec<_>>().join(",");
        if let Ok(output) = std::process::Command::new("lsof")
            .args(["-p", &pid.to_string(), "-a", "-d", &fd_list])
            .output()
            && output.status.success()
        {
            let lsof = String::from_utf8_lossy(&output.stdout);
            debug!(target: "xet_runtime::fd_track", label, lsof = %lsof, "FD diff lsof details");
        }
    }
    #[cfg(not(feature = "fd-track"))]
    {
        let _ = (label, before, after);
    }
}

#[cfg(not(unix))]
pub fn print_new_fds(label: &str, before: &[(i32, String)], after: &[(i32, String)]) {
    let _ = (label, before, after);
}
