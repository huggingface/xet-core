use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sysinfo::{Networks, Pid, Process, ProcessRefreshKind, RefreshKind, System};
use thiserror::Error;
use tracing::error;

/// A utility for monitoring system resource usage of a process.
///
/// `SystemMonitor` can be configured to track a specific process ID or the current process.
/// It periodically samples CPU usage, memory usage, disk I/O, and network I/O,
/// and writes the metrics to a specified output file or to the tracing log.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use utils::system_monitor::SystemMonitor;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let monitor = SystemMonitor::follow_process(Duration::from_secs(5), Some("monitor_log.txt".to_string()))?;
/// monitor.start()?;
///
/// // ... application logic ...
///
/// monitor.stop()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SystemMonitor {
    pid: Option<Pid>,
    sample_interval: Duration,
    output_path: Option<String>,
    monitor_loop: Mutex<Option<JoinHandle<Result<()>>>>,
    stop: Arc<AtomicBool>,
}

/// Internal state for sampling system metrics.
///
/// This struct holds the `sysinfo` `System` and `Networks` objects, which are refreshed
/// at each sampling interval. It also tracks the process ID being monitored and timing
/// information to calculate rates and averages.
#[derive(Debug)]
struct SystemSampler {
    system: System,
    network: Networks,
    pid: Option<Pid>,
    start_measurement_time: Instant,
    last_measurement_time: Instant,
    last_sample: Option<Metrics>,
    baseline_sample: Metrics,
}

/// A snapshot of system metrics at a specific point in time.
///
/// This struct contains detailed information about CPU, memory, disk, and network usage
/// for the monitored process.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Metrics {
    /// Process ID of the monitored process
    pid: u32,
    /// Name of the process
    name: String,
    /// Total run time of the process in seconds
    run_time: u64,
    /// CPU usage metrics
    cpu_usage: CpuUsage,
    /// Memory usage metrics
    memory: MemoryUsage,
    /// Disk I/O numbers and speed
    disk: DiskUsage,
    /// Network I/O numbers and speed
    network: NetworkUsage,
}

impl Metrics {
    pub fn create(
        system: &System,
        network: &Networks,
        pid: Pid,
        sample_interval: Duration,
        total_duration: Duration,
        last_sample: Option<Metrics>,
        baseline: &Metrics,
    ) -> Option<Self> {
        let process = system.process(pid)?;

        Some(Self {
            pid: pid.as_u32(),
            name: process.name().to_string_lossy().into(),
            run_time: process.run_time(),
            cpu_usage: CpuUsage::from(process, system),
            memory: MemoryUsage::from(process, system, last_sample.map(|s| s.memory)),
            disk: DiskUsage::from(process, sample_interval, total_duration, &baseline.disk),
            network: NetworkUsage::from(network, sample_interval, total_duration, &baseline.network),
        })
    }

    /// Creates a baseline `Metrics` snapshot at the start of monitoring.
    ///
    /// This captures the initial state of disk and network I/O, which are reported
    /// as cumulative values by the underlying system library. This baseline allows
    /// for calculating the delta of resource usage during the monitoring session.
    ///
    /// This helps provide useful information when used by hf_xet in a long running
    /// Python process, e.g. a iPython notebook
    pub fn baseline(system: &System, network: &Networks, pid: Pid) -> Result<Self> {
        let Some(process) = system.process(pid) else {
            return Err(SystemMonitorError::NoProcess(pid.as_u32()));
        };

        Ok(Self {
            pid: pid.as_u32(),
            name: process.name().to_string_lossy().into(),
            run_time: process.run_time(),
            cpu_usage: CpuUsage::from(process, system),
            memory: MemoryUsage::from(process, system, None),
            disk: DiskUsage::baseline(process),
            network: NetworkUsage::baseline(network),
        })
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}

/// Represents CPU usage metrics.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct CpuUsage {
    /// CPU usage of the monitored process as a percentage.
    process_usage: f32,
    /// Total number of CPUs in the system.
    ncpus: u32,
    /// Usage of individual CPUs as a percentage.
    global_usage: Vec<f32>,
}

impl CpuUsage {
    pub fn from(process: &Process, system: &System) -> Self {
        Self {
            process_usage: process.cpu_usage(),
            ncpus: system.cpus().len() as u32,
            global_usage: system.cpus().iter().map(|c| c.cpu_usage()).collect(),
        }
    }
}

/// Represents memory usage metrics.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct MemoryUsage {
    /// Current memory usage in bytes of the monitored process.
    used_bytes: u64,
    /// Peak memory usage in bytes observed for the monitored process during the session.
    peak_used_bytes: u64,
    /// Memory usage of the monitored process as a percentage of total system RAM.
    percentage: f64,
    /// Total system RAM size in bytes.
    total_bytes: u64,
}

impl MemoryUsage {
    pub fn from(process: &Process, system: &System, last_sample: Option<MemoryUsage>) -> Self {
        Self {
            used_bytes: process.memory(),
            peak_used_bytes: process.memory().max(last_sample.map(|s| s.peak_used_bytes).unwrap_or_default()),
            percentage: process.memory() as f64 / system.total_memory() as f64,
            total_bytes: system.total_memory(),
        }
    }
}

/// Represents disk I/O metrics.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct DiskUsage {
    /// Total number of bytes written by the process since the monitor started.
    total_written_bytes: u64,
    /// Number of bytes written by the process since the last sample.
    written_bytes: u64,
    /// Total number of bytes read by the process since the monitor started.
    total_read_bytes: u64,
    /// Number of bytes read by the process since the last sample.
    read_bytes: u64,

    /// Average write speed in bytes per second over the entire monitoring duration.
    average_write_speed: f64,
    /// Instantaneous write speed in bytes per second over the last sample interval.
    instant_write_speed: f64,
    /// Average read speed in bytes per second over the entire monitoring duration.
    average_read_speed: f64,
    /// Instantaneous read speed in bytes per second over the last sample interval.
    instant_read_speed: f64,
}

impl DiskUsage {
    /// Creates a baseline for disk usage at the start of monitoring.
    ///
    /// This is necessary because `sysinfo` provides cumulative disk I/O statistics
    /// since the process started. To measure usage only during the monitoring period,
    /// we capture this initial state and subtract it from later samples.
    pub fn baseline(process: &Process) -> Self {
        let usage = process.disk_usage();
        Self {
            total_written_bytes: usage.total_written_bytes,
            written_bytes: 0,
            total_read_bytes: usage.total_read_bytes,
            read_bytes: 0,
            average_write_speed: 0.,
            instant_write_speed: 0.,
            average_read_speed: 0.,
            instant_read_speed: 0.,
        }
    }

    pub fn from(process: &Process, sample_interval: Duration, total_duration: Duration, baseline: &DiskUsage) -> Self {
        let usage = process.disk_usage();

        // Subtract stats before the monitor
        let total_written_bytes = usage.total_written_bytes - baseline.total_written_bytes;
        let total_read_bytes = usage.total_read_bytes - baseline.total_read_bytes;

        Self {
            total_written_bytes,
            written_bytes: usage.written_bytes,
            total_read_bytes,
            read_bytes: usage.read_bytes,
            average_write_speed: total_written_bytes as f64 / total_duration.as_secs_f64(),
            instant_write_speed: usage.written_bytes as f64 / sample_interval.as_secs_f64(),
            average_read_speed: total_read_bytes as f64 / total_duration.as_secs_f64(),
            instant_read_speed: usage.read_bytes as f64 / sample_interval.as_secs_f64(),
        }
    }
}

/// Represents network I/O metrics for all interfaces combined.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct NetworkUsage {
    /// Total number of bytes transmitted across all network interfaces since the monitor started.
    total_tx_bytes: u64,
    /// Number of bytes transmitted across all network interfaces since the last sample.
    tx_bytes: u64,
    /// Total number of bytes received across all network interfaces since the monitor started.
    total_rx_bytes: u64,
    /// Number of bytes received across all network interfaces since the last sample.
    rx_bytes: u64,

    /// Average transmit speed in bytes per second over the entire monitoring duration.
    average_tx_speed: f64,
    /// Instantaneous transmit speed in bytes per second over the last sample interval.
    instant_tx_speed: f64,
    /// Average receive speed in bytes per second over the entire monitoring duration.
    average_rx_speed: f64,
    /// Instantaneous receive speed in bytes per second over the last sample interval.
    instant_rx_speed: f64,
}

impl NetworkUsage {
    /// Creates a baseline for network usage at the start of monitoring.
    ///
    /// This is necessary because `sysinfo` provides cumulative network I/O statistics
    /// since the system booted. To measure usage only during the monitoring period,
    /// we capture this initial state and subtract it from later samples.
    pub fn baseline(network: &Networks) -> Self {
        let total_tx_bytes = network.iter().fold(0u64, |sum, (_, nic)| sum + nic.total_transmitted());
        let total_rx_bytes = network.iter().fold(0u64, |sum, (_, nic)| sum + nic.total_received());

        Self {
            total_tx_bytes,
            tx_bytes: 0,
            total_rx_bytes,
            rx_bytes: 0,
            average_tx_speed: 0.,
            instant_tx_speed: 0.,
            average_rx_speed: 0.,
            instant_rx_speed: 0.,
        }
    }

    pub fn from(
        network: &Networks,
        sample_interval: Duration,
        total_duration: Duration,
        baseline: &NetworkUsage,
    ) -> Self {
        let total_tx_bytes =
            network.iter().fold(0u64, |sum, (_, nic)| sum + nic.total_transmitted()) - baseline.total_tx_bytes;
        let tx_bytes = network.iter().fold(0u64, |sum, (_, nic)| sum + nic.transmitted());
        let total_rx_bytes =
            network.iter().fold(0u64, |sum, (_, nic)| sum + nic.total_received()) - baseline.total_rx_bytes;
        let rx_bytes = network.iter().fold(0u64, |sum, (_, nic)| sum + nic.received());

        Self {
            total_tx_bytes,
            tx_bytes,
            total_rx_bytes,
            rx_bytes,
            average_tx_speed: total_tx_bytes as f64 / total_duration.as_secs_f64(),
            instant_tx_speed: tx_bytes as f64 / sample_interval.as_secs_f64(),
            average_rx_speed: total_rx_bytes as f64 / total_duration.as_secs_f64(),
            instant_rx_speed: rx_bytes as f64 / sample_interval.as_secs_f64(),
        }
    }
}

impl SystemSampler {
    pub fn new(pid: Option<Pid>) -> Result<Self> {
        let Some(pid) = pid.or_else(|| sysinfo::get_current_pid().ok()) else {
            return Err(SystemMonitorError::NoPid);
        };

        let system = System::new_all();
        let network = Networks::new_with_refreshed_list();

        let baseline = Metrics::baseline(&system, &network, pid)?;

        let now = Instant::now();

        Ok(Self {
            system,
            network,
            pid: Some(pid),
            start_measurement_time: now,
            last_measurement_time: now,
            last_sample: None,
            baseline_sample: baseline,
        })
    }

    pub fn sample(&mut self, output_path_tmpl: &Option<String>) -> Result<()> {
        // refresh process, cpu, memory and disk usage
        self.system.refresh_all();
        // refresh network interface usage
        self.network.refresh(true);

        let pid = self.pid.or_else(|| sysinfo::get_current_pid().ok());

        let sample_interval = self.last_measurement_time.elapsed();
        self.last_measurement_time = Instant::now();
        let total_duration = self.start_measurement_time.elapsed();

        if let Some(pid) = pid {
            self.last_sample = Metrics::create(
                &self.system,
                &self.network,
                pid,
                sample_interval,
                total_duration,
                self.last_sample.take(),
                &self.baseline_sample,
            );
        }

        match &self.last_sample {
            Some(sample) => self.output_report(sample, output_path_tmpl),
            None => Ok(()),
        }
    }

    fn output_report(&self, sample: &Metrics, output_path_tmpl: &Option<String>) -> Result<()> {
        let json_report = sample.to_json()?;

        if let Some(path_tmpl) = output_path_tmpl {
            let path = path_tmpl.replace("#PID#", &sample.pid.to_string());
            let mut file = OpenOptions::new().create(true).append(true).open(path)?;
            writeln!(file, "{}", json_report)?;
        } else {
            error!("{}", json_report);
        }

        Ok(())
    }
}

/// Errors that can occur during system monitoring.
#[derive(Error, Debug)]
pub enum SystemMonitorError {
    #[error("Failed to get pid")]
    NoPid,

    #[error("Failed to get process from pid {0}")]
    NoProcess(u32),

    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Serde Json error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

type Result<T> = std::result::Result<T, SystemMonitorError>;

impl SystemMonitor {
    /// Creates a new SystemMonitor that follows the current process.
    ///
    /// # Arguments
    /// * `sample_interval` - The interval at which to sample system metrics.
    /// * `output_path` - Optional path template for the output log file. If None, logs to tracing error.
    ///   If the string contains "#PID#", it will be replaced with the monitored process ID.
    ///
    /// # Errors
    /// Returns an error if the current process ID cannot be determined.
    pub fn follow_process(sample_interval: Duration, output_path: Option<String>) -> Result<Self> {
        sysinfo::get_current_pid().map_err(|_| SystemMonitorError::NoPid)?;
        Self::new_impl(None, sample_interval, output_path)
    }

    /// Creates a new SystemMonitor that follows a specific process ID.
    ///
    /// # Arguments
    /// Note that this function does not check if the process ID is valid. The check is
    /// deferred until `start()` is called.
    ///
    /// * `pid` - The process ID to monitor.
    /// * `sample_interval` - The interval at which to sample system metrics.
    /// * `output_path` - Optional path template for the output log file. If None, logs to tracing error.
    ///   If the string contains "#PID#", it will be replaced with the monitored process ID.
    ///
    /// # Errors
    /// Returns an error if no active process has the specific ID.
    pub fn with_pid(pid: Pid, sample_interval: Duration, output_path: Option<String>) -> Result<Self> {
        let system =
            System::new_with_specifics(RefreshKind::nothing().with_processes(ProcessRefreshKind::everything()));
        let Some(_) = system.process(pid) else {
            return Err(SystemMonitorError::NoProcess(pid.as_u32()));
        };

        Self::new_impl(Some(pid), sample_interval, output_path)
    }

    fn new_impl(pid: Option<Pid>, sample_interval: Duration, output_path: Option<String>) -> Result<Self> {
        Ok(Self {
            pid,
            sample_interval,
            output_path,
            monitor_loop: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Starts the monitoring thread.
    ///
    /// This spawns a background thread that wakes up every `sample_interval` to collect metrics
    /// and write them to the output.
    ///
    /// # Errors
    /// Returns an error if the process to monitor cannot be found, or if there's an
    /// internal error starting the monitoring thread.
    pub fn start(&self) -> Result<()> {
        let mut sampler = SystemSampler::new(self.pid)?;

        let mut inner_runner = self
            .monitor_loop
            .lock()
            .map_err(|e| SystemMonitorError::Internal(e.to_string()))?;
        self.stop.store(false, Ordering::Relaxed);

        let sample_interval = self.sample_interval;
        let output_path = self.output_path.clone();
        let stop_clone = self.stop.clone();

        *inner_runner = Some(std::thread::spawn(move || {
            loop {
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(sample_interval);
                sampler.sample(&output_path)?;
            }
            Ok(())
        }));

        Ok(())
    }

    /// Stops the monitoring thread.
    ///
    /// Signals the background thread to stop and waits for it to join.
    ///
    /// # Errors
    /// Returns an error if there is an issue stopping the thread, such as if the thread
    /// panicked or if there are internal synchronization issues.
    pub fn stop(&self) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);

        if let Some(inner_runner) = self
            .monitor_loop
            .lock()
            .map_err(|e| SystemMonitorError::Internal(e.to_string()))?
            .take()
        {
            inner_runner
                .join()
                .map_err(|_| SystemMonitorError::Internal("join error".to_owned()))??;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::os::unix::fs::MetadataExt;
    use std::time::Duration;

    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;

    #[test]
    #[serial(monitor_process)]
    fn test_monitor_self_templated_output_path() -> Result<()> {
        // Verifies that the system monitor correctly fills the output path template
        // with the id of this process.

        let tempdir = tempdir()?;
        let tempdir_path = tempdir.path();
        let log_path_template = tempdir_path.join("system_monitor_#PID#.txt").to_str().unwrap().to_owned();
        let sample_interval = Duration::from_secs(1);
        let monitor = SystemMonitor::follow_process(sample_interval, Some(log_path_template.clone()))?;
        monitor.start()?;

        // wait for the system monitor to run a while
        std::thread::sleep(Duration::from_secs(5));
        monitor.stop()?;

        // check the monitor file exists
        let pid = sysinfo::get_current_pid().unwrap();
        let log_path = log_path_template.replace("#PID#", &pid.to_string());
        assert!(std::fs::exists(&log_path)?);

        let log_reader = BufReader::new(File::open(log_path)?);
        assert!(log_reader.lines().count() >= 1);

        Ok(())
    }

    #[test]
    #[serial(monitor_process)]
    fn test_monitor_self_disk_usage() -> Result<()> {
        // Verifies that the system monitor correctly tracks and reports disk usage of this process

        let tempdir = tempdir()?;
        let tempdir_path = tempdir.path();
        let log_path = tempdir_path.join("system_monitor.txt").to_str().unwrap().to_owned();
        let sample_interval = Duration::from_secs(1);
        let monitor = SystemMonitor::follow_process(sample_interval, Some(log_path.clone()))?;
        monitor.start()?;

        // produce some disk usage
        let data_file = tempdir_path.join("data");
        let total_written_bytes = {
            let buffer = vec![0; 1024 * 1024]; // 1MiB
            let mut fd = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&data_file)?;

            for _ in 0..10 {
                fd.write_all(&buffer)?;
                std::thread::sleep(Duration::from_secs(1));
            }
            fd.flush()?;

            10 * 1024 * 1024 // 10MiB
        };

        // wait for the last sample and abort monitor
        std::thread::sleep(Duration::from_secs(5));
        monitor.stop()?;

        // check monitor logs
        let filesize = std::fs::metadata(data_file)?.size();
        assert_eq!(filesize, total_written_bytes);

        let log_reader = BufReader::new(File::open(log_path)?);
        let last_message = log_reader.lines().last().unwrap()?;
        let metrics: Metrics = serde_json::from_str(&last_message)?;

        // The total_written_bytes should be at least the size of the file created by this process.
        assert!(metrics.disk.total_written_bytes >= total_written_bytes);

        Ok(())
    }

    #[test]
    #[serial(monitor_process)]
    fn test_monitor_self_memory_usage() -> Result<()> {
        // Verifies that the system monitor correctly tracks and reports peak memory usage.
        let tempdir = tempdir()?;
        let tempdir_path = tempdir.path();
        let log_path = tempdir_path.join("system_monitor.txt").to_str().unwrap().to_owned();
        let sample_interval = Duration::from_millis(500);
        let monitor = SystemMonitor::follow_process(sample_interval, Some(log_path.clone()))?;
        monitor.start()?;

        let peak_allocation_size = 512 * 1024 * 1024; // 512 MiB

        // Allocate a large chunk of memory.
        let mut large_vec = vec![0u8; peak_allocation_size];
        // Touch each Page to commit usage.
        for i in 0..peak_allocation_size / (4 * 1024) {
            large_vec[i * 4 * 1024] = 1;
        }

        // Wait for a sample to be taken while memory usage is high.
        std::thread::sleep(Duration::from_secs(2));

        // Drop the large allocation.
        drop(large_vec);

        // Wait for more samples after memory has been released.
        std::thread::sleep(Duration::from_secs(2));

        monitor.stop()?;

        // Check monitor logs.
        let log_reader = BufReader::new(File::open(log_path)?);
        let last_message = log_reader.lines().last().unwrap()?;
        let metrics: Metrics = serde_json::from_str(&last_message)?;

        // The peak memory usage should be at least the size of our large allocation.
        assert!(metrics.memory.peak_used_bytes >= peak_allocation_size as u64);

        Ok(())
    }

    #[test]
    #[serial(monitor_process)]
    fn test_monitor_nonexist_process() -> Result<()> {
        // Verifies that the system monitor fails to initiate if targeted at an invalid pid

        let maybe_monitor = SystemMonitor::with_pid(Pid::from_u32(u32::MAX), Duration::from_secs(5), None);
        assert!(maybe_monitor.is_err());

        Ok(())
    }
}
