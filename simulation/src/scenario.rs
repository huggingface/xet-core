use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cas_client::simulation::NetworkProfileOptions;
use cas_client::simulation::local_server::ServerLatencyProfile;
use cas_client::{LocalTestServer, LocalTestServerBuilder, build_http_client};
use reqwest_middleware::Error as ReqwestMiddlewareError;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::upload_concurrency::generate_timeline_csv;

pub const VALID_SCENARIOS: &[&str] = &["sanity_check", "single_upload", "gitxet_upload_burst", "added_uploads"];

pub const MIN_DATA_KB: u64 = 49152;
pub const MAX_DATA_KB: u64 = 65536;

const PING_PATH: &str = "/simulation/ping";
const SERVER_READY_POLL_INTERVAL_MS: u64 = 100;
const SERVER_READY_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, thiserror::Error)]
pub enum ScenarioError {
    #[error("Unknown scenario: {name}. Valid: {valid:?}")]
    UnknownScenario {
        name: String,
        valid: &'static [&'static str],
    },
    #[error("Client error: {0}")]
    Client(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Scenario error: {0}")]
    Scenario(String),
    #[error(transparent)]
    CasClient(#[from] cas_client::CasClientError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

pub type ScenarioResult<T> = Result<T, ScenarioError>;

/// One sample of network stats (recorded at the scenario's sampling interval).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStats {
    pub timestamp: String,
    pub latency_ms: f64,
    pub bandwidth_bytes_per_sec: u64,
    pub congestion_mode: Option<String>,
    pub interface: Option<String>,
    /// Elapsed seconds since scenario start.
    #[serde(default)]
    pub elapsed_sec: f64,
    /// Total upload bytes possible since proxy start (0 if no proxy).
    #[serde(default)]
    pub total_upload_possible: u64,
    /// Server latency profile name (e.g. "realistic") when present.
    #[serde(default)]
    pub server_latency_profile: Option<String>,
}

const DEFAULT_NETWORK_SAMPLING_INTERVAL_MS: u64 = 250;

/// Builder for constructing and starting a [`SimulationScenario`].
///
/// Network profile is built from `NetworkProfileOptions` in `start()`; pass-through methods configure it.
#[derive(Default)]
pub struct SimulationScenarioBuilder {
    out_dir: Option<PathBuf>,
    latency_profile: Option<ServerLatencyProfile>,
    server_latency_profile_name: Option<String>,
    network_profile_options: Option<NetworkProfileOptions>,
    network_sampling_interval: Option<Duration>,
}

impl SimulationScenarioBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_out_dir(mut self, out_dir: impl Into<PathBuf>) -> Self {
        self.out_dir = Some(out_dir.into());
        self
    }

    pub fn with_latency_profile(mut self, profile: ServerLatencyProfile) -> Self {
        self.latency_profile = Some(profile);
        self
    }

    /// Name for the server latency profile (e.g. "realistic"), used when writing network_stats.json.
    pub fn with_server_latency_profile_name(mut self, name: impl Into<String>) -> Self {
        self.server_latency_profile_name = Some(name.into());
        self
    }

    pub fn with_bandwidth_str(mut self, s: &str) -> ScenarioResult<Self> {
        let opts = self.network_profile_options.take().unwrap_or_default();
        self.network_profile_options = Some(opts.with_bandwidth_str(s).map_err(ScenarioError::from)?);
        Ok(self)
    }

    pub fn with_latency(mut self, latency: std::time::Duration, jitter: std::time::Duration) -> Self {
        let opts = self.network_profile_options.take().unwrap_or_default();
        self.network_profile_options = Some(opts.with_latency(latency, jitter));
        self
    }

    pub fn with_congestion(mut self, name: &str) -> ScenarioResult<Self> {
        let opts = self.network_profile_options.take().unwrap_or_default();
        self.network_profile_options = Some(opts.with_congestion(name).map_err(ScenarioError::from)?);
        Ok(self)
    }

    pub fn with_congestion_strength(mut self, strength: f64) -> Self {
        let opts = self.network_profile_options.take().unwrap_or_default();
        self.network_profile_options = Some(opts.with_congestion_strength(strength));
        self
    }

    /// Override the network stats sampling interval (default 250ms).
    pub fn with_network_sampling_interval(mut self, interval: Duration) -> Self {
        self.network_sampling_interval = Some(interval);
        self
    }

    /// Starts the server and returns a running scenario. Requires out_dir and latency_profile.
    /// Builds the network profile from stored options and spawns a background task that records network stats at the
    /// sampling interval (default 250ms).
    pub async fn start(self) -> ScenarioResult<SimulationScenario> {
        let out_dir = self
            .out_dir
            .ok_or_else(|| ScenarioError::Scenario("missing out_dir".to_string()))?;
        let latency_profile = self
            .latency_profile
            .ok_or_else(|| ScenarioError::Scenario("missing latency_profile".to_string()))?;

        let mut server_builder = LocalTestServerBuilder::new()
            .with_ephemeral_disk()
            .with_server_latency_profile(latency_profile);
        if let Some(ref opts) = self.network_profile_options {
            server_builder = server_builder.with_network_profile(opts.clone().build());
        }
        let server = Arc::new(server_builder.start().await);
        let start_instant = Instant::now();
        let parent_token = CancellationToken::new();
        let task_handles: Vec<JoinHandle<ScenarioResult<()>>> = Vec::new();
        let network_stats: Arc<Mutex<Vec<NetworkStats>>> = Arc::new(Mutex::new(Vec::new()));

        let mut scenario = SimulationScenario {
            server: Arc::clone(&server),
            out_dir: out_dir.clone(),
            server_latency_profile_name: self.server_latency_profile_name.clone(),
            parent_token: parent_token.clone(),
            task_handles,
            start_instant,
            network_stats: Arc::clone(&network_stats),
        };
        scenario.wait_until_ready().await?;

        let server_stats = Arc::clone(&server);
        let profile_name = scenario.server_latency_profile_name.clone();
        let start_instant_stats = scenario.start_instant;
        let sampling_interval = self
            .network_sampling_interval
            .unwrap_or(Duration::from_millis(DEFAULT_NETWORK_SAMPLING_INTERVAL_MS));
        scenario.add_task(move |child_token| {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(sampling_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let sample = sample_network_stats(&server_stats, start_instant_stats, &profile_name).await;
                            if let Ok(mut guard) = network_stats.lock() {
                                guard.push(sample);
                            }
                        }
                        _ = child_token.cancelled() => break
                    }
                }
                Ok(())
            })
        });

        Ok(scenario)
    }
}

/// A running simulation scenario that manages a test server, background tasks, and network stats collection.
///
/// Created via [`SimulationScenarioBuilder::start`]. Call [`finish`](Self::finish) to cancel tasks,
/// write output files, and shut down the server.
pub struct SimulationScenario {
    server: Arc<LocalTestServer>,
    out_dir: PathBuf,
    server_latency_profile_name: Option<String>,
    parent_token: CancellationToken,
    task_handles: Vec<JoinHandle<ScenarioResult<()>>>,
    start_instant: Instant,
    network_stats: Arc<Mutex<Vec<NetworkStats>>>,
}

/// Samples current network stats from the server.
async fn sample_network_stats(
    server: &LocalTestServer,
    start_instant: Instant,
    profile_name: &Option<String>,
) -> NetworkStats {
    NetworkStats {
        timestamp: chrono::Utc::now().timestamp_millis().to_string(),
        latency_ms: server.current_latency_ms().await,
        bandwidth_bytes_per_sec: server.current_bandwidth().await.unwrap_or(0),
        congestion_mode: None,
        interface: None,
        elapsed_sec: start_instant.elapsed().as_secs_f64(),
        total_upload_possible: server.total_upload_bytes_possible(),
        server_latency_profile: profile_name.clone(),
    }
}

/// Normalizes a server address to a base URL (e.g. "127.0.0.1:8080" -> "http://127.0.0.1:8080").
pub(crate) fn base_url(server_addr: &str) -> String {
    let s = server_addr.trim();
    if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("http://{}", s)
    }
}

impl SimulationScenario {
    /// Waits for the server (and proxy if present) to be ready by GETting the ping endpoint every 100ms.
    /// Errors if not ready within 30s.
    async fn wait_until_ready(&self) -> ScenarioResult<()> {
        let http_client =
            build_http_client("simulation_scenario", None, None).map_err(|e| ScenarioError::Scenario(e.to_string()))?;
        let server_addr = self.server.http_endpoint();
        let ping_url = format!("{}{}", base_url(server_addr).trim_end_matches('/'), PING_PATH);
        let timeout = Duration::from_secs(SERVER_READY_TIMEOUT_SECS);
        let poll_interval = Duration::from_millis(SERVER_READY_POLL_INTERVAL_MS);
        let start = Instant::now();

        loop {
            match http_client.get(&ping_url).timeout(Duration::from_secs(1)).send().await {
                Ok(res) if res.status().is_success() => return Ok(()),
                Ok(_) => {
                    if start.elapsed() >= timeout {
                        return Err(ScenarioError::Scenario(format!(
                            "Server at {} did not return success within {}s",
                            server_addr, SERVER_READY_TIMEOUT_SECS
                        )));
                    }
                    tokio::time::sleep(poll_interval).await;
                },
                Err(e) => {
                    let is_connect_error = match &e {
                        ReqwestMiddlewareError::Reqwest(we) => we.is_connect() || we.is_timeout(),
                        _ => false,
                    };
                    if is_connect_error {
                        if start.elapsed() >= timeout {
                            return Err(ScenarioError::Scenario(format!(
                                "Server at {} not ready within {}s",
                                server_addr, SERVER_READY_TIMEOUT_SECS
                            )));
                        }
                        tokio::time::sleep(poll_interval).await;
                    } else {
                        return Err(ScenarioError::Scenario(e.to_string()));
                    }
                },
            }
        }
    }

    /// Registers a task that receives a child cancellation token. When the parent token is cancelled (e.g. at finish),
    /// the child is cancelled; the returned token can be used to cancel only this task.
    /// Returns the child token so the caller can cancel this task before finish.
    pub fn add_task<F>(&mut self, f: F) -> CancellationToken
    where
        F: FnOnce(CancellationToken) -> JoinHandle<ScenarioResult<()>>,
    {
        let child = self.parent_token.child_token();
        let handle = f(child.clone());
        self.task_handles.push(handle);
        child
    }

    pub fn server(&self) -> &Arc<LocalTestServer> {
        &self.server
    }

    pub fn out_dir(&self) -> &Path {
        &self.out_dir
    }

    /// Returns a copy of the recorded network stats samples.
    pub fn network_stats(&self) -> Vec<NetworkStats> {
        self.network_stats.lock().unwrap().clone()
    }

    /// Writes the recorded network stats to a JSON lines file (one JSON object per line).
    pub fn write_network_stats(&self, path: impl AsRef<Path>) -> ScenarioResult<()> {
        let path = path.as_ref();
        let guard = self.network_stats.lock().unwrap();
        let mut content = String::new();
        for stat in guard.iter() {
            content.push_str(&serde_json::to_string(stat)?);
            content.push('\n');
        }
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Cancels all tasks (via parent token), waits for them, appends a final network stats sample, writes
    /// network_stats.json and timeline.csv, then drops the server.
    pub async fn finish(mut self) -> ScenarioResult<()> {
        self.parent_token.cancel();
        for handle in self.task_handles.drain(..) {
            handle.await??;
        }
        let final_sample =
            sample_network_stats(&self.server, self.start_instant, &self.server_latency_profile_name).await;
        if let Ok(mut guard) = self.network_stats.lock() {
            guard.push(final_sample);
        }
        self.write_network_stats(self.out_dir.join("network_stats.json"))?;
        generate_timeline_csv(&self.out_dir).map_err(|e| ScenarioError::Scenario(e.to_string()))?;
        drop(self.server);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cas_client::simulation::local_server::ServerLatencyProfile;

    use super::SimulationScenarioBuilder;

    /// Writes minimal client_stats so generate_timeline_csv can run (needs at least one client with valid timestamps).
    fn write_minimal_client_stats(out_dir: &std::path::Path) -> std::io::Result<()> {
        let path = out_dir.join("client_stats_0.json");
        let line1 = r#"{"client_id":0,"server_tag":"test","timestamp":"1000","elapsed_seconds":0.0,"interval_bytes":0,"total_bytes":0,"interval_sec":0.2,"interval_throughput_bps":0.0,"total_throughput_bps":0.0,"average_round_trip_time_ms":0.0,"total_server_calls":0,"total_retries":0,"total_successful_transmissions":0,"current_max_concurrency":1,"current_active_connections":0,"concurrency_controller_stats":null,"latency_model_stats":null}"#;
        let line2 = r#"{"client_id":0,"server_tag":"test","timestamp":"2000","elapsed_seconds":1.0,"interval_bytes":0,"total_bytes":0,"interval_sec":0.2,"interval_throughput_bps":0.0,"total_throughput_bps":0.0,"average_round_trip_time_ms":0.0,"total_server_calls":0,"total_retries":0,"total_successful_transmissions":0,"current_max_concurrency":1,"current_active_connections":0,"concurrency_controller_stats":null,"latency_model_stats":null}"#;
        std::fs::write(path, format!("{}\n{}\n", line1, line2))?;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_cancels_all_tasks_and_writes_output() {
        let temp = tempfile::tempdir().unwrap();
        let out_dir = temp.path().to_path_buf();
        let latency_profile = ServerLatencyProfile::from_name("none").unwrap();
        let mut scenario = SimulationScenarioBuilder::new()
            .with_out_dir(&out_dir)
            .with_latency_profile(latency_profile)
            .with_server_latency_profile_name("none")
            .with_bandwidth_str("1gbps")
            .unwrap()
            .with_latency(Duration::from_millis(20), Duration::ZERO)
            .with_congestion("none")
            .unwrap()
            .start()
            .await
            .unwrap();
        for _ in 0..3 {
            scenario.add_task(|child_token| {
                tokio::spawn(async move {
                    child_token.cancelled().await;
                    Ok(())
                })
            });
        }
        write_minimal_client_stats(&out_dir).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = scenario.finish().await;
        assert!(result.is_ok(), "finish() should succeed: {:?}", result.err());
        assert!(out_dir.join("network_stats.json").exists());
        assert!(out_dir.join("timeline.csv").exists());
    }
}
