//! TCP proxy that enforces bandwidth, latency, and optional connection drop.
//! Binds to an unused port and forwards to the given upstream (e.g. LocalTestServer).
//!
//! Uses tokio semaphores for bandwidth; adds per-chunk latency (with jitter) and
//! optionally drops new connections with a configurable probability.
//! Supports a time-based profile provider: every 250ms the provider returns the current
//! NetworkProfile from elapsed time; bandwidth is recorded for baseline reporting.

use std::sync::{Arc, Mutex};

use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, Semaphore, broadcast};
use tokio::time::{Duration, interval, sleep};
use tracing::debug;

use super::error::{Result, SimulationError};
use super::network_profile::NetworkProfile;
use super::profile_provider::NetworkProfileProvider;

const BUF_SIZE: usize = 65536;
const REFILL_INTERVAL_MS: u64 = 50;
const CONFIG_POLL_INTERVAL_MS: u64 = 250;

/// Configuration for the bandwidth-limit proxy (bandwidth, latency, drop probability).
#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub upload_bandwidth_bytes_per_sec: u64,
    pub download_bandwidth_bytes_per_sec: u64,
    pub latency_ms: Option<u32>,
    pub jitter_ms: Option<u32>,
    pub drop_probability: f64,
}

impl ProxyConfig {
    /// Build from a network profile; uses profile bandwidth for both upload and download.
    pub fn from_profile(profile: &NetworkProfile) -> Self {
        let bps = profile.bandwidth_kbps.map(|k| (u64::from(k) * 1000) / 8).unwrap_or(0);
        Self {
            upload_bandwidth_bytes_per_sec: bps,
            download_bandwidth_bytes_per_sec: bps,
            latency_ms: profile.latency_ms,
            jitter_ms: profile.jitter_ms,
            drop_probability: profile.drop_probability.unwrap_or(0.0),
        }
    }
}

/// Per-chunk latency parameters (optional).
#[derive(Clone)]
struct LatencyParams {
    latency_ms: u32,
    jitter_ms: u32,
}

/// Copy data from `reader` to `writer`. If `limiter` is `Some`, acquires `n` permits
/// before writing. If `latency` is `Some`, sleeps latency ± jitter before each write.
async fn copy_with_limit<R, W>(
    mut reader: R,
    mut writer: W,
    limiter: Option<Arc<Semaphore>>,
    latency: Option<LatencyParams>,
) -> std::io::Result<u64>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = [0u8; BUF_SIZE];
    let mut total: u64 = 0;
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        if n > 0 {
            if let Some(ref lim) = limiter {
                let permit = lim.acquire_many(n as u32).await.map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::ConnectionReset, "bandwidth limiter closed")
                })?;
                permit.forget();
            }
            if let Some(ref lat) = latency {
                let jitter_range = lat.jitter_ms as i32;
                let jitter_ms = rand::rng().random_range(-jitter_range..=jitter_range);
                let delay_ms = (lat.latency_ms as i32 + jitter_ms).max(0) as u64;
                if delay_ms > 0 {
                    sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
        writer.write_all(&buf[..n]).await?;
        total += n as u64;
    }
    Ok(total)
}

/// Recorded (elapsed, bandwidth_bytes_per_sec, latency_ms) for baseline reporting.
pub type BandwidthRecording = Vec<(Duration, u64, f64)>;

/// Guard that stops the proxy and refill loops when dropped.
/// When started with a provider, holds a recording of (elapsed, bandwidth, latency) for baseline.
pub struct BandwidthLimitProxyGuard {
    shutdown_tx: Option<broadcast::Sender<()>>,
    #[allow(dead_code)]
    proxy_join: tokio::task::JoinHandle<()>,
    recording: Option<Arc<Mutex<BandwidthRecording>>>,
}

impl Drop for BandwidthLimitProxyGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Starts a TCP proxy with the given config (bandwidth, latency, drop probability).
/// Binds to an unused port on 127.0.0.1 and returns the listen address (host:port).
pub async fn start_bandwidth_limit_proxy_with_config(
    upstream: &str,
    config: &ProxyConfig,
) -> Result<(String, BandwidthLimitProxyGuard)> {
    let upstream = upstream.to_string();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| SimulationError::Proxy(e.to_string()))?;
    let listen_addr = listener.local_addr().map_err(|e| SimulationError::Proxy(e.to_string()))?;
    let listen_str = format!("127.0.0.1:{}", listen_addr.port());

    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_rx_proxy = shutdown_tx.subscribe();

    let upload_limiter = if config.upload_bandwidth_bytes_per_sec > 0 {
        let limiter = Arc::new(Semaphore::new(0));
        let rx = shutdown_tx.subscribe();
        tokio::spawn(refill_loop(limiter.clone(), permits_per_interval(config.upload_bandwidth_bytes_per_sec), rx));
        Some(limiter)
    } else {
        None
    };

    let download_limiter = if config.download_bandwidth_bytes_per_sec > 0 {
        let limiter = Arc::new(Semaphore::new(0));
        let rx = shutdown_tx.subscribe();
        tokio::spawn(refill_loop(limiter.clone(), permits_per_interval(config.download_bandwidth_bytes_per_sec), rx));
        Some(limiter)
    } else {
        None
    };

    let latency = config
        .latency_ms
        .zip(config.jitter_ms)
        .map(|(latency_ms, jitter_ms)| LatencyParams { latency_ms, jitter_ms });

    let drop_probability = config.drop_probability;
    let proxy_join = tokio::spawn(async move {
        run_proxy_loop(
            listener,
            upstream,
            upload_limiter,
            download_limiter,
            latency,
            drop_probability,
            shutdown_rx_proxy,
        )
        .await;
    });

    Ok((
        listen_str,
        BandwidthLimitProxyGuard {
            shutdown_tx: Some(shutdown_tx),
            proxy_join,
            recording: None,
        },
    ))
}

/// Starts a TCP proxy driven by a time-based profile provider.
/// Every 250ms the provider is called with elapsed time; the returned profile is applied and
/// (elapsed, bandwidth, latency) is recorded for baseline reporting. Shutdown is checked each 250ms.
pub async fn start_bandwidth_limit_proxy_with_provider(
    upstream: &str,
    provider: Arc<dyn NetworkProfileProvider + Send + Sync>,
) -> Result<(String, BandwidthLimitProxyGuard)> {
    let upstream = upstream.to_string();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| SimulationError::Proxy(e.to_string()))?;
    let listen_addr = listener.local_addr().map_err(|e| SimulationError::Proxy(e.to_string()))?;
    let listen_str = format!("127.0.0.1:{}", listen_addr.port());

    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_rx_proxy = shutdown_tx.subscribe();
    let shutdown_rx_refill = shutdown_tx.subscribe();
    let shutdown_rx_provider = shutdown_tx.subscribe();

    let initial = ProxyConfig::from_profile(&provider.profile_at(Duration::ZERO));
    let current_config = Arc::new(RwLock::new(initial));
    let upload_limiter = Arc::new(Semaphore::new(0));
    let download_limiter = Arc::new(Semaphore::new(0));
    let recording: BandwidthRecording = Vec::new();
    let recording = Arc::new(Mutex::new(recording));

    tokio::spawn(refill_loop_scheduled(
        upload_limiter.clone(),
        download_limiter.clone(),
        current_config.clone(),
        shutdown_rx_refill,
    ));
    tokio::spawn(provider_loop(provider, current_config.clone(), recording.clone(), shutdown_rx_provider));

    let proxy_join = tokio::spawn(async move {
        run_proxy_loop_scheduled(
            listener,
            upstream,
            upload_limiter,
            download_limiter,
            current_config,
            shutdown_rx_proxy,
        )
        .await;
    });

    Ok((
        listen_str,
        BandwidthLimitProxyGuard {
            shutdown_tx: Some(shutdown_tx),
            proxy_join,
            recording: Some(recording),
        },
    ))
}

impl BandwidthLimitProxyGuard {
    /// Takes the bandwidth recording (elapsed, bandwidth_bps, latency_ms) if this guard was started with a provider.
    pub fn take_bandwidth_recording(&self) -> Option<BandwidthRecording> {
        let rec = self.recording.as_ref()?;
        rec.lock().ok().map(|mut v| std::mem::take(&mut *v))
    }
}

/// Refill loop for scheduled proxy: each tick, read current config and add permits for upload and download.
async fn refill_loop_scheduled(
    upload_limiter: Arc<Semaphore>,
    download_limiter: Arc<Semaphore>,
    current_config: Arc<RwLock<ProxyConfig>>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut ticker = interval(Duration::from_millis(REFILL_INTERVAL_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("bandwidth limit scheduled refill loop shutting down");
                break;
            }
            _ = ticker.tick() => {
                let config = current_config.read().await;
                let up = permits_per_interval(config.upload_bandwidth_bytes_per_sec);
                let down = permits_per_interval(config.download_bandwidth_bytes_per_sec);
                if up > 0 {
                    upload_limiter.add_permits(up);
                }
                if down > 0 {
                    download_limiter.add_permits(down);
                }
            }
        }
    }
}

/// Provider loop: every 250ms get profile from provider, update config, record (elapsed, bandwidth, latency).
async fn provider_loop(
    provider: Arc<dyn NetworkProfileProvider + Send + Sync>,
    current_config: Arc<RwLock<ProxyConfig>>,
    recording: Arc<Mutex<BandwidthRecording>>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let start = tokio::time::Instant::now();
    {
        let profile = provider.profile_at(Duration::ZERO);
        let config = ProxyConfig::from_profile(&profile);
        let bps = config.upload_bandwidth_bytes_per_sec;
        let latency_ms = config.latency_ms.map(f64::from).unwrap_or(0.0);
        if let Ok(mut rec) = recording.lock() {
            rec.push((Duration::ZERO, bps, latency_ms));
        }
    }
    let poll_interval = Duration::from_millis(CONFIG_POLL_INTERVAL_MS);
    let mut ticker = interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("bandwidth limit provider loop shutting down");
                break;
            }
            _ = ticker.tick() => {
                let elapsed = start.elapsed();
                let profile = provider.profile_at(elapsed);
                let config = ProxyConfig::from_profile(&profile);
                let bps = config.upload_bandwidth_bytes_per_sec;
                let latency_ms = config.latency_ms.map(f64::from).unwrap_or(0.0);
                *current_config.write().await = config;
                if let Ok(mut rec) = recording.lock() {
                    rec.push((elapsed, bps, latency_ms));
                }
            }
        }
    }
}

/// Proxy accept loop that reads current config for latency and drop on each accept.
async fn run_proxy_loop_scheduled(
    listener: TcpListener,
    upstream: String,
    upload_limiter: Arc<Semaphore>,
    download_limiter: Arc<Semaphore>,
    current_config: Arc<RwLock<ProxyConfig>>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        let accept = listener.accept();
        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                debug!("bandwidth limit proxy shutting down");
                break;
            }
            res = accept => {
                let (client_stream, _) = match res {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error = %e, "bandwidth proxy accept error");
                        continue;
                    }
                };
                let config = current_config.read().await.clone();
                let drop_probability = config.drop_probability;
                let latency = config
                    .latency_ms
                    .zip(config.jitter_ms)
                    .map(|(latency_ms, jitter_ms)| LatencyParams { latency_ms, jitter_ms });
                drop(config);
                if drop_probability > 0.0 && rand::random::<f64>() < drop_probability {
                    drop(client_stream);
                    continue;
                }
                let upstream = upstream.clone();
                let ul = upload_limiter.clone();
                let dl = download_limiter.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(client_stream, &upstream, Some(ul), Some(dl), latency).await {
                        tracing::debug!(error = %e, "bandwidth proxy connection error");
                    }
                });
            }
        }
    }
}

/// Starts a TCP proxy that forwards to `upstream` and enforces separate upload and download
/// bandwidth only (no latency or drop). Convenience wrapper around `start_bandwidth_limit_proxy_with_config`.
pub async fn start_bandwidth_limit_proxy(
    upstream: &str,
    upload_bandwidth_bytes_per_sec: u64,
    download_bandwidth_bytes_per_sec: u64,
) -> Result<(String, BandwidthLimitProxyGuard)> {
    let config = ProxyConfig {
        upload_bandwidth_bytes_per_sec,
        download_bandwidth_bytes_per_sec,
        latency_ms: None,
        jitter_ms: None,
        drop_probability: 0.0,
    };
    start_bandwidth_limit_proxy_with_config(upstream, &config).await
}

fn permits_per_interval(bandwidth_bytes_per_sec: u64) -> usize {
    if bandwidth_bytes_per_sec == 0 {
        0
    } else {
        (bandwidth_bytes_per_sec / (1000 / REFILL_INTERVAL_MS)).max(1) as usize
    }
}

/// Adds permits to the semaphore every 50ms until shutdown.
async fn refill_loop(limiter: Arc<Semaphore>, permits_per_interval: usize, mut shutdown_rx: broadcast::Receiver<()>) {
    let mut ticker = interval(Duration::from_millis(REFILL_INTERVAL_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("bandwidth limit refill loop shutting down");
                break;
            }
            _ = ticker.tick() => {
                limiter.add_permits(permits_per_interval);
            }
        }
    }
}

async fn run_proxy_loop(
    listener: TcpListener,
    upstream: String,
    upload_limiter: Option<Arc<Semaphore>>,
    download_limiter: Option<Arc<Semaphore>>,
    latency: Option<LatencyParams>,
    drop_probability: f64,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        let accept = listener.accept();
        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                debug!("bandwidth limit proxy shutting down");
                break;
            }
            res = accept => {
                let (client_stream, _) = match res {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error = %e, "bandwidth proxy accept error");
                        continue;
                    }
                };
                if drop_probability > 0.0 && rand::random::<f64>() < drop_probability {
                    drop(client_stream);
                    continue;
                }
                let upstream = upstream.clone();
                let ul = upload_limiter.clone();
                let dl = download_limiter.clone();
                let lat = latency.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(client_stream, &upstream, ul, dl, lat).await {
                        tracing::debug!(error = %e, "bandwidth proxy connection error");
                    }
                });
            }
        }
    }
}

async fn handle_connection(
    client: TcpStream,
    upstream: &str,
    upload_limiter: Option<Arc<Semaphore>>,
    download_limiter: Option<Arc<Semaphore>>,
    latency: Option<LatencyParams>,
) -> std::io::Result<()> {
    let upstream_stream = TcpStream::connect(upstream).await?;
    let (client_read, mut client_write) = client.into_split();
    let (upstream_read, upstream_write) = upstream_stream.into_split();

    let to_upstream = copy_with_limit(client_read, upstream_write, upload_limiter, latency.clone());
    let from_upstream = copy_with_limit(upstream_read, &mut client_write, download_limiter, latency);

    let _ = tokio::join!(to_upstream, from_upstream);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn proxy_forwards_data_through_semaphore_limiter() {
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let server_addr_str = format!("127.0.0.1:{}", server_addr.port());

        let (proxy_addr, _guard) = start_bandwidth_limit_proxy(&server_addr_str, 10_000, 10_000)
            .await
            .expect("start proxy");
        let proxy_addr = proxy_addr.clone();

        const PAYLOAD: usize = 5000;
        tokio::spawn(async move {
            let (mut stream, _) = server.accept().await.unwrap();
            let mut buf = vec![0u8; PAYLOAD];
            let _ = stream.read(&mut buf).await;
            let _ = stream.write_all(&[1u8; PAYLOAD]).await;
        });

        let mut client = TcpStream::connect(&proxy_addr).await.unwrap();
        client.write_all(&[0u8; PAYLOAD]).await.unwrap();
        let mut buf = [0u8; PAYLOAD];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(n, PAYLOAD);
        assert!(buf.iter().all(|&b| b == 1));
    }
}
