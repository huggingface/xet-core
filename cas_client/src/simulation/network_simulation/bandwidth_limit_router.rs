//! TCP proxy that enforces bandwidth, latency, and optional connection drop.
//!
//! Centered on [`NetworkSimulationProxy`]: one struct holds profile state, limiters,
//! and provider. Refill and accept loops take `Arc<Self>` and check a shared
//! shutdown flag. When the last `Arc<NetworkSimulationProxy>` is dropped, `Drop` sets the flag.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{Instant, interval, sleep, sleep_until};
use utils::CallbackGuard;

use super::network_profile::{NetworkConfig, NetworkProfile};
use crate::error::{CasClientError, Result};

const BUF_SIZE: usize = 65536;
const REFILL_INTERVAL_MS: u64 = 50;
const ACCEPT_POLL_MS: u64 = 50;
/// Each semaphore permit represents this many bytes (1 KiB). Permits are acquired/issued in these units to stay within
/// u32.
const BASE_BANDWIDTH_PERMIT_SIZE: u64 = 1024;

/// Depth of the latency simulation pipe — limits how many chunks can be "in flight"
/// between the bandwidth-limited reader and the latency-delayed writer.
const LATENCY_PIPE_DEPTH: usize = 16;

/// Single struct for the network simulation proxy. Methods take `Arc<Self>` for use in async tasks.
pub struct NetworkSimulationProxy {
    pub(crate) current_network_profile: Mutex<NetworkConfig>,
    pub(crate) total_upload_possible: Arc<AtomicU64>,
    pub(crate) total_download_possible: Arc<AtomicU64>,
    pub(crate) shutdown_flag: AtomicBool,
    pub(crate) network_profile_provider: NetworkProfile,
    pub(crate) start_instant: Instant,
    pub(crate) last_refill_instant: Mutex<Instant>,
    pub(crate) active_connections: AtomicU64,
    pub(crate) upload_limiter: Arc<Semaphore>,
    pub(crate) download_limiter: Arc<Semaphore>,
    pub(crate) upstream: String,
    pub(crate) listener: Mutex<Option<TcpListener>>,
}

impl fmt::Debug for NetworkSimulationProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetworkSimulationProxy")
            .field("shutdown_flag", &self.shutdown_flag.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Drop for NetworkSimulationProxy {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }
}

impl NetworkSimulationProxy {
    /// Build the proxy, bind the listener, and return `(Arc<Self>, listen_address)`.
    pub async fn new(upstream_endpoint: String, network_profile: NetworkProfile) -> Result<(Arc<Self>, String)> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(map_proxy_err)?;
        let listen_addr = listener.local_addr().map_err(map_proxy_err)?;
        let listen_str = format!("127.0.0.1:{}", listen_addr.port());

        let now = Instant::now();
        let initial = network_profile.current_config(Duration::ZERO);
        let proxy = Arc::new(Self {
            current_network_profile: Mutex::new(initial),
            total_upload_possible: Arc::new(AtomicU64::new(0)),
            total_download_possible: Arc::new(AtomicU64::new(0)),
            shutdown_flag: AtomicBool::new(false),
            network_profile_provider: network_profile,
            start_instant: now,
            last_refill_instant: Mutex::new(now),
            active_connections: AtomicU64::new(0),
            upload_limiter: Arc::new(Semaphore::new(0)),
            download_limiter: Arc::new(Semaphore::new(0)),
            upstream: upstream_endpoint,
            listener: Mutex::new(Some(listener)),
        });
        Ok((proxy, listen_str))
    }

    /// Call from a spawned task. Every refill interval: swap in current profile from provider,
    /// then add bytes (and optionally permits) based on elapsed time since last refill.
    /// Permits are skipped when available permits already exceed one second of bandwidth or
    /// when there are no active connections.
    pub fn run_refill_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(REFILL_INTERVAL_MS));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if self.shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }
                ticker.tick().await;
                let now = Instant::now();
                let elapsed_since_start = now.duration_since(self.start_instant);
                let profile = self.network_profile_provider.current_config(elapsed_since_start);
                *self.current_network_profile.lock().await = profile.clone();

                let mut last_guard = self.last_refill_instant.lock().await;
                let elapsed_since_last = now.duration_since(*last_guard);
                *last_guard = now;
                drop(last_guard);

                let bytes_per_sec = profile.bandwidth_bytes_per_sec.unwrap_or(0);
                let add_bytes = (elapsed_since_last.as_secs_f64() * bytes_per_sec as f64).round() as u64;
                self.total_upload_possible.fetch_add(add_bytes, Ordering::Relaxed);
                self.total_download_possible.fetch_add(add_bytes, Ordering::Relaxed);

                // Only add permits if there are active connections; this prevents a situation where
                // a slow network with just builds up a pool of bandwidth permits and allows a burst through
                // on the next connection.
                let current_active_connections = self.active_connections.load(Ordering::Relaxed);
                if current_active_connections > 0 {
                    let add_permits = add_bytes.div_ceil(BASE_BANDWIDTH_PERMIT_SIZE).min(usize::MAX as u64) as usize;
                    let one_second_permits =
                        (bytes_per_sec / BASE_BANDWIDTH_PERMIT_SIZE).min(usize::MAX as u64) as usize;

                    if add_permits > 0 && self.upload_limiter.available_permits() < one_second_permits {
                        self.upload_limiter.add_permits(add_permits);
                    }

                    if add_permits > 0 && self.download_limiter.available_permits() < one_second_permits {
                        self.download_limiter.add_permits(add_permits);
                    }
                }
            }
        });
    }

    /// Call from a spawned task. Accepts connections and spawns a handler for each; exits when shutdown_flag is set.
    pub async fn run_accept_loop(self: Arc<Self>) {
        let listener = {
            let mut guard = self.listener.lock().await;
            guard.take().expect("accept loop already started or listener taken")
        };
        loop {
            if self.shutdown_flag.load(Ordering::Relaxed) {
                break;
            }
            tokio::select! {
                _ = sleep(Duration::from_millis(ACCEPT_POLL_MS)) => {}
                res = listener.accept() => {
                    let (client_stream, _) = match res {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(error = %e, "bandwidth proxy accept error");
                            continue;
                        }
                    };
                    let proxy = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Err(e) = proxy.handle_connection(client_stream).await {
                            tracing::debug!(error = %e, "bandwidth proxy connection error");
                        }
                    });
                }
            }
        }
    }

    /// Handle one client connection: use current profile for latency and drop, then forward both ways.
    /// Counts as an active connection for the refill loop until the handler returns.
    pub async fn handle_connection(self: Arc<Self>, client: TcpStream) -> std::io::Result<()> {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        let me = Arc::clone(&self);
        let _active_guard = CallbackGuard::new(move || {
            me.active_connections.fetch_sub(1, Ordering::Relaxed);
        });
        let profile = self.current_network_profile.lock().await.clone();
        if profile.drop_probability > 0.0 && rand::random::<f64>() < profile.drop_probability {
            drop(client);
            return Ok(());
        }
        let upstream_stream = TcpStream::connect(self.upstream.as_str()).await?;
        let (client_read, client_write) = client.into_split();
        let (upstream_read, upstream_write) = upstream_stream.into_split();
        let latency = (profile.latency, profile.jitter);
        let upload_limiter = Arc::clone(&self.upload_limiter);
        let download_limiter = Arc::clone(&self.download_limiter);
        let to_upstream =
            tokio::spawn(copy_with_rate_and_latency(client_read, upstream_write, Some(upload_limiter), latency));
        let from_upstream =
            tokio::spawn(copy_with_rate_and_latency(upstream_read, client_write, Some(download_limiter), latency));
        let (to_res, from_res) = tokio::join!(to_upstream, from_upstream);
        to_res.map_err(std::io::Error::other)??;
        from_res.map_err(std::io::Error::other)??;
        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }

    /// Total bytes that could have been uploaded since proxy start (from refill loop).
    pub fn total_upload_bytes_possible(&self) -> u64 {
        self.total_upload_possible.load(Ordering::Relaxed)
    }

    /// Total bytes that could have been downloaded since proxy start (from refill loop).
    pub fn total_download_bytes_possible(&self) -> u64 {
        self.total_download_possible.load(Ordering::Relaxed)
    }

    /// Current bandwidth from the current network profile, in bytes per second, or `None` if unlimited.
    pub async fn current_bandwidth(&self) -> Option<u64> {
        let profile = self.current_network_profile.lock().await;
        profile.bandwidth_bytes_per_sec
    }

    /// Current base latency from the current network profile (jitter not included), in milliseconds.
    pub async fn current_latency_ms(&self) -> f64 {
        let profile = self.current_network_profile.lock().await;
        profile.latency.as_secs_f64() * 1000.0
    }
}

/// Copy data from `reader` to `writer` with bandwidth limiting and latency simulation.
///
/// Bandwidth is enforced by acquiring semaphore permits before each chunk is read.
/// When latency is non-zero, data flows through a pipeline that models a network link:
/// the reader acquires bandwidth permits and enqueues each chunk with a delivery timestamp
/// of `now + latency ± jitter`. A writer task dequeues chunks and sleeps until their
/// delivery time before forwarding. This allows multiple chunks to be "in flight"
/// simultaneously, so latency adds a fixed offset to delivery without reducing throughput.
async fn copy_with_rate_and_latency<R, W>(
    reader: R,
    writer: W,
    limiter: Option<Arc<Semaphore>>,
    latency: (Duration, Duration),
) -> std::io::Result<u64>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin,
{
    let has_latency = latency.0 > Duration::ZERO || latency.1 > Duration::ZERO;

    if !has_latency {
        return copy_bandwidth_only(reader, writer, limiter).await;
    }

    // Pipeline: reader task enqueues (delivery_time, data), current task dequeues and delivers.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Instant, Vec<u8>)>(LATENCY_PIPE_DEPTH);

    let base_latency = latency.0;
    let jitter = latency.1;

    // Spawn reader: reads chunks, acquires bandwidth permits, sends with delivery timestamp.
    let reader_handle = tokio::spawn(async move {
        let mut reader = reader;
        let mut buf = [0u8; BUF_SIZE];
        let base_secs = base_latency.as_secs_f64();
        let jitter_secs = jitter.as_secs_f64();
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            if let Some(ref lim) = limiter {
                let permits = (n as u64).div_ceil(BASE_BANDWIDTH_PERMIT_SIZE).min(u32::MAX as u64) as u32;
                let permit = lim.acquire_many(permits).await.map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::ConnectionReset, "bandwidth limiter closed")
                })?;
                permit.forget();
            }
            let delay_secs = base_secs + rand::rng().random_range(-jitter_secs..=jitter_secs);
            let delivery_time = Instant::now() + Duration::from_secs_f64(delay_secs.max(0.0));
            if tx.send((delivery_time, buf[..n].to_vec())).await.is_err() {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    });

    // Writer (current task): dequeue, sleep until delivery time, write.
    let mut total: u64 = 0;
    let mut writer = writer;
    while let Some((delivery_time, chunk)) = rx.recv().await {
        if delivery_time > Instant::now() {
            sleep_until(delivery_time).await;
        }
        writer.write_all(&chunk).await?;
        total += chunk.len() as u64;
    }

    reader_handle.await.map_err(std::io::Error::other)??;

    Ok(total)
}

/// Bandwidth-only copy without latency simulation. Used as a fast path when latency is zero.
async fn copy_bandwidth_only<R, W>(
    mut reader: R,
    mut writer: W,
    limiter: Option<Arc<Semaphore>>,
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
        if let Some(ref lim) = limiter {
            let permits = (n as u64).div_ceil(BASE_BANDWIDTH_PERMIT_SIZE).min(u32::MAX as u64) as u32;
            let permit = lim
                .acquire_many(permits)
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::ConnectionReset, "bandwidth limiter closed"))?;
            permit.forget();
        }
        writer.write_all(&buf[..n]).await?;
        total += n as u64;
    }
    Ok(total)
}

fn map_proxy_err(e: impl std::fmt::Display) -> CasClientError {
    CasClientError::Other(format!("Proxy error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::super::network_profile::NetworkProfileOptions;
    use super::*;

    #[tokio::test]
    async fn proxy_forwards_data_through_semaphore_limiter() {
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let server_addr_str = format!("127.0.0.1:{}", server_addr.port());

        let provider = NetworkProfileOptions::new().with_bandwidth_bytes_per_sec(10_000).build();
        let (proxy, listen_str) = NetworkSimulationProxy::new(server_addr_str, provider).await.expect("new proxy");
        Arc::clone(&proxy).run_refill_loop();
        tokio::spawn(async move { proxy.run_accept_loop().await });
        let proxy_addr = listen_str;

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
