//! Minimal TCP proxy that enforces separate upload and download bandwidth caps.
//! Binds to an unused port and forwards to the given upstream (e.g. LocalTestServer
//! host:port). Callers (e.g. Toxiproxy) use the returned listen address as their upstream.
//!
//! Uses tokio semaphores: each chunk acquires `n` permits (and discards them), and 50ms
//! refill loops add permits at the target rates so fairness is preserved.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast};
use tokio::time::{Duration, interval};
use tracing::debug;

use super::error::{Result, SimulationError};

const BUF_SIZE: usize = 65536;
const REFILL_INTERVAL_MS: u64 = 50;

/// Copy data from `reader` to `writer`. If `limiter` is `Some`, acquires `n` permits
/// (consumed, not returned) before writing each chunk.
async fn copy_with_limit<R, W>(mut reader: R, mut writer: W, limiter: Option<Arc<Semaphore>>) -> std::io::Result<u64>
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
        }
        writer.write_all(&buf[..n]).await?;
        total += n as u64;
    }
    Ok(total)
}

/// Guard that stops the proxy and refill loops when dropped.
pub struct BandwidthLimitProxyGuard {
    shutdown_tx: Option<broadcast::Sender<()>>,
    #[allow(dead_code)]
    proxy_join: tokio::task::JoinHandle<()>,
}

impl Drop for BandwidthLimitProxyGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Starts a TCP proxy that forwards to `upstream` (server host:port, e.g. LocalTestServer)
/// and enforces separate upload and download bandwidth. Binds to an unused port on 127.0.0.1
/// and returns the listen address (host:port) for callers to use (e.g. as Toxiproxy upstream).
/// Drop the guard to stop the proxy.
///
/// * `upstream`: server address (host:port); no http:// prefix.
/// * `upload_bandwidth_bytes_per_sec`: cap for client→server; 0 = no limit.
/// * `download_bandwidth_bytes_per_sec`: cap for server→client; 0 = no limit.
pub async fn start_bandwidth_limit_proxy(
    upstream: &str,
    upload_bandwidth_bytes_per_sec: u64,
    download_bandwidth_bytes_per_sec: u64,
) -> Result<(String, BandwidthLimitProxyGuard)> {
    let upstream = upstream.to_string();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| SimulationError::Toxiproxy(e.to_string()))?;
    let listen_addr = listener.local_addr().map_err(|e| SimulationError::Toxiproxy(e.to_string()))?;
    let listen_str = format!("127.0.0.1:{}", listen_addr.port());

    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_rx_proxy = shutdown_tx.subscribe();

    let upload_limiter = if upload_bandwidth_bytes_per_sec > 0 {
        let limiter = Arc::new(Semaphore::new(0));
        let rx = shutdown_tx.subscribe();
        tokio::spawn(refill_loop(limiter.clone(), permits_per_interval(upload_bandwidth_bytes_per_sec), rx));
        Some(limiter)
    } else {
        None
    };

    let download_limiter = if download_bandwidth_bytes_per_sec > 0 {
        let limiter = Arc::new(Semaphore::new(0));
        let rx = shutdown_tx.subscribe();
        tokio::spawn(refill_loop(limiter.clone(), permits_per_interval(download_bandwidth_bytes_per_sec), rx));
        Some(limiter)
    } else {
        None
    };

    let proxy_join = tokio::spawn(async move {
        run_proxy_loop(listener, upstream, upload_limiter, download_limiter, shutdown_rx_proxy).await;
    });

    Ok((
        listen_str,
        BandwidthLimitProxyGuard {
            shutdown_tx: Some(shutdown_tx),
            proxy_join,
        },
    ))
}

fn permits_per_interval(bandwidth_bytes_per_sec: u64) -> usize {
    (bandwidth_bytes_per_sec / (1000 / REFILL_INTERVAL_MS)).max(1) as usize
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
                let upstream = upstream.clone();
                let ul = upload_limiter.clone();
                let dl = download_limiter.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(client_stream, &upstream, ul, dl).await {
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
) -> std::io::Result<()> {
    let upstream_stream = TcpStream::connect(upstream).await?;
    let (client_read, mut client_write) = client.into_split();
    let (upstream_read, upstream_write) = upstream_stream.into_split();

    let to_upstream = copy_with_limit(client_read, upstream_write, upload_limiter);
    let from_upstream = copy_with_limit(upstream_read, &mut client_write, download_limiter);

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
