//! Fails a download stream whose throughput stays below a minimum, like curl's `--speed-limit`.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::future::Either;
use futures::{Stream, StreamExt};
use tokio::time::{Instant, Sleep};

struct LowSpeedState<S> {
    stream: S,
    aborted: Arc<AtomicBool>,
    /// End of the current window.
    window_end: Pin<Box<Sleep>>,
    window_bytes: u64,
}

/// Wraps `stream` so that it fails with [`io::ErrorKind::TimedOut`] once fewer than `min_bytes_per_sec`
/// bytes per second arrive over a `window`, and sets `aborted`. If `aborted` is already set, the stream is
/// returned unchanged, so sharing one flag across the attempts of a request aborts at most one of them.
/// A zero limit or window disables the check, and so does wasm, which has no tokio timer.
///
/// The read timeout only fires when no data arrives at all, so a connection that keeps receiving a trickle
/// of bytes would otherwise hold its request open indefinitely. Failing the stream lets the caller retry
/// on a new connection. A link that is slow for every request then costs one restart, not a failure.
pub(crate) fn with_low_speed_limit<S>(
    stream: S,
    min_bytes_per_sec: u64,
    window: Duration,
    aborted: Arc<AtomicBool>,
) -> impl Stream<Item = io::Result<Bytes>> + Unpin
where
    S: Stream<Item = io::Result<Bytes>> + Unpin,
{
    if cfg!(target_family = "wasm") || min_bytes_per_sec == 0 || window.is_zero() || aborted.load(Ordering::Relaxed) {
        return Either::Left(stream);
    }

    let min_window_bytes = (min_bytes_per_sec as f64 * window.as_secs_f64()) as u64;
    let state = LowSpeedState {
        stream,
        aborted,
        window_end: Box::pin(tokio::time::sleep(window)),
        window_bytes: 0,
    };

    Either::Right(Box::pin(futures::stream::unfold(Some(state), move |state| async move {
        let mut state = state?;
        loop {
            tokio::select! {
                biased;

                item = state.stream.next() => {
                    if let Some(Ok(bytes)) = &item {
                        state.window_bytes += bytes.len() as u64;
                    }
                    return item.map(|item| (item, Some(state)));
                },
                () = &mut state.window_end => {
                    if state.window_bytes < min_window_bytes {
                        tracing::warn!(
                            received_bytes = state.window_bytes,
                            window = ?window,
                            min_bytes_per_sec,
                            "Download below the minimum speed, aborting the request"
                        );
                        let message = format!(
                            "received {} bytes in {window:?}, below the minimum of {min_bytes_per_sec} bytes/s",
                            state.window_bytes
                        );
                        state.aborted.store(true, Ordering::Relaxed);
                        return Some((Err(io::Error::new(io::ErrorKind::TimedOut, message)), None));
                    }
                    state.window_end.as_mut().reset(Instant::now() + window);
                    state.window_bytes = 0;
                },
            }
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(10);

    /// A stream that yields `chunk_len` bytes every `interval`, `count` times.
    fn paced_stream(
        chunk_len: usize,
        interval: Duration,
        count: usize,
    ) -> impl Stream<Item = io::Result<Bytes>> + Unpin {
        Box::pin(futures::stream::iter(0..count).then(move |_| async move {
            tokio::time::sleep(interval).await;
            Ok(Bytes::from(vec![0u8; chunk_len]))
        }))
    }

    async fn collect(stream: impl Stream<Item = io::Result<Bytes>>) -> io::Result<usize> {
        let mut total = 0;
        let mut stream = std::pin::pin!(stream);
        while let Some(item) = stream.next().await {
            total += item?.len();
        }
        Ok(total)
    }

    #[tokio::test(start_paused = true)]
    async fn test_steady_stream_above_limit_completes() {
        // 2000 bytes/s for 60s against a 1000 bytes/s limit.
        let stream = paced_stream(200, Duration::from_millis(100), 600);
        assert_eq!(
            collect(with_low_speed_limit(stream, 1000, WINDOW, Arc::default()))
                .await
                .unwrap(),
            120_000
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_that_slows_down_fails() {
        // Fast for 25s, then a trickle: fails at the end of the first window that falls below the limit,
        // which is the window starting at 30s.
        let start = Instant::now();
        let fast = paced_stream(1000, Duration::from_millis(100), 250);
        let slow = paced_stream(1, Duration::from_millis(100), 10_000);
        let stream = fast.chain(slow);
        let err = collect(with_low_speed_limit(stream, 1000, WINDOW, Arc::default()))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(start.elapsed(), 4 * WINDOW);
    }

    #[tokio::test(start_paused = true)]
    async fn test_silent_stream_fails() {
        let start = Instant::now();
        let stream = futures::stream::pending::<io::Result<Bytes>>();
        let err = collect(with_low_speed_limit(stream, 1000, WINDOW, Arc::default()))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(start.elapsed(), WINDOW);
    }

    #[tokio::test(start_paused = true)]
    async fn test_aborts_at_most_once_per_flag() {
        let aborted = Arc::new(AtomicBool::new(false));
        let stream = paced_stream(10, Duration::from_millis(100), 300);
        let err = collect(with_low_speed_limit(stream, 1000, WINDOW, aborted.clone()))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(aborted.load(Ordering::Relaxed));

        let stream = paced_stream(10, Duration::from_millis(100), 300);
        assert_eq!(collect(with_low_speed_limit(stream, 1000, WINDOW, aborted)).await.unwrap(), 3000);
    }

    #[tokio::test(start_paused = true)]
    async fn test_disabled_limit_passes_trickle_through() {
        let stream = paced_stream(1, Duration::from_secs(1), 30);
        assert_eq!(collect(with_low_speed_limit(stream, 0, WINDOW, Arc::default())).await.unwrap(), 30);
        let stream = paced_stream(1, Duration::from_secs(1), 30);
        assert_eq!(
            collect(with_low_speed_limit(stream, 1000, Duration::ZERO, Arc::default()))
                .await
                .unwrap(),
            30
        );
    }
}
