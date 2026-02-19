use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use more_asserts::debug_assert_le;

/// Progress callback receiving (delta, completed, total) in bytes.
pub type ProgressCallback = Arc<dyn Fn(u64, u64, u64) + Send + Sync + 'static>;

/// Helper for progress reporting with reset/resume semantics. When a stream is
/// cloned or recreated (e.g. on retry), progress is only reported for bytes
/// beyond the previous high-water mark, avoiding double-counting.
///
/// Use the builder pattern: `StreamProgressReporter::new(transfer_total_bytes)`
/// `.with_progress_callback(callback)` for user progress and/or
/// `.with_adaptive_concurrency_reporter(permit.get_partial_completion_reporting_function())`.
/// Both callbacks receive (delta, completed, total) in transfer/stream bytes.
#[derive(Clone)]
pub struct StreamProgressReporter {
    /// Total size in transfer/stream bytes.
    total_if_known: u64,
    bytes_reported: Arc<AtomicUsize>,
    progress_callback: Option<ProgressCallback>,
    adaptive_concurrency_reporter: Option<ProgressCallback>,
}

impl StreamProgressReporter {
    /// Creates a reporter for a transfer of `transfer_total_bytes` (stream/transfer units).
    /// Use `0` when total size is unknown (e.g. download).
    pub fn new(total_if_known: u64) -> Self {
        Self {
            total_if_known,
            bytes_reported: Arc::new(AtomicUsize::new(0)),
            progress_callback: None,
            adaptive_concurrency_reporter: None,
        }
    }

    /// User progress callback; receives (delta, completed, total) in transfer/stream bytes.
    pub fn with_progress_callback(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Connection permit reporter; receives (delta, completed, total). Pass the result of
    /// [crate::adaptive_concurrency::ConnectionPermit::get_partial_completion_reporting_function].
    pub fn with_adaptive_concurrency_reporter(mut self, reporter: ProgressCallback) -> Self {
        self.adaptive_concurrency_reporter = Some(reporter);
        self
    }

    /// Reports progress only if `new_completed` exceeds the previous high-water
    /// mark. Invokes both callbacks with (delta, completed, total).
    pub fn report_progress(&self, new_completed: usize) {
        let old_completed = self
            .bytes_reported
            .fetch_max(new_completed, std::sync::atomic::Ordering::Relaxed);

        if old_completed >= new_completed {
            return;
        }

        let delta = (new_completed - old_completed) as u64;
        let completed = new_completed as u64;
        let total = self.total_if_known.max(completed);

        if let Some(cb) = self.progress_callback.as_ref() {
            cb(delta, completed, total);
        }
        if let Some(cb) = self.adaptive_concurrency_reporter.as_ref() {
            cb(delta, completed, total);
        }
    }
}

pub struct UploadProgressStream {
    data: Bytes,
    block_size: usize,
    bytes_sent: usize,
    reporter: StreamProgressReporter,
}

impl Stream for UploadProgressStream {
    type Item = std::result::Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        debug_assert_le!(self.bytes_sent, self.data.len());

        if self.bytes_sent == self.data.len() {
            return Poll::Ready(None);
        }

        if self.bytes_sent != 0 {
            self.reporter.report_progress(self.bytes_sent);
        }

        let slice_start = self.bytes_sent;
        let slice_end = (self.bytes_sent + self.block_size).min(self.data.len());

        self.bytes_sent = slice_end;

        Poll::Ready(Some(Ok(self.data.slice(slice_start..slice_end))))
    }
}

impl UploadProgressStream {
    pub fn new(data: impl Into<Bytes>, block_size: usize) -> Self {
        let data = data.into();
        let total = data.len() as u64;
        Self::wrap_bytes_as_stream(data, block_size, StreamProgressReporter::new(total))
    }

    /// Wraps `data` as a stream that yields chunks of size at most `block_size`,
    /// reporting progress via `reporter`. Create the reporter outside the retry loop
    /// and pass `reporter.clone()` on each attempt so progress is not double-counted.
    pub fn wrap_bytes_as_stream(data: impl Into<Bytes>, block_size: usize, reporter: StreamProgressReporter) -> Self {
        Self {
            data: data.into(),
            block_size,
            bytes_sent: 0,
            reporter,
        }
    }
}

/// Wraps a download stream and reports progress (bytes received / transfer bytes) with the same
/// reset/resume semantics as [UploadProgressStream]. Use by wrapping the response body stream.
/// On retry, create a new [DownloadProgressStream] with the new response stream and the same
/// reporter (via `reporter.clone()`).
pub struct DownloadProgressStream<S> {
    inner: Pin<Box<S>>,
    bytes_received: usize,
    reporter: StreamProgressReporter,
}

impl<S, B, E> DownloadProgressStream<S>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    E: Into<std::io::Error>,
{
    pub fn wrap_stream(stream: S, reporter: StreamProgressReporter) -> Self {
        Self {
            inner: Box::pin(stream),
            bytes_received: 0,
            reporter,
        }
    }
}

impl<S, B, E> Stream for DownloadProgressStream<S>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    E: Into<std::io::Error>,
{
    type Item = Result<B, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(b))) => {
                let len = b.as_ref().len();
                let self_ = self.get_mut();
                self_.bytes_received += len;
                self_.reporter.report_progress(self_.bytes_received);
                Poll::Ready(Some(Ok(b)))
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::executor::block_on;
    use futures::stream::StreamExt;

    use super::*;

    #[test]
    fn test_basic_streaming_and_progress() {
        let data = Bytes::from("abcdefghij");
        let block_size = 3;

        let progress_reported = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let progress_reported = progress_reported.clone();
            move |delta: u64, _completed: u64, _total: u64| progress_reported.lock().unwrap().push(delta)
        };

        let reporter = StreamProgressReporter::new(10).with_progress_callback(Arc::new(callback));
        let mut stream = UploadProgressStream::wrap_bytes_as_stream(data.clone(), block_size, reporter);

        let mut result: Vec<Bytes> = Vec::new();
        block_on(async {
            while let Some(chunk) = stream.next().await {
                result.push(chunk.unwrap());
            }
        });

        assert_eq!(
            result,
            vec![
                Bytes::from("abc"),
                Bytes::from("def"),
                Bytes::from("ghi"),
                Bytes::from("j"),
            ]
        );

        assert_eq!(*progress_reported.lock().unwrap(), vec![3, 3, 3]);
    }

    #[test]
    fn test_wrap_bytes_as_stream_retry_does_not_duplicate_progress() {
        let data = Bytes::from("abcdef");
        let block_size = 3;

        let progress_reported = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let progress_reported = progress_reported.clone();
            move |delta: u64, _completed: u64, _total: u64| progress_reported.lock().unwrap().push(delta)
        };
        let reporter = StreamProgressReporter::new(6).with_progress_callback(Arc::new(callback));

        let mut stream = UploadProgressStream::wrap_bytes_as_stream(data.clone(), block_size, reporter.clone());
        block_on(async {
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("abc"));
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("def"));
            assert!(stream.next().await.is_none());
        });

        let mut retry_stream = UploadProgressStream::wrap_bytes_as_stream(data.clone(), block_size, reporter.clone());
        block_on(async {
            assert_eq!(retry_stream.next().await.unwrap().unwrap(), Bytes::from("abc"));
            assert_eq!(retry_stream.next().await.unwrap().unwrap(), Bytes::from("def"));
            assert!(retry_stream.next().await.is_none());
        });

        assert_eq!(*progress_reported.lock().unwrap(), vec![3]);
    }

    #[test]
    fn test_partial_progress_reporting() {
        let data = Bytes::from("abcdef");
        let block_size = 2;

        let progress_reported = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let progress_reported = progress_reported.clone();
            move |delta: u64, _completed: u64, _total: u64| progress_reported.lock().unwrap().push(delta)
        };

        let reporter = StreamProgressReporter::new(6).with_progress_callback(Arc::new(callback));
        let mut stream = UploadProgressStream::wrap_bytes_as_stream(data.clone(), block_size, reporter);

        block_on(async {
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("ab"));
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("cd"));
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("ef"));
            assert!(stream.next().await.is_none());
        });

        assert_eq!(*progress_reported.lock().unwrap(), vec![2, 2]);
    }

    #[test]
    fn test_download_progress_stream_reports_progress() {
        let chunks: Vec<Result<Bytes, std::io::Error>> =
            vec![Ok(Bytes::from("ab")), Ok(Bytes::from("cd")), Ok(Bytes::from("ef"))];
        let inner = futures::stream::iter(chunks);
        let progress_reported = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let progress_reported = progress_reported.clone();
            move |delta: u64, completed: u64, total: u64| {
                progress_reported.lock().unwrap().push((delta, completed, total))
            }
        };
        let reporter = StreamProgressReporter::new(6).with_progress_callback(Arc::new(callback));
        let mut stream = DownloadProgressStream::wrap_stream(inner, reporter);

        let mut result: Vec<Bytes> = Vec::new();
        block_on(async {
            while let Some(chunk) = stream.next().await {
                result.push(chunk.unwrap());
            }
        });

        assert_eq!(result, vec![Bytes::from("ab"), Bytes::from("cd"), Bytes::from("ef")]);
        assert_eq!(*progress_reported.lock().unwrap(), vec![(2, 2, 6), (2, 4, 6), (2, 6, 6)]);
    }
}
