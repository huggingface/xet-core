use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use more_asserts::*;
use tokio::time::Instant;
use utils::configurable_constants;

configurable_constants! {
    /// The min and max block sizes to send for the upload.  This is set dynamically
    /// to maximize bandwidth in large blocks while still having accurate reporting.
    ref UPLOAD_STREAM_MIN_BLOCK_SIZE : u64 = 512 * 1024;
    ref UPLOAD_STREAM_MAX_BLOCK_SIZE : u64 = 16 * 1024 * 1024;

    // Send a block that targets taking this long to send.  We want to maximize throughput on
    // fast connections, which means a bigger block size, while still allowing for fast
    // reporting on slow connections.
    ref UPLOAD_STREAM_ROUND_TRIP_TARGET_MS: u64 = 250;
}

struct ProgressCallbackWrapper<UpdateFunction>
where
    UpdateFunction: Fn(u64) + Send + Unpin + 'static,
{
    progress_callback: UpdateFunction,
    bytes_sent_already_reported: AtomicU64,
}

impl<UpdateFunction> ProgressCallbackWrapper<UpdateFunction>
where
    UpdateFunction: Fn(u64) + Send + Sync + Unpin + 'static,
{
    fn update(&self, new_completed: u64) {
        // We strictly increment here; that way, if there's been a clone and a restart of the stream, we only
        // report new bytes sent as progress.
        let old_completed = self.bytes_sent_already_reported.fetch_max(new_completed, Ordering::Relaxed);

        if old_completed < new_completed {
            (self.progress_callback)((new_completed - old_completed) as u64);
        }
    }
}

pub struct UploadProgressStream<UpdateFunction>
where
    UpdateFunction: Fn(u64) + Send + Sync + Unpin + 'static,
{
    data: Bytes,
    progress_callback_wrapper: Arc<ProgressCallbackWrapper<UpdateFunction>>,

    /// Number of bytes that have been sent already
    bytes_sent: u64,

    /// The start time for the stream.  This is used to set the next block size dynamically.
    start_time: tokio::time::Instant,

    /// If there is a fixed block size set, use this.
    fixed_block_size: Option<u64>,
}

impl<UpdateFunction> Stream for UploadProgressStream<UpdateFunction>
where
    UpdateFunction: Fn(u64) + Send + Sync + Unpin + 'static,
{
    type Item = std::result::Result<Bytes, std::io::Error>;

    // Send the next block of data; also update the
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        debug_assert_le!(self.bytes_sent, self.data.len() as u64);

        if self.bytes_sent == self.data.len() as u64 {
            return Poll::Ready(None);
        }

        // Set the block size dynamically.
        let block_size = {
            // Prefer the fixed block size if given (for testing).
            if let Some(block_size) = self.fixed_block_size {
                block_size
            } else {
                // Set dynamically, keeping it between bounds.
                {
                    if self.bytes_sent == 0 {
                        *UPLOAD_STREAM_MIN_BLOCK_SIZE
                    } else {
                        // Adjust the block size so that the next
                        let rt_time = self.start_time.elapsed().as_millis() as u64;
                        let current_bytes_per_ms = self.bytes_sent / rt_time.max(1);

                        // Set block size so that the next block should take around UPLOAD_STREAM_ROUND_TRIP_TARGET_MS
                        // before we get called again.
                        current_bytes_per_ms.saturating_mul(*UPLOAD_STREAM_ROUND_TRIP_TARGET_MS)
                    }
                }
                .clamp(*UPLOAD_STREAM_MIN_BLOCK_SIZE, *UPLOAD_STREAM_MAX_BLOCK_SIZE)
            }
        };

        let slice_start = self.bytes_sent as u64;
        let slice_end = (self.bytes_sent + block_size).min(self.data.len() as u64);

        // Report the previous progress before updating this value.
        self.progress_callback_wrapper.update(self.bytes_sent);

        self.bytes_sent = slice_end;

        Poll::Ready(Some(Ok(self.data.slice((slice_start as usize)..(slice_end as usize)))))
    }
}

impl<UpdateFunction> UploadProgressStream<UpdateFunction>
where
    UpdateFunction: Fn(u64) + Send + Sync + Unpin + 'static,
{
    pub fn new(data: impl Into<Bytes>, progress_callback: UpdateFunction, fixed_block_size: Option<u64>) -> Self {
        let data = data.into();

        let progress_callback_wrapper = Arc::new(ProgressCallbackWrapper {
            progress_callback,
            bytes_sent_already_reported: 0.into(),
        });

        Self {
            bytes_sent: 0,
            data,
            progress_callback_wrapper,
            start_time: Instant::now(),
            fixed_block_size,
        }
    }

    /// Creates a duplicate of the stream with the location tracker reset.  Progress updates are only
    /// reported after new progress is achieved
    pub fn clone_with_reset(&self) -> Self {
        Self {
            data: self.data.clone(),
            progress_callback_wrapper: self.progress_callback_wrapper.clone(),

            // This resets the position of the stream on clone as this is just used
            // for retries within reqwest.
            bytes_sent: 0,
            start_time: Instant::now(),
            fixed_block_size: self.fixed_block_size,
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
        let data = Bytes::from("abcdefghij"); // 10 bytes
        let block_size = 3;

        let progress_reported = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let progress_reported = progress_reported.clone();
            move |v| progress_reported.clone().lock().unwrap().push(v)
        };

        let mut stream = UploadProgressStream::new(data.clone(), callback, Some(block_size));

        let mut result = Vec::new();
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

        // Progress callback is only called *after* a chunk has been confirmed sent (on the *next* poll).
        // So it only fires for second and later chunks.
        assert_eq!(*progress_reported.lock().unwrap(), vec![3, 3, 3]);
    }

    #[test]
    fn test_clone_with_reset_does_not_duplicate_progress() {
        let data = Bytes::from("abcdef"); // 6 bytes
        let block_size = 3;

        let progress_reported = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let progress_reported = progress_reported.clone();
            move |v| {
                progress_reported.lock().unwrap().push(v);
            }
        };

        let mut stream = UploadProgressStream::new(data.clone(), callback, Some(block_size));
        block_on(async {
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("abc"));
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("def"));
            assert!(stream.next().await.is_none());
        });

        let mut cloned = stream.clone_with_reset();
        block_on(async {
            assert_eq!(cloned.next().await.unwrap().unwrap(), Bytes::from("abc"));
            assert_eq!(cloned.next().await.unwrap().unwrap(), Bytes::from("def"));
            assert!(cloned.next().await.is_none());
        });

        // The progress callback only fires after the *first* stream reports new bytes sent.
        // The cloned stream starts from zero, but those bytes were already reported, so only the new delta is recorded.
        // Since the clone sends the same total bytes as the original, and no new progress is made, nothing is reported.
        assert_eq!(*progress_reported.lock().unwrap(), vec![3]);
    }

    #[test]
    fn test_partial_progress_reporting() {
        let data = Bytes::from("abcdef"); // 6 bytes
        let block_size = 2;

        let progress_reported = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let progress_reported = progress_reported.clone();
            move |v| {
                progress_reported.lock().unwrap().push(v);
            }
        };

        let mut stream = UploadProgressStream::new(data.clone(), callback, Some(block_size));

        block_on(async {
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("ab")); // nothing reported yet
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("cd")); // +2 reported
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("ef")); // +2 reported
            assert!(stream.next().await.is_none()); // last call triggers +6
        });

        assert_eq!(*progress_reported.lock().unwrap(), vec![2, 2]);
    }
}
