use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use more_asserts::*;

pub struct UploadProgressStream {
    data: Bytes,
    block_size: usize,

    progress_callback: Option<Arc<dyn Fn(u64, u64) + Send + Sync + Unpin + 'static>>,
    bytes_sent_already_reported: Arc<AtomicUsize>,

    /// Number of bytes that have been sent already
    bytes_sent: usize,
}

impl Stream for UploadProgressStream {
    type Item = std::result::Result<Bytes, std::io::Error>;

    // Send the next block of data; also update the
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        debug_assert_le!(self.bytes_sent, self.data.len());

        if self.bytes_sent == self.data.len() {
            return Poll::Ready(None);
        }

        // First, see if we need to send off a progress report -- we assume that when this method is called,
        // the previous data has
        // successfully completed uploading.

        if self.bytes_sent != 0 {
            self.update_progress(self.bytes_sent);
        }

        let slice_start = self.bytes_sent;
        let slice_end = (self.bytes_sent + self.block_size).min(self.data.len());

        self.bytes_sent = slice_end;

        Poll::Ready(Some(Ok(self.data.slice(slice_start..slice_end))))
    }
}

impl UploadProgressStream {
    pub fn new(data: impl Into<Bytes>, block_size: usize) -> Self {
        Self {
            data: data.into(),
            progress_callback: None,
            bytes_sent_already_reported: Arc::new(0.into()),
            block_size,
            bytes_sent: 0,
        }
    }

    pub fn new_with_progress_callback<F>(data: impl Into<Bytes>, block_size: usize, progress_callback: F) -> Self
    where
        F: Fn(u64, u64) + Send + Sync + Unpin + 'static,
    {
        Self {
            data: data.into(),
            progress_callback: Some(Arc::new(progress_callback)),
            bytes_sent_already_reported: Arc::new(0.into()),
            block_size,
            bytes_sent: 0,
        }
    }

    /// Creates a duplicate of the stream with the location tracker reset.  Progress updates are only
    /// reported after new progress is achieved
    pub fn clone_with_reset(&self) -> Self {
        Self {
            data: self.data.clone(),
            block_size: self.block_size,
            progress_callback: self.progress_callback.clone(),
            bytes_sent_already_reported: self.bytes_sent_already_reported.clone(),

            // This resets the position of the stream on clone as this is just used
            // for retries within reqwest.
            bytes_sent: 0,
        }
    }

    /// Creates a duplicate of the stream with the location tracker reset.  Progress updates are only
    /// reported after new progress is achieved
    pub fn clone_with_reset_and_new_callback<F>(&self, progress_callback: F) -> Self
    where
        F: Fn(u64, u64) + Send + Sync + Unpin + 'static,
    {
        Self {
            data: self.data.clone(),
            block_size: self.block_size,
            progress_callback: Some(Arc::new(progress_callback)),
            bytes_sent_already_reported: self.bytes_sent_already_reported.clone(),

            // This resets the position of the stream on clone as this is just used
            // for retries within reqwest.
            bytes_sent: 0,
        }
    }

    fn update_progress(&self, new_completed: usize) {
        // We strictly increment here; that way, if there's been a clone and a restart of the stream, we only
        // report new bytes sent as progress.
        let old_completed = self
            .bytes_sent_already_reported
            .fetch_max(new_completed, std::sync::atomic::Ordering::Relaxed);

        if old_completed < new_completed {
            let delta = (new_completed - old_completed) as u64;
            let total = new_completed as u64;
            if let Some(progress_callback) = self.progress_callback.as_ref() {
                progress_callback(delta, total);
            }
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
            move |delta: u64, _total: u64| progress_reported.lock().unwrap().push(delta)
        };

        let mut stream = UploadProgressStream::new_with_progress_callback(data.clone(), block_size, callback);

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
            move |delta: u64, _total: u64| progress_reported.lock().unwrap().push(delta)
        };

        let mut stream = UploadProgressStream::new_with_progress_callback(data.clone(), block_size, callback);
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
            move |delta: u64, _total: u64| progress_reported.lock().unwrap().push(delta)
        };

        let mut stream = UploadProgressStream::new_with_progress_callback(data.clone(), block_size, callback);

        block_on(async {
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("ab")); // nothing reported yet
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("cd")); // +2 reported
            assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("ef")); // +2 reported
            assert!(stream.next().await.is_none()); // last call triggers +6
        });

        assert_eq!(*progress_reported.lock().unwrap(), vec![2, 2]);
    }
}
