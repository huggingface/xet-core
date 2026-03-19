use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::errors::Result;
use super::{LFSProtocolResponseEvent, ProgressResponse, to_line_delimited_json_string};

/// A progress updater that send custom transfer agent progress messages to git-lfs.
///
/// This updater ensures that these messages contain monotonic increasing progress
/// regardless of reordered/unordered underlying progress registrations due to scheduling.
///
/// The implemention handles concurrent updates correctly and is "wait-free": concurrent callers
/// to `update_bytes_so_far` don't wait for turns to use the update message channel, only the
/// first that acquires the lock gets to send a message with the latest progress.
pub struct ProgressUpdater<W: Write + Send + Sync + 'static> {
    update_channel: Arc<Mutex<W>>,
    request_oid: String,

    bytes_so_far: AtomicU64,
    bytes_last_sent: AtomicU64,
}

impl<W: Write + Send + Sync + 'static> ProgressUpdater<W> {
    pub fn new(update_channel: Arc<Mutex<W>>, request_oid: &str) -> Self {
        Self {
            update_channel,
            request_oid: request_oid.to_owned(),
            bytes_so_far: 0.into(),
            bytes_last_sent: 0.into(),
        }
    }

    pub fn update_bytes_so_far(&self, number: u64) -> Result<()> {
        let current: u64 = self.bytes_so_far.fetch_max(number, Ordering::Relaxed);

        if current < number {
            // now this is a valid update, try send only if channel not busy
            self.try_send_update_message()?;
        }

        Ok(())
    }

    fn try_send_update_message(&self) -> Result<()> {
        // check if the channel is available for sending
        let maybe_channel = self.update_channel.try_lock();

        let Ok(mut channel) = maybe_channel else {
            // channel is busy, skip this message
            return Ok(());
        };

        // get the latest bytes_so_far in case other threads updated it before this
        // thread acquires the lock
        let current = self.bytes_so_far.load(Ordering::Relaxed);
        let last = self.bytes_last_sent.load(Ordering::Relaxed);

        let bytes_since_last = current - last;
        let message = to_line_delimited_json_string(LFSProtocolResponseEvent::Progress(ProgressResponse {
            oid: self.request_oid.clone(),
            bytes_so_far: current,
            bytes_since_last,
        }))?;
        channel.write_all(message.as_bytes())?;
        self.bytes_last_sent.store(current, Ordering::Relaxed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use anyhow::Result;

    use super::ProgressUpdater;

    #[derive(Default)]
    struct MockWriter {
        writes: Vec<Vec<u8>>,
        delay: Option<Duration>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                writes: vec![],
                delay: None,
            }
        }

        fn with_delay(delay: Duration) -> Self {
            Self {
                writes: vec![],
                delay: Some(delay),
            }
        }

        fn into_inner(self) -> Vec<Vec<u8>> {
            self.writes
        }
    }

    impl Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if let Some(d) = self.delay {
                std::thread::sleep(d);
            }
            self.writes.push(buf.to_vec());
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct DummyRequest {
        oid: String,
    }

    impl DummyRequest {
        fn new(oid: &str) -> Self {
            Self { oid: oid.into() }
        }
    }

    type TransferRequest = DummyRequest;

    fn make_updater(writer: Arc<Mutex<MockWriter>>, oid: &str) -> Arc<ProgressUpdater<MockWriter>> {
        let req = TransferRequest::new(oid);
        Arc::new(ProgressUpdater::new(writer, &req.oid))
    }

    #[tokio::test]
    async fn test_basic() -> Result<()> {
        // Test basic operation that a progress message is sent correctly.

        let writer = Arc::new(Mutex::new(MockWriter::new()));
        let updater = make_updater(writer.clone(), "abc123");

        updater.update_bytes_so_far(50)?;
        drop(updater);

        let msgs = Arc::into_inner(writer).unwrap().into_inner()?.into_inner();
        assert_eq!(msgs.len(), 1, "Should send exactly one message");

        let mut received = String::from_utf8_lossy(&msgs[0]).to_string();
        let mut expected = r#"
            { "event": "progress", "oid": "abc123", "bytesSoFar": 50, "bytesSinceLast": 50 }"#
            .to_owned();

        received.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(received, expected);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_send_monotonic_increasing_updates() -> Result<()> {
        // Test that progress updates should be monotonic increasing, regardless of
        // reordering of tasks calling into the `update_bytes_so_far` function.

        let writer = Arc::new(Mutex::new(MockWriter::new()));
        let updater = make_updater(writer.clone(), "abc123");

        let u1 = updater.clone();
        let u2 = updater.clone();
        let u3 = updater.clone();

        let h1 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            u1.update_bytes_so_far(30) // delayed, should skip
        });
        let h2 = tokio::spawn(async move { u2.update_bytes_so_far(40) });
        let h3 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            u3.update_bytes_so_far(40) // duplicate, should skip
        });

        let (r1, r2, r3) = tokio::join!(h1, h2, h3);
        r1??;
        r2??;
        r3??;

        drop(updater);

        let msgs = Arc::into_inner(writer).unwrap().into_inner()?.into_inner();
        assert_eq!(msgs.len(), 1, "Only the second update should send");

        let mut received = String::from_utf8_lossy(&msgs[0]).to_string();
        let mut expected = r#"
            { "event": "progress", "oid": "abc123", "bytesSoFar": 40, "bytesSinceLast": 40 }"#
            .to_owned();

        received.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(received, expected);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_updates() -> Result<()> {
        // Test that concurrent updates don't wait for turns to send progress messages: only
        // the first one that acquires the channel will send and others will skip.

        let writer = Arc::new(Mutex::new(MockWriter::with_delay(Duration::from_secs(1))));
        let updater = make_updater(writer.clone(), "abc123");

        let u1 = updater.clone();
        let u2 = updater.clone();
        let u3 = updater.clone();

        let start = Instant::now();

        let h1 = tokio::spawn(async move { u1.update_bytes_so_far(10) });
        let h2 = tokio::spawn(async move { u2.update_bytes_so_far(20) });
        let h3 = tokio::spawn(async move { u3.update_bytes_so_far(30) });

        let (r1, r2, r3) = tokio::join!(h1, h2, h3);
        r1??;
        r2??;
        r3??;

        let duration = start.elapsed();

        drop(updater);

        let msgs = Arc::into_inner(writer).unwrap().into_inner()?.into_inner();
        assert_eq!(msgs.len(), 1);

        assert!(duration.ge(&Duration::from_secs(1)));
        assert!(duration.lt(&Duration::from_secs(2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_sends_multiple_when_contention_free() -> Result<()> {
        // Test that multiple progress messages can be sent through the channel when there is no contention.

        let writer = Arc::new(Mutex::new(MockWriter::new()));
        let updater = make_updater(writer.clone(), "abc123");

        updater.update_bytes_so_far(10)?;
        updater.update_bytes_so_far(20)?;
        updater.update_bytes_so_far(50)?;
        drop(updater);

        let msgs = Arc::into_inner(writer).unwrap().into_inner()?.into_inner();
        assert_eq!(msgs.len(), 3, "Should send all values when lock is free");
        assert!(msgs.iter().any(|m| String::from_utf8_lossy(m).contains(r#""bytesSoFar":50"#)));

        Ok(())
    }
}
