use std::io::Write;

use bytes::Bytes;
use tokio::sync::mpsc;

/// Adapts an async `mpsc::Sender<Bytes>` to sync `io::Write` using `blocking_send`,
/// providing backpressure when the channel buffer is full.
pub(crate) struct ChannelWriter(mpsc::Sender<Bytes>);

impl ChannelWriter {
    pub(crate) fn new(sender: mpsc::Sender<Bytes>) -> Self {
        Self(sender)
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .blocking_send(Bytes::copy_from_slice(buf))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
