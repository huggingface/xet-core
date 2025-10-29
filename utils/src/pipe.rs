use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::io::StreamReader;

pub fn pipe(buffer_size: usize) -> (ChannelWriter, ChannelStream) {
    let (sender, receiver) = mpsc::channel(buffer_size);
    (ChannelWriter::new(sender), ChannelStream::new(receiver))
}

/// Adapter that implements AsyncRead from an mpsc Receiver
pub struct ChannelStream(mpsc::Receiver<io::Result<Bytes>>);

impl ChannelStream {
    fn new(rx: mpsc::Receiver<io::Result<Bytes>>) -> Self {
        Self(rx)
    }

    pub fn reader(self) -> ChannelReader {
        ChannelReader::new(self)
    }
}

impl Stream for ChannelStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

type ChannelReader = StreamReader<ChannelStream, Bytes>;

/// Adapter that implements AsyncWrite from a mpsc Sender
pub struct ChannelWriter(mpsc::Sender<io::Result<Bytes>>);

impl ChannelWriter {
    fn new(tx: mpsc::Sender<io::Result<Bytes>>) -> Self {
        Self(tx)
    }
}

impl AsyncWrite for ChannelWriter {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let perm = match self.0.try_reserve() {
            Ok(p) => p,
            Err(TrySendError::Closed(_)) => {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "receiver closed")));
            },
            Err(TrySendError::Full(_)) => return Poll::Pending,
        };

        let data = Bytes::copy_from_slice(buf);
        let len = data.len();
        perm.send(Ok(data));

        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // mpsc channels don't buffer in the same way, so flush is a no-op
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Dropping the sender will close the channel
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn test_channel_read_write() {
        let (mut writer, stream) = pipe(10);
        let mut reader = stream.reader();

        // Write some data
        writer.write_all(b"Hello, ").await.unwrap();
        writer.write_all(b"World!").await.unwrap();

        // Drop writer to signal EOF
        drop(writer);

        // Read the data
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"Hello, World!");
    }
}
