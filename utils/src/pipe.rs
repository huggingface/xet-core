use std::io;
use std::io::Error;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{ready, Stream};
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

pub struct TBD<T: AsyncWrite + Unpin + 'static + Send> {
    inner: T,
    left_to_pass: usize,
}

impl<T: AsyncWrite + Unpin + 'static + Send> TBD<T> {
    pub fn new(inner: T, pass: usize) -> Self {
        Self { inner, left_to_pass: pass }
    }
}

impl<T: AsyncWrite + Unpin + 'static + Send> AsyncWrite for TBD<T> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        if self.left_to_pass == 0 {
            return Poll::Ready(Ok(buf.len()));
        }
        let pass = if buf.len() > self.left_to_pass {
            &buf[..self.left_to_pass]
        } else {
            buf
        };
        let written = ready!(pin!(&mut self.inner).as_mut().poll_write(cx, pass))?;
        self.left_to_pass -= written;
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        pin!(&mut self.inner).as_mut().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        pin!(&mut self.inner).as_mut().poll_shutdown(cx)
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
