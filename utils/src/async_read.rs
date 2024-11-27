use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::AsyncRead;

// (AsyncRead) adaptor
// wraps over an AsyncRead, copying all the contents read from the inner reader
// and buffers it in an internal buffer which can be retrieved by calling .consume()
// to return a copy of all the content that was read.
pub struct CopyReader<'a, T> {
    src: Pin<&'a mut T>,
    buf: Vec<u8>,
}

impl<'a, T: AsyncRead + Unpin> AsyncRead for CopyReader<'a, T> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let res = ready!(self.src.as_mut().poll_read(cx, buf))?;
        self.buf.extend_from_slice(&buf[..res]);
        Poll::Ready(Ok(res))
    }
}

impl<'a, T: AsyncRead + Unpin> Unpin for CopyReader<'a, T> {}

impl<'a, T: AsyncRead + Unpin> CopyReader<'a, T> {
    pub fn new(src: Pin<&'a mut T>) -> Self {
        Self { src, buf: Vec::new() }
    }

    pub fn consume(self) -> Vec<u8> {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::io::Cursor;
    use futures::{AsyncReadExt, TryStreamExt};

    use super::*;

    #[tokio::test]
    async fn test_copy_reader() {
        let readers: Vec<Box<dyn AsyncRead + Unpin>> = vec![
            Box::new(Cursor::new("abcdef".as_bytes())),
            Box::new(Cursor::new(vec![0x88; 1024])),
            n_stream(3),
            n_stream(500),
            n_stream(10000),
        ];

        for mut reader in readers {
            let mut copy_reader = CopyReader::new(Pin::new(&mut reader));
            let mut buf = Vec::new();
            assert!(copy_reader.read_to_end(&mut buf).await.is_ok());
            assert_eq!(buf, copy_reader.consume());
        }
    }

    #[tokio::test]
    async fn test_copy_reader_partially() {
        let readers: Vec<Box<dyn AsyncRead + Unpin>> = vec![Box::new(Cursor::new(vec![0x88; 1024])), n_stream(10000)];

        for mut reader in readers {
            let mut copy_reader = CopyReader::new(Pin::new(&mut reader));
            let mut buf = vec![0; 512];
            assert!(copy_reader.read_exact(&mut buf).await.is_ok());
            assert_eq!(buf, copy_reader.consume());
        }
    }

    struct NIter {
        remaining: usize,
    }

    impl NIter {
        fn new(n: usize) -> Self {
            Self { remaining: n }
        }
    }

    impl Iterator for NIter {
        type Item = Result<Bytes, std::io::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.remaining == 0 {
                return None;
            }
            self.remaining -= 1;
            Some(Ok(Bytes::from_static(b"hello world")))
        }
    }

    fn n_stream(n: usize) -> Box<dyn AsyncRead + Unpin> {
        Box::new(futures::stream::iter(NIter::new(n)).into_async_read())
    }
}
