use std::io::Write;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::{AsyncRead, AsyncReadExt};

// (AsyncRead) adaptor
// wraps over an AsyncRead, copying all the contents read from the inner reader
// and buffers it in an internal buffer which can be retrieved by calling .consume()
// to return a copy of all the content that was read.
pub struct CopyReader<'r, 'w, R, W> {
    src: Pin<&'r mut R>,
    writer: &'w mut W,
}

impl<R: AsyncRead + Unpin, W: Write> AsyncRead for CopyReader<'_, '_, R, W> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let res = ready!(self.src.as_mut().poll_read(cx, buf))?;
        self.writer.write_all(&buf[..res])?;
        Poll::Ready(Ok(res))
    }
}

impl<R: AsyncRead + Unpin, W: Write> Unpin for CopyReader<'_, '_, R, W> {}

impl<'r, 'w, R: AsyncRead + Unpin, W: Write> CopyReader<'r, 'w, R, W> {
    pub fn new(src: &'r mut R, writer: &'w mut W) -> Self {
        let src = Pin::new(src);
        Self { src, writer }
    }
}

/// An extension trait for `AsyncRead` that provides additional methods beyond the normal `AsyncReadExt`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait AsyncReadCustomExt: AsyncReadExt + Unpin {
    /// Reads all data from the stream until EOF and discards it.
    async fn drain<'a>(&'a mut self) -> std::io::Result<()> {
        const BUFFER_SIZE: usize = 8192;
        let mut buf = [0u8; BUFFER_SIZE];
        loop {
            let n = self.read(&mut buf).await?;
            if n == 0 {
                break; // EOF
            }
        }
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<T: AsyncReadExt + Unpin> AsyncReadCustomExt for T {}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom};

    use bytes::Bytes;
    use futures::io::Cursor;
    use futures::{AsyncReadExt, TryStreamExt};
    use tempfile::tempfile;

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
            let mut writer = Vec::new();
            let mut copy_reader = CopyReader::new(&mut reader, &mut writer);
            let mut buf = Vec::new();
            assert!(copy_reader.read_to_end(&mut buf).await.is_ok());
            assert_eq!(buf, writer);
        }
    }

    #[tokio::test]
    async fn test_copy_reader_to_file() {
        let readers: Vec<Box<dyn AsyncRead + Unpin>> = vec![
            Box::new(Cursor::new("abcdeflaksjdlakjsldkajlfkjal".as_bytes())),
            Box::new(Cursor::new(vec![0x88; 10000])),
            n_stream(3),
            n_stream(500),
            n_stream(10000),
        ];

        for mut reader in readers {
            let mut writer = tempfile().unwrap();
            let mut copy_reader = CopyReader::new(&mut reader, &mut writer);
            let mut buf = Vec::new();
            assert!(copy_reader.read_to_end(&mut buf).await.is_ok());

            // read file to compare contents
            assert!(writer.seek(SeekFrom::Start(0)).is_ok());
            let mut file_contents = Vec::new();
            assert!(writer.read_to_end(&mut file_contents).is_ok());

            assert_eq!(buf, file_contents);
        }
    }

    #[tokio::test]
    async fn test_copy_reader_partially() {
        let readers: Vec<Box<dyn AsyncRead + Unpin>> = vec![Box::new(Cursor::new(vec![0x88; 1024])), n_stream(10000)];

        for mut reader in readers {
            let mut writer = Vec::new();
            let mut copy_reader = CopyReader::new(&mut reader, &mut writer);
            let mut buf = vec![0; 512];
            assert!(copy_reader.read_exact(&mut buf).await.is_ok());
            assert_eq!(buf, writer);
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
