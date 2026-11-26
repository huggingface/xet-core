//! Git filter protocol v2 framing. File payloads are arbitrary bytes.
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_PACKET: usize = 65520;
/// Limit on the number of header lines in one message, bounding their memory to this many packets. Git
/// sends at most six per request (`convert.c`: command, pathname, ref, treeish, blob, can-delay) and a
/// few more in the handshake; the rest is room for new ones.
const MAX_HEADERS: usize = 16;
/// Size of the buffer used to read file content.
pub(crate) const READ_CHUNK_SIZE: usize = 64 * 1024;

pub(crate) enum Packet {
    Data(Vec<u8>),
    Flush,
    Eof,
}

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) async fn read_packet(input: &mut (impl AsyncRead + Unpin)) -> io::Result<Packet> {
    let mut header = [0; 4];
    loop {
        match input.read(&mut header[..1]).await {
            Ok(0) => return Ok(Packet::Eof),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    input.read_exact(&mut header[1..]).await?;
    if !header.iter().all(u8::is_ascii_hexdigit) {
        return Err(invalid("Invalid packet length"));
    }
    let len = usize::from_str_radix(std::str::from_utf8(&header).map_err(|_| invalid("Invalid packet header"))?, 16)
        .map_err(|_| invalid("Invalid packet length"))?;
    if len == 0 {
        return Ok(Packet::Flush);
    }
    if !(4..=MAX_PACKET).contains(&len) {
        return Err(invalid("Packet length out of range"));
    }
    let mut data = vec![0; len - 4];
    input.read_exact(&mut data).await?;
    Ok(Packet::Data(data))
}

pub(crate) async fn write_data(output: &mut (impl AsyncWrite + Unpin), data: &[u8]) -> io::Result<()> {
    for chunk in data.chunks(MAX_PACKET - 4) {
        output.write_all(format!("{:04x}", chunk.len() + 4).as_bytes()).await?;
        output.write_all(chunk).await?;
    }
    Ok(())
}

pub(crate) async fn flush_packet(output: &mut (impl AsyncWrite + Unpin)) -> io::Result<()> {
    output.write_all(b"0000").await
}

pub(crate) async fn headers(input: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Vec<Vec<u8>>>> {
    let mut lines = Vec::new();
    loop {
        match read_packet(input).await? {
            Packet::Eof if lines.is_empty() => return Ok(None),
            Packet::Eof => return Err(invalid("EOF inside headers")),
            Packet::Flush => return Ok(Some(lines)),
            Packet::Data(mut data) => {
                if lines.len() == MAX_HEADERS {
                    return Err(invalid("Too many filter headers"));
                }
                if data.last() == Some(&b'\n') {
                    data.pop();
                }
                lines.push(data);
            },
        }
    }
}

/// One request's content: the data packets up to the next flush packet.
pub(crate) struct Content<'a, R> {
    input: &'a mut R,
    packet: io::Cursor<Vec<u8>>,
    done: bool,
}

impl<'a, R: AsyncRead + Unpin> Content<'a, R> {
    pub(crate) fn new(input: &'a mut R) -> Self {
        Self {
            input,
            packet: io::Cursor::new(Vec::new()),
            done: false,
        }
    }

    /// Reads up to `out.len()` bytes; 0 means the content has ended.
    pub(crate) async fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = io::Read::read(&mut self.packet, out)?;
            if n > 0 {
                return Ok(n);
            }
            if self.done {
                return Ok(0);
            }
            match read_packet(self.input).await? {
                Packet::Flush => {
                    self.done = true;
                    return Ok(0);
                },
                Packet::Eof => return Err(invalid("EOF inside file content")),
                Packet::Data(data) => self.packet = io::Cursor::new(data),
            }
        }
    }

    /// Appends content to `buffer` until it holds `limit` bytes or the content ends.
    pub(crate) async fn read_up_to(&mut self, buffer: &mut Vec<u8>, limit: usize) -> io::Result<()> {
        let mut chunk = vec![0; READ_CHUNK_SIZE];
        while buffer.len() < limit {
            let n = self.read(&mut chunk[..(limit - buffer.len()).min(READ_CHUNK_SIZE)]).await?;
            if n == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..n]);
        }
        Ok(())
    }

    /// Reads and discards the rest of the content.
    pub(crate) async fn drain(&mut self) -> io::Result<()> {
        let mut chunk = vec![0; READ_CHUNK_SIZE];
        while self.read(&mut chunk).await? > 0 {}
        Ok(())
    }
}

/// Runs a future to completion on a fresh current-thread runtime, for tests that only parse bytes.
#[cfg(test)]
pub(crate) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(future)
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::ReadBuf;

    use super::*;

    #[test]
    fn interrupted_probe_is_retried() {
        struct InterruptedOnce<'a> {
            bytes: &'a [u8],
            interrupted: bool,
        }
        impl AsyncRead for InterruptedOnce<'_> {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
                }
                Pin::new(&mut self.bytes).poll_read(cx, buf)
            }
        }
        let mut input = InterruptedOnce {
            bytes: b"0005x",
            interrupted: false,
        };
        assert!(matches!(block_on(read_packet(&mut input)).unwrap(), Packet::Data(data) if data == b"x"));
    }

    #[test]
    fn roundtrip_and_boundaries() {
        block_on(async {
            for size in [0, 1, 65516, 65517, 200000] {
                let data = vec![0xff; size];
                let mut wire = Vec::new();
                write_data(&mut wire, &data).await.unwrap();
                flush_packet(&mut wire).await.unwrap();
                let mut input = wire.as_slice();
                let mut content = Content::new(&mut input);
                let mut out = Vec::new();
                content.read_up_to(&mut out, usize::MAX).await.unwrap();
                assert_eq!(out, data);
                assert!(matches!(read_packet(&mut input).await.unwrap(), Packet::Eof));
            }
        });
    }

    #[test]
    fn malformed_and_truncated_packets() {
        block_on(async {
            for data in [
                b"0".as_slice(),
                b"000",
                b"zzzz",
                b"0001",
                b"0002",
                b"0003",
                b"ffff",
                b"0008abc",
            ] {
                assert!(read_packet(&mut &data[..]).await.is_err(), "{data:?}");
            }
            assert!(Content::new(&mut &b"0005a"[..]).drain().await.is_err());
        });
    }
}
