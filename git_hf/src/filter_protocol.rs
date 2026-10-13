//! Git filter protocol v2 framing. File payloads are arbitrary bytes.
use std::io::{self, Read, Write};

const MAX_PACKET: usize = 65520;

pub(crate) enum Packet {
    Data(Vec<u8>),
    Flush,
    Eof,
}

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) fn read_packet(input: &mut impl Read) -> io::Result<Packet> {
    let mut header = [0; 4];
    loop {
        match input.read(&mut header[..1]) {
            Ok(0) => return Ok(Packet::Eof),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    input.read_exact(&mut header[1..])?;
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
    input.read_exact(&mut data)?;
    Ok(Packet::Data(data))
}

pub(crate) fn write_data(output: &mut impl Write, data: &[u8]) -> io::Result<()> {
    for chunk in data.chunks(MAX_PACKET - 4) {
        write!(output, "{:04x}", chunk.len() + 4)?;
        output.write_all(chunk)?;
    }
    Ok(())
}
pub(crate) fn flush_packet(output: &mut impl Write) -> io::Result<()> {
    output.write_all(b"0000")
}

pub(crate) fn headers(input: &mut impl Read) -> io::Result<Option<Vec<Vec<u8>>>> {
    let mut lines = Vec::new();
    let mut size = 0;
    loop {
        match read_packet(input)? {
            Packet::Eof if lines.is_empty() => return Ok(None),
            Packet::Eof => return Err(invalid("EOF inside headers")),
            Packet::Flush => return Ok(Some(lines)),
            Packet::Data(mut data) => {
                size += data.len() + 4;
                if size > 65536 {
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

pub(crate) struct Content<'a, R> {
    input: &'a mut R,
    packet: std::io::Cursor<Vec<u8>>,
    done: bool,
}
impl<'a, R: Read> Content<'a, R> {
    pub(crate) fn new(input: &'a mut R) -> Self {
        Self {
            input,
            packet: std::io::Cursor::new(Vec::new()),
            done: false,
        }
    }
}
impl<R: Read> Read for Content<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.packet.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            if self.done {
                return Ok(0);
            }
            match read_packet(self.input)? {
                Packet::Flush => {
                    self.done = true;
                    return Ok(0);
                },
                Packet::Eof => return Err(invalid("EOF inside file content")),
                Packet::Data(data) => self.packet = std::io::Cursor::new(data),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interrupted_probe_is_retried() {
        struct InterruptedOnce<'a> {
            bytes: &'a [u8],
            interrupted: bool,
        }
        impl Read for InterruptedOnce<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                self.bytes.read(out)
            }
        }
        let mut input = InterruptedOnce {
            bytes: b"0005x",
            interrupted: false,
        };
        assert!(matches!(read_packet(&mut input).unwrap(), Packet::Data(data) if data == b"x"));
    }

    #[test]
    fn roundtrip_and_boundaries() {
        for size in [0, 1, 65516, 65517, 200000] {
            let data = vec![0xff; size];
            let mut wire = Vec::new();
            write_data(&mut wire, &data).unwrap();
            flush_packet(&mut wire).unwrap();
            let mut input = wire.as_slice();
            let mut content = Content::new(&mut input);
            let mut out = Vec::new();
            content.read_to_end(&mut out).unwrap();
            assert_eq!(out, data);
            assert!(matches!(read_packet(&mut input).unwrap(), Packet::Eof));
        }
    }
    #[test]
    fn malformed_and_truncated_packets() {
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
            assert!(read_packet(&mut &data[..]).is_err(), "{data:?}");
        }
        assert!(Content::new(&mut &b"0005a"[..]).read_to_end(&mut Vec::new()).is_err());
    }
}
