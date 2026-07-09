use std::fs::File;
use std::io;

/// Write `buf` at absolute byte `offset` in `file` without using or disturbing the
/// file's cursor. Safe to call concurrently on clones of the same `Arc<File>`.
#[cfg(unix)]
fn positioned_write(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

/// Windows equivalent: `seek_write` may write fewer bytes than requested, so loop.
#[cfg(windows)]
fn positioned_write(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "seek_write wrote 0 bytes"));
        }
        written += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use super::*;

    #[test]
    fn positioned_write_out_of_order_fills_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        // Write the second half before the first half.
        positioned_write(&file, b"world", 6).unwrap();
        positioned_write(&file, b"hello ", 0).unwrap();

        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    }
}
