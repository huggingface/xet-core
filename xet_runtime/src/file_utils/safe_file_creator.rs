use std::fs::{self, File, Metadata};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use rand::distr::Alphanumeric;
use rand::{RngExt, rng};

use super::create_file;
use super::file_metadata::set_file_metadata;

pub struct SafeFileCreator {
    dest_path: Option<PathBuf>,
    temp_path: PathBuf,
    original_metadata: Option<Metadata>,
    writer: Option<BufWriter<File>>,
    // Tracks whether the last write, flush, or seek failed; the next successful one clears it.
    // A failed writer holds incomplete data, so dropping the creator discards the temporary
    // file instead of publishing it, while an explicit close still publishes after recovery.
    failed: bool,
}

impl SafeFileCreator {
    /// Safely creates a new file at a specific location.  Ensures the file is not created with elevated privileges,
    /// and a temporary file is created then renamed on close.
    pub fn new<P: AsRef<Path>>(dest_path: P) -> io::Result<Self> {
        let dest_path = dest_path.as_ref().to_path_buf();

        let parent = dest_path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path doesn't have a valid parent directory"))?;
        let file_name = parent
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path doesn't have a valid file name"))?
            .to_str();

        let temp_path = Self::temp_file_path(parent, file_name);

        // This matches the permissions and ownership of the parent directory
        let file = create_file(&temp_path)?;
        let writer = BufWriter::new(file);

        Ok(SafeFileCreator {
            dest_path: Some(dest_path),
            temp_path,
            original_metadata: None,
            writer: Some(writer),
            failed: false,
        })
    }

    /// Safely creates a new file while a destination name can't be decided now. Users need to call
    /// ```ignore
    /// pub fn set_dest_path<P: AsRef<Path>>(dest_path: P)
    /// ```
    /// to set the destination before closing the file.
    pub fn new_unnamed(temp_root: impl AsRef<Path>) -> io::Result<Self> {
        let temp_path = Self::temp_file_path(temp_root, None);

        // This matches the permissions and ownership of the parent directory
        let file = create_file(&temp_path)?;
        let writer = BufWriter::new(file);

        Ok(SafeFileCreator {
            dest_path: None,
            temp_path,
            original_metadata: None,
            writer: Some(writer),
            failed: false,
        })
    }

    /// Safely replaces a new file at a specific location.  Ensures the file is not created with elevated privileges,
    /// and additionally the metadata of the old one will match the new metadata.
    pub fn replace_existing<P: AsRef<Path>>(dest_path: P) -> io::Result<Self> {
        let mut s = Self::new(&dest_path)?;
        s.original_metadata = fs::metadata(dest_path).ok();
        Ok(s)
    }

    /// Generates a temporary file path in the same directory as the destination file
    fn temp_file_path(dest_dir: impl AsRef<Path>, file: Option<&str>) -> PathBuf {
        let mut rng = rng();
        let random_hash: String = (0..10).map(|_| rng.sample(Alphanumeric)).map(char::from).collect();
        let temp_file_name = if let Some(filename) = file {
            format!(".{filename}.{random_hash}.tmp")
        } else {
            format!(".{random_hash}.tmp")
        };
        dest_dir.as_ref().join(temp_file_name)
    }

    pub fn set_dest_path<P: AsRef<Path>>(&mut self, dest_path: P) {
        let dest_path = dest_path.as_ref().to_path_buf();
        self.dest_path = Some(dest_path);
    }

    // abort the writing process and delete the temporary file
    pub fn abort(&mut self) -> io::Result<()> {
        self.failed = false;
        self.discard()
    }

    // drops the writer and deletes the temporary file, leaving the destination untouched
    fn discard(&mut self) -> io::Result<()> {
        self.writer = None;
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path)?;
        }
        Ok(())
    }

    /// Closes the writer and replaces the original file with the temporary file.
    ///
    /// If the final flush fails, the error is returned and the writer is kept so that `close`
    /// can be retried after the problem is fixed. Dropping the creator without a successful
    /// `close` discards the temporary file whenever a write, flush, or seek has failed, and
    /// then leaves the destination file untouched.
    pub fn close(&mut self) -> io::Result<()> {
        let Some(dest_path) = &self.dest_path else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "destination file name not set"));
        };

        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };

        // A failed flush leaves the temporary file incomplete, so give the writer back for a
        // retry instead of consuming it.
        if let Err(e) = writer.flush() {
            self.failed = true;
            self.writer = Some(writer);
            return Err(e);
        }
        drop(writer);
        self.failed = false;

        // Replace the original file with the new file
        if let Err(e) = fs::rename(&self.temp_path, dest_path) {
            // The destination is untouched, so mark the temporary file for cleanup on drop.
            self.failed = true;
            return Err(e);
        }

        if let Some(metadata) = self.original_metadata.as_ref() {
            set_file_metadata(dest_path, metadata, false)?;
        }
        let original_permissions = if dest_path.exists() {
            Some(fs::metadata(dest_path)?.permissions())
        } else {
            None
        };

        // Set the original file's permissions to the new file if they exist
        if let Some(permissions) = original_permissions {
            fs::set_permissions(dest_path, permissions.clone())?;
        }

        Ok(())
    }

    fn writer(&mut self) -> io::Result<&mut BufWriter<File>> {
        match &mut self.writer {
            Some(wr) => Ok(wr),
            None => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("Writing to {:?} already completed.", self.dest_path),
            )),
        }
    }
}

impl Write for SafeFileCreator {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writer()?.write(buf) {
            Ok(n) => {
                self.failed = false;
                Ok(n)
            },
            Err(e) => {
                self.failed = true;
                Err(e)
            },
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.writer()?.flush() {
            Ok(()) => {
                self.failed = false;
                Ok(())
            },
            Err(e) => {
                self.failed = true;
                Err(e)
            },
        }
    }
}

impl Seek for SafeFileCreator {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self.writer()?.seek(pos) {
            Ok(pos) => {
                self.failed = false;
                Ok(pos)
            },
            Err(e) => {
                self.failed = true;
                Err(e)
            },
        }
    }
}

impl Drop for SafeFileCreator {
    fn drop(&mut self) {
        if self.failed {
            // The last write, flush, or seek failed, so the temporary file holds incomplete
            // data. Discard it rather than publishing it over the destination.
            if let Err(e) = self.discard() {
                eprintln!("Error: Failed to discard temporary file for {:?}: {}", self.dest_path, e);
            }
            return;
        }
        if let Err(e) = self.close() {
            eprintln!("Error: Failed to close writer for {:?}: {}", self.dest_path, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_safe_file_creator_new() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("new_file.txt");

        let mut safe_file_creator = SafeFileCreator::new(&dest_path).unwrap();
        writeln!(safe_file_creator, "Hello, world!").unwrap();
        safe_file_creator.close().unwrap();

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.trim(), "Hello, world!");

        // Verify file permissions: owner should have read/write.
        // Group/other bits depend on host umask and may vary (e.g. 0o600 vs 0o644).
        #[cfg(unix)]
        {
            let metadata = std::fs::metadata(&dest_path).unwrap();
            let permissions = metadata.permissions();
            let mode = permissions.mode() & 0o777;
            // Default creation mode is 0o666 masked by umask.
            assert!(mode & 0o600 == 0o600, "Owner should have rw permissions, got {mode:#o}");
        }
    }

    #[test]
    fn test_safe_file_creator_new_unnamed() {
        let _dir = tempdir().unwrap();
        let mut safe_file_creator = SafeFileCreator::new_unnamed(_dir.path()).unwrap();
        writeln!(safe_file_creator, "Hello, world!").unwrap();

        // Test error checking
        let ret = safe_file_creator.close();
        assert!(ret.is_err());

        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("new_file.txt");
        safe_file_creator.set_dest_path(&dest_path);
        safe_file_creator.close().unwrap();

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.trim(), "Hello, world!");

        // Verify file permissions: owner should have read/write.
        // Group/other bits depend on host umask and may vary (e.g. 0o600 vs 0o644).
        #[cfg(unix)]
        {
            let metadata = std::fs::metadata(&dest_path).unwrap();
            let permissions = metadata.permissions();
            let mode = permissions.mode() & 0o777;
            // Default creation mode is 0o666 masked by umask.
            assert!(mode & 0o600 == 0o600, "Owner should have rw permissions, got {mode:#o}");
        }
    }

    #[test]
    fn test_safe_file_creator_replace_existing() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("existing_file.txt");

        // Create the existing file
        {
            let mut file = File::create(&dest_path).unwrap();
            file.write_all(b"Old content").unwrap();
            #[cfg(unix)]
            {
                let mut perms = file.metadata().unwrap().permissions();
                perms.set_mode(0o600);
                std::fs::set_permissions(&dest_path, perms).unwrap();
            }
        }

        let mut safe_file_creator = SafeFileCreator::replace_existing(&dest_path).unwrap();
        writeln!(safe_file_creator, "New content").unwrap();
        safe_file_creator.close().unwrap();

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.trim(), "New content");

        // Verify file permissions
        #[cfg(unix)]
        {
            let metadata = std::fs::metadata(&dest_path).unwrap();
            let permissions = metadata.permissions();
            assert_eq!(permissions.mode() & 0o777, 0o600); // Original file mode
        }
    }

    #[test]
    fn test_safe_file_creator_drop() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("drop_file.txt");

        {
            let mut safe_file_creator = SafeFileCreator::new(&dest_path).unwrap();
            writeln!(safe_file_creator, "Hello, world!").unwrap();
            // safe_file_creator is dropped here
        }

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.trim(), "Hello, world!");
    }

    #[test]
    fn test_safe_file_creator_double_close() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("double_close_file.txt");

        let mut safe_file_creator = SafeFileCreator::new(&dest_path).unwrap();
        writeln!(safe_file_creator, "Hello, world!").unwrap();
        safe_file_creator.close().unwrap();
        safe_file_creator.close().unwrap(); // Should be a no-op

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.trim(), "Hello, world!");
    }

    #[test]
    #[cfg(unix)]
    fn test_safe_file_creator_set_metadata() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("metadata_file.txt");

        // Create the existing file
        {
            let mut file = File::create(&dest_path).unwrap();
            file.write_all(b"Old content").unwrap();
            let mut perms = file.metadata().unwrap().permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&dest_path, perms).unwrap();
        }

        let mut safe_file_creator = SafeFileCreator::replace_existing(&dest_path).unwrap();
        writeln!(safe_file_creator, "New content").unwrap();
        safe_file_creator.close().unwrap();

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.trim(), "New content");

        // Verify file permissions
        let metadata = fs::metadata(&dest_path).unwrap();
        let permissions = metadata.permissions();
        #[cfg(unix)]
        assert_eq!(permissions.mode() & 0o777, 0o600); // Original file mode
    }

    // The tests below need an underlying file whose every write fails. A file opened read
    // only does exactly that: every write or flush through it fails, without touching any
    // other file, and the original temporary file it reads from stays untouched as well.
    fn read_only_file(path: &Path) -> File {
        File::options().read(true).open(path).unwrap()
    }

    #[test]
    fn test_safe_file_creator_drop_after_write_failure_discards() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("dest_file.txt");

        // An existing destination that must survive the failed write.
        {
            let mut file = File::create(&dest_path).unwrap();
            file.write_all(b"Old content").unwrap();
        }

        {
            let mut safe_file_creator = SafeFileCreator::replace_existing(&dest_path).unwrap();
            let temp_path = safe_file_creator.temp_path.clone();
            safe_file_creator.writer = Some(BufWriter::new(read_only_file(&temp_path)));
            // A write larger than the BufWriter buffer goes straight to the read only file.
            let payload = vec![0u8; 1 << 16];
            assert!(safe_file_creator.write_all(&payload).is_err());
            // safe_file_creator is dropped here without an explicit close
        }

        // The destination still has its original content and the temporary file is gone.
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "Old content");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);

        // A new destination is not created either.
        let fresh_path = dir.path().join("fresh_file.txt");
        {
            let mut safe_file_creator = SafeFileCreator::new(&fresh_path).unwrap();
            let temp_path = safe_file_creator.temp_path.clone();
            safe_file_creator.writer = Some(BufWriter::new(read_only_file(&temp_path)));
            let payload = vec![0u8; 1 << 16];
            assert!(safe_file_creator.write_all(&payload).is_err());
        }
        assert!(!fresh_path.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn test_safe_file_creator_drop_after_flush_failure_preserves_destination() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("dest_file.txt");

        // An existing destination that must survive the failed flush.
        {
            let mut file = File::create(&dest_path).unwrap();
            file.write_all(b"Old content").unwrap();
        }

        {
            let mut safe_file_creator = SafeFileCreator::replace_existing(&dest_path).unwrap();
            let temp_path = safe_file_creator.temp_path.clone();
            safe_file_creator.writer = Some(BufWriter::new(read_only_file(&temp_path)));
            // Small writes stay in the BufWriter buffer, so the failure only surfaces when
            // close flushes, and the creator is dropped without retrying the close.
            writeln!(safe_file_creator, "New content").unwrap();
            assert!(safe_file_creator.close().is_err());
        }

        // The destination still has its original content and the temporary file is gone.
        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "Old content");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn test_safe_file_creator_close_flush_failure_preserves_writer_for_retry() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("dest_file.txt");

        let mut safe_file_creator = SafeFileCreator::new(&dest_path).unwrap();
        let temp_path = safe_file_creator.temp_path.clone();
        safe_file_creator.writer = Some(BufWriter::new(read_only_file(&temp_path)));
        // Small writes stay in the BufWriter buffer; the flush inside close hits the read
        // only file and fails.
        writeln!(safe_file_creator, "Broken write").unwrap();
        assert!(safe_file_creator.close().is_err());

        // The failed close keeps the writer for a retry instead of consuming it.
        assert!(safe_file_creator.writer.is_some());

        // Recovery: replace the read only file with a writable one, finish writing, and close
        // explicitly.
        let temp_path = safe_file_creator.temp_path.clone();
        safe_file_creator.writer = Some(BufWriter::new(File::options().write(true).open(&temp_path).unwrap()));
        writeln!(safe_file_creator, "Recovered write").unwrap();
        safe_file_creator.close().unwrap();

        let mut contents = String::new();
        File::open(&dest_path).unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.trim(), "Recovered write");
    }
}
