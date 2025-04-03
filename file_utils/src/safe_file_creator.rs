use std::fs::{self, File, Metadata};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tempfile::NamedTempFile;

use crate::create_file;
use crate::file_metadata::set_file_metadata;

pub struct SafeFileCreator {
    dest_path: Option<PathBuf>,
    temp_path: PathBuf,
    original_metadata: Option<Metadata>,
    writer: Option<BufWriter<File>>,
}

impl SafeFileCreator {
    /// Safely creates a new file at a specific location.  Ensures the file is not created with elevated privileges,
    /// and a temporary file is created then renamed on close.
    pub fn new<P: AsRef<Path>>(dest_path: P) -> io::Result<Self> {
        let dest_path = dest_path.as_ref().to_path_buf();
        let temp_path = Self::temp_file_path(Some(&dest_path))?;

        // This matches the permissions and ownership of the parent directory
        let file = create_file(&temp_path)?;
        let writer = BufWriter::new(file);

        Ok(SafeFileCreator {
            dest_path: Some(dest_path),
            temp_path,
            original_metadata: None,
            writer: Some(writer),
        })
    }

    /// Safely creates a new file while a destination name can't be decided now. Users need to call
    /// ```ignore
    /// pub fn set_dest_path<P: AsRef<Path>>(dest_path: P)
    /// ```
    /// to set the destination before closing the file.
    pub fn new_unnamed() -> io::Result<Self> {
        let temp_path = Self::temp_file_path(None)?;

        // This matches the permissions and ownership of the parent directory
        let file = create_file(&temp_path)?;
        let writer = BufWriter::new(file);

        Ok(SafeFileCreator {
            dest_path: None,
            temp_path,
            original_metadata: None,
            writer: Some(writer),
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
    fn temp_file_path(dest_path: Option<&Path>) -> io::Result<PathBuf> {
        let (parent, file_name) = match dest_path {
            Some(p) => {
                let parent = p.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "path doesn't have a valid parent directory")
                })?;
                let file_name = p.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "path doesn't have a valid file name")
                })?;
                (parent.to_owned(), file_name.to_str().unwrap_or_default())
            },
            None => (std::env::temp_dir(), ""),
        };

        let mut rng = thread_rng();
        let random_hash: String = (0..10).map(|_| rng.sample(Alphanumeric)).map(char::from).collect();
        let temp_file_name = format!(".{}.{hash}.tmp", file_name, hash = random_hash);
        Ok(parent.join(temp_file_name))
    }

    pub fn set_dest_path<P: AsRef<Path>>(&mut self, dest_path: P) {
        let dest_path = dest_path.as_ref().to_path_buf();
        self.dest_path = Some(dest_path);
    }

    /// Closes the writer and replaces the original file with the temporary file
    pub fn close(&mut self) -> io::Result<()> {
        let Some(dest_path) = &self.dest_path else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "destination file name not set"));
        };

        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };

        writer.flush()?;
        drop(writer);

        // Replace the original file with the new file
        fs::rename(&self.temp_path, dest_path)?;

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
                format!("Writing to {:?} already completed.", &self.dest_path),
            )),
        }
    }
}

impl Write for SafeFileCreator {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer()?.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer()?.flush()
    }
}

impl Drop for SafeFileCreator {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            eprintln!("Error: Failed to close writer for {:?}: {}", &self.dest_path, e);
        }
    }
}

/// Write all bytes
pub fn write_all_safe(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if !path.as_os_str().is_empty() {
        let dir = path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("Unable to find parent path from {path:?}"))
        })?;

        // Make sure dir exists.
        if !dir.exists() {
            fs::create_dir_all(dir)?;
        }

        let mut tempfile = create_temp_file(dir, "")?;
        tempfile.write_all(bytes)?;
        tempfile.persist(path).map_err(|e| e.error)?;
    }

    Ok(())
}

pub fn create_temp_file(dir: &Path, suffix: &str) -> io::Result<NamedTempFile> {
    let tempfile = tempfile::Builder::new()
        .prefix(&format!("{}.", std::process::id()))
        .suffix(suffix)
        .tempfile_in(dir)?;

    Ok(tempfile)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
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

        // Verify file permissions
        let metadata = fs::metadata(&dest_path).unwrap();
        let permissions = metadata.permissions();
        #[cfg(unix)]
        assert_eq!(permissions.mode() & 0o777, 0o644); // Assuming default creation mode
    }

    #[test]
    fn test_safe_file_creator_new_unnamed() {
        let mut safe_file_creator = SafeFileCreator::new_unnamed().unwrap();
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

        // Verify file permissions
        let metadata = fs::metadata(&dest_path).unwrap();
        let permissions = metadata.permissions();
        #[cfg(unix)]
        assert_eq!(permissions.mode() & 0o777, 0o644); // Assuming default creation mode
    }

    #[test]
    fn test_safe_file_creator_replace_existing() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("existing_file.txt");

        // Create the existing file
        {
            let mut file = File::create(&dest_path).unwrap();
            file.write_all(b"Old content").unwrap();
            let mut perms = file.metadata().unwrap().permissions();
            #[cfg(unix)]
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
}
