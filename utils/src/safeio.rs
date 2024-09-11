use std::{
    io::{self, Write},
    path::Path,
};
use tempfile::NamedTempFile;

/// Write all bytes
pub fn write_all_file_safe(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if !path.as_os_str().is_empty() {
        let dir = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to find parent path from {path:?}"),
            )
        })?;

        // Make sure dir exists.
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
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
mod test {
    use anyhow::Result;
    use std::fs;
    use tempfile::TempDir;

    use super::write_all_file_safe;

    #[test]
    fn test_small_file_write() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let bytes = vec![1u8; 1000];
        let file_name = tmp_dir.path().join("data");

        write_all_file_safe(&file_name, &bytes)?;

        assert_eq!(fs::read(file_name)?, bytes);

        Ok(())
    }
}
