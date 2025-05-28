use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
#[cfg(unix)]
use std::time::SystemTime;

/// Matches the metadata of a file to another file's metadata
#[cfg(unix)]
pub fn set_file_metadata<P: AsRef<Path>>(path: P, metadata: &Metadata, match_owner: bool) -> std::io::Result<()> {
    let path = path.as_ref();

    // Set permissions
    let permissions = metadata.permissions();
    std::fs::set_permissions(path, permissions.clone())?;

    // Set timestamps
    let atime = metadata.accessed()?;
    let mtime = metadata.modified()?;
    let atime = atime.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let mtime = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let times = [
        libc::timespec {
            tv_sec: atime.as_secs() as libc::time_t,
            tv_nsec: atime.subsec_nanos() as libc::c_long,
        },
        libc::timespec {
            tv_sec: mtime.as_secs() as libc::time_t,
            tv_nsec: mtime.subsec_nanos() as libc::c_long,
        },
    ];

    if let Some(path_s) = path.to_str() {
        if match_owner {
            // Set ownership
            let uid = metadata.uid();
            let gid = metadata.gid();
            unsafe {
                libc::chown(path_s.as_bytes().as_ptr() as *const libc::c_char, uid, gid);
            }
        }

        unsafe {
            libc::utimensat(libc::AT_FDCWD, path_s.as_bytes().as_ptr() as *const libc::c_char, times.as_ptr(), 0);
        }
    }
    Ok(())
}

/// Matches the metadata of a file to another file's metadata
#[cfg(not(unix))]
pub fn set_file_metadata<P: AsRef<Path>>(path: P, metadata: &Metadata, _match_owner: bool) -> std::io::Result<()> {
    let path = path.as_ref();

    // Set permissions
    let permissions = metadata.permissions();
    std::fs::set_permissions(path, permissions.clone())?;
    Ok(())
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use std::fs::{self, File};
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, SystemTime};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_set_metadata_permissions() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");
        File::create(&file_path).unwrap();

        // Set some initial permissions
        let mut perms = File::open(&file_path).unwrap().metadata().unwrap().permissions();
        perms.set_mode(0o644);

        fs::set_permissions(&file_path, perms.clone()).unwrap();

        // Create a file with different permissions to copy from
        let src_file_path = dir.path().join("src_file");
        let src_file = File::create(&src_file_path).unwrap();
        let mut src_perms = src_file.metadata().unwrap().permissions();
        src_perms.set_mode(0o600);
        fs::set_permissions(&src_file_path, src_perms.clone()).unwrap();

        let src_metadata = src_file.metadata().unwrap();

        // Apply set_metadata
        set_file_metadata(&file_path, &src_metadata, false).unwrap();

        // Check that permissions have been updated.  But we need to re-read some of the things
        // as Unix adds an extra bit indicating a regular file.
        let updated_metadata = File::open(file_path).unwrap().metadata().unwrap();
        let src_metadata = File::open(src_file_path).unwrap().metadata().unwrap();

        assert_eq!(updated_metadata.permissions().mode(), src_metadata.permissions().mode());
        assert_eq!(updated_metadata.modified().unwrap(), src_metadata.modified().unwrap());
    }

    #[test]
    fn test_set_metadata_timestamps() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");
        let file = File::create(&file_path).unwrap();

        // Create a file with specific timestamps to copy from
        let src_file_path = dir.path().join("src_file");
        let src_file = File::create(&src_file_path).unwrap();

        let src_metadata = src_file.metadata().unwrap();

        let atime = SystemTime::now() - Duration::from_secs(24 * 3600);
        let mtime = SystemTime::now() - Duration::from_secs(48 * 3600);

        let times = [
            libc::timespec {
                tv_sec: atime.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as libc::time_t,
                tv_nsec: atime.duration_since(SystemTime::UNIX_EPOCH).unwrap().subsec_nanos() as libc::c_long,
            },
            libc::timespec {
                tv_sec: mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as libc::time_t,
                tv_nsec: mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap().subsec_nanos() as libc::c_long,
            },
        ];

        unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                src_file_path.to_str().unwrap().as_bytes().as_ptr() as *const libc::c_char,
                times.as_ptr(),
                0,
            );
        }

        // Apply set_metadata
        set_file_metadata(&file_path, &src_metadata, false).unwrap();

        // Check that timestamps have been updated
        let updated_metadata = file.metadata().unwrap();
        assert_eq!(updated_metadata.modified().unwrap(), src_metadata.modified().unwrap());
    }

    #[test]
    fn test_set_metadata_owner() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");
        let file = File::create(&file_path).unwrap();

        // Create a file with specific ownership to copy from
        let src_file_path = dir.path().join("src_file");
        let src_file = File::create(&src_file_path).unwrap();

        // Set some ownership (only works on Unix systems)
        let uid = 1000;
        let gid = 1000;
        unsafe {
            libc::chown(src_file_path.to_str().unwrap().as_bytes().as_ptr() as *const libc::c_char, uid, gid);
        }

        let src_metadata = src_file.metadata().unwrap();

        // Apply set_metadata
        set_file_metadata(&file_path, &src_metadata, true).unwrap();

        // Check that ownership has been updated
        let updated_metadata = file.metadata().unwrap();
        assert_eq!(updated_metadata.uid(), src_metadata.uid());
        assert_eq!(updated_metadata.gid(), src_metadata.gid());
    }
}

#[cfg(test)]
#[cfg(not(unix))]
mod tests {
    use std::fs::{self, File};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_set_metadata_permissions() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");
        File::create(&file_path).unwrap();

        // Set some initial permissions
        let mut perms = File::open(&file_path).unwrap().metadata().unwrap().permissions();
        perms.set_readonly(false);

        fs::set_permissions(&file_path, perms.clone()).unwrap();

        // Create a file with different permissions to copy from
        let src_file_path = dir.path().join("src_file");
        let src_file = File::create(&src_file_path).unwrap();
        let mut src_perms = src_file.metadata().unwrap().permissions();
        src_perms.set_readonly(true);
        fs::set_permissions(&src_file_path, src_perms.clone()).unwrap();

        let src_metadata = src_file.metadata().unwrap();

        // Apply set_metadata
        set_file_metadata(&file_path, &src_metadata, false).unwrap();

        // Check that permissions have been updated.  But we need to re-read some of the things
        // as Unix adds an extra bit indicating a regular file.
        let updated_metadata = File::open(file_path).unwrap().metadata().unwrap();
        let src_metadata = File::open(src_file_path).unwrap().metadata().unwrap();

        assert_eq!(updated_metadata.permissions().readonly(), src_metadata.permissions().readonly());
    }
}
