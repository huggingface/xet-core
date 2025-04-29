use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;

#[cfg(unix)]
use colored::Colorize;
use lazy_static::lazy_static;
use tracing::error;
#[cfg(windows)]
use winapi::um::{
    processthreadsapi::GetCurrentProcess,
    processthreadsapi::OpenProcessToken,
    securitybaseapi::GetTokenInformation,
    winnt::{TokenElevation, HANDLE, TOKEN_ELEVATION, TOKEN_QUERY},
};

#[cfg(test)]
static mut WARNING_PRINTED: bool = false;

/// Checks if the program is running under elevated privilege
fn is_elevated_impl() -> bool {
    // In a Unix-like environment, when a program is run with sudo,
    // the effective user ID (euid) of the process is set to 0.
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() == 0 }
    }

    #[cfg(windows)]
    {
        let mut token: HANDLE = std::ptr::null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return false;
        }

        let mut elevation: TOKEN_ELEVATION = unsafe { std::mem::zeroed() };
        let mut return_length = 0;
        let success = unsafe {
            GetTokenInformation(
                token,
                TokenElevation,
                &mut elevation as *mut _ as *mut _,
                std::mem::size_of::<TOKEN_ELEVATION>() as u32,
                &mut return_length,
            )
        };

        if success == 0 {
            false
        } else {
            elevation.TokenIsElevated != 0
        }
    }
}

lazy_static! {
    static ref IS_ELEVATED: bool = is_elevated_impl();
}

pub fn is_elevated() -> bool {
    *IS_ELEVATED
}

// Facts:
// Assume there's a standard user A that is not a root user.
// 1. On Unix systems, suppose there is a path 'dir/f' where 'dir' is created by A but 'f' created by 'sudo A', A can
//    read, rename or remove 'dir/f'. This implies that it's enough to check the permission of 'dir' if we don't
//    directly write into 'dir/f'. This is exactly how we interact with the xorb cache: if an eviction is deemed
//    necessary, the replacement data is written to a tempfile first and then renamed to the to-be-evicted entry. So
//    even if a certain cache file was created by 'sudo A', the eviction by 'A' will succeed.
// 2. On Windows, 'Run as administrator' by logged in user A actually sets %HOMEPATH% to administrator's HOME, so by
//    default the xet metadata folders are isolated. If 'run as admin A' explicility configures cache or repo path to
//    another location owned by A, ACLs for the created path inherit from the parent folder, so A still has full
//    control.

#[derive(Debug, Clone, Copy)]
pub enum PrivilegedExecutionContext {
    Regular,
    Elevated,
}

impl PrivilegedExecutionContext {
    pub fn current() -> PrivilegedExecutionContext {
        match is_elevated() {
            false => PrivilegedExecutionContext::Regular,
            true => PrivilegedExecutionContext::Elevated,
        }
    }

    pub fn is_elevated(&self) -> bool {
        match self {
            PrivilegedExecutionContext::Regular => false,
            PrivilegedExecutionContext::Elevated => true,
        }
    }

    /// Recursively create a directory and all of its parent components if they are missing for write.
    /// If the current process is running with elevated privileges, the entries created
    /// will inherit permission from the path parent.
    pub fn create_dir_all(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        // if path is absolute, cwd is ignored.
        let path = std::env::current_dir()?.join(path);
        let path = path.as_path();

        // first find an ancestor of the path that exists.
        let mut root = path;
        while !root.exists() {
            let Some(pparent) = root.parent() else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Path {root:?} has no parent."),
                ));
            };

            root = pparent;
        }

        // try recursively create all the directories.
        std::fs::create_dir_all(path).inspect_err(|err| {
            if err.kind() == std::io::ErrorKind::PermissionDenied {
                permission_warning(root, true);
            }
        })?;

        // with elevated privileges we chown for all entries from path to root.
        // Permission inheriting from the parent is the default behavior on Windows, thus
        // the below implementation only targets Unix systems.
        #[cfg(unix)]
        if self.is_elevated() {
            let root_meta = std::fs::metadata(root)?;
            let mut path = path;
            while path != root {
                std::os::unix::fs::chown(path, Some(root_meta.uid()), Some(root_meta.gid()))?;
                let Some(pparent) = path.parent() else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Path {path:?} has no parent."),
                    ));
                };
                path = pparent;
            }
        }

        Ok(())
    }

    /// Open or create a file for write.
    /// If the current process is running with elevated privileges, the entries created
    /// will inherit permission from the path parent.
    pub fn create_file(&self, path: impl AsRef<Path>) -> std::io::Result<File> {
        // if path is absolute, cwd is ignored.
        let path = std::env::current_dir()?.join(path);
        let path = path.as_path();

        let Some(pparent) = path.parent() else {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Path {path:?} has no parent.")));
        };

        self.create_dir_all(pparent)?;

        #[allow(unused_variables)]
        let parent_meta = std::fs::metadata(pparent)?;

        #[cfg(unix)]
        let exist = path.exists();

        let create = || {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)
                .inspect_err(|err| {
                    if err.kind() == std::io::ErrorKind::PermissionDenied {
                        permission_warning(path, false);
                    }
                })
        };

        // Test if the current context has write access to the file.
        create()?;

        // exist is only trustable if opening file for R+W succeeded.
        // Permission inheriting from the parent is the default behavior on Windows, thus
        // the below implementation only targets Unix systems.
        #[cfg(unix)]
        if !exist && self.is_elevated() {
            // changes the ownership.
            std::os::unix::fs::chown(path, Some(parent_meta.uid()), Some(parent_meta.gid()))?;
        }

        // Now reopen it.
        create()
    }
}

pub fn create_dir_all(path: impl AsRef<Path>) -> std::io::Result<()> {
    PrivilegedExecutionContext::current().create_dir_all(path)
}

pub fn create_file(path: impl AsRef<Path>) -> std::io::Result<File> {
    PrivilegedExecutionContext::current().create_file(path)
}

#[allow(unused_variables)]
fn permission_warning(path: &Path, recursive: bool) {
    #[cfg(unix)]
    {
        let message = format!(
            "The process doesn't have correct read-write permission into path {path:?}, please resets 
        ownership by 'sudo chown{}{} {path:?}'.",
            if recursive { " -R " } else { " " },
            whoami::username()
        );

        eprintln!("{}", message.bright_blue());
    }

    #[cfg(windows)]
    eprintln!(
        "The process doesn't have correct read-write permission into path {path:?}, please resets
    permission in the Properties dialog box under the Security tab."
    );

    error!("Permission denied for path {path:?}");

    #[cfg(test)]
    unsafe {
        WARNING_PRINTED = true
    };
}

#[cfg(all(test, unix))]
mod test {
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    use super::{PrivilegedExecutionContext, WARNING_PRINTED};

    #[test]
    #[ignore = "run manually"]
    fn test_create_dir_all() -> anyhow::Result<()> {
        // Run this test manually, steps:

        /* For Unix
            1. Run the below shell script in an empty dir with standard privileges.
            2. Set env var 'HF_XET_TEST_PATH' to this path.
            3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
            4. Locate the path to the executable as TEST_EXE
            5. Run test with a non-root user: 'TEST_EXE config::permission::test::test_create_dir_all --exact --nocapture --include-ignored'
            sudo mkdir rootdir
        */

        let test_path = std::env::var("HF_XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;
        let permission = PrivilegedExecutionContext::current();

        let test = Path::new("rootdir/regdir1/regdir2");

        assert!(permission.create_dir_all(test).is_err());
        unsafe { assert!(WARNING_PRINTED) };

        Ok(())
    }

    #[test]
    #[ignore = "run manually"]
    fn test_create_dir_all_sudo() -> anyhow::Result<()> {
        // Run this test manually, steps:

        /* For Unix
            1. Run the below shell script in an empty dir with standard privileges.
            2. Set env var 'HF_XET_TEST_PATH' to this path.
            3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
            4. Locate the path to the executable as TEST_EXE
            5. Run test with a non-root user: 'sudo -E TEST_EXE config::permission::test::test_create_dir_all_sudo --exact --nocapture --include-ignored'

            mkdir regdir
        */

        let test_path = std::env::var("HF_XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;
        let permission = PrivilegedExecutionContext::current();

        let test = Path::new("regdir/regdir1/regdir2");

        permission.create_dir_all(test)?;

        assert!(test.exists());

        // not owned by root
        assert!(std::fs::metadata(test)?.uid() != 0);

        let parent = test.parent().unwrap();

        // parent not owned by root
        assert!(std::fs::metadata(parent)?.uid() != 0);

        Ok(())
    }

    #[test]
    #[ignore = "run manually"]
    fn test_create_file() -> anyhow::Result<()> {
        // Run this test manually, steps:

        /* For Unix
            1. Run the below shell script in an empty dir with standard privileges.
            2. Set env var 'HF_XET_TEST_PATH' to this path.
            3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
            4. Locate the path to the executable as TEST_EXE
            5. Run test with a non-root user: 'TEST_EXE config::permission::test::test_create_file --exact --nocapture --include-ignored'

        sudo mkdir rootdir
        sudo touch rootdir/file
         */

        let test_path = std::env::var("HF_XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;
        let permission = PrivilegedExecutionContext::current();

        let test1 = Path::new("rootdir/regdir1/regdir2/file");

        assert!(permission.create_file(test1).is_err());
        unsafe { assert!(WARNING_PRINTED) };

        unsafe { WARNING_PRINTED = false };

        let test2 = Path::new("rootdir/file");
        assert!(permission.create_file(test2).is_err());
        unsafe { assert!(WARNING_PRINTED) };

        Ok(())
    }

    #[test]
    #[ignore = "run manually"]
    fn test_create_file_sudo() -> anyhow::Result<()> {
        // Run this test manually, steps:

        /* For Unix
            1. Run the below shell script in an empty dir with standard privileges.
            2. Set env var 'HF_XET_TEST_PATH' to this path.
            3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
            4. Locate the path to the executable as TEST_EXE
            5. Run test with a non-root user: 'sudo -E TEST_EXE config::permission::test::test_create_file_sudo --exact --nocapture --include-ignored'
            mkdir regdir
        */

        let test_path = std::env::var("HF_XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;

        let test = Path::new("regdir/regdir1/regdir2/file");

        let permission = PrivilegedExecutionContext::current();
        permission.create_file(test)?;

        assert!(test.exists());

        // not owned by root
        assert!(std::fs::metadata(test)?.uid() != 0);

        let parent = test.parent().unwrap();

        // parent not owned by root
        assert!(std::fs::metadata(parent)?.uid() != 0);

        Ok(())
    }
}
