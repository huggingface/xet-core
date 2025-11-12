use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// Guard that temporarily sets an environment variable and restores the previous value on drop.
///
/// This guard is useful for testing or temporarily modifying environment variables in a
/// controlled manner. When the guard is dropped, it automatically restores the environment
/// variable to its previous state (or removes it if it didn't exist before).
///
/// # Safety
///
/// This struct uses `unsafe` blocks to modify environment variables, which is necessary
/// because Rust's standard library marks `env::set_var` and `env::remove_var` as unsafe
/// due to their potential for data races in multi-threaded contexts. The guard ensures
/// that modifications are properly cleaned up.
///
/// # Examples
///
/// ```no_run
/// use utils::EnvVarGuard;
///
/// // Temporarily set an environment variable
/// let _guard = EnvVarGuard::set("MY_VAR", "test_value");
/// // Environment variable is set here
/// // When _guard is dropped, the previous value (or absence) is restored
/// ```
pub struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    pub fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let prev = env::var(key).ok();
        unsafe {
            env::set_var(key, value);
        }
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(v) = &self.prev {
            unsafe {
                env::set_var(self.key, v);
            }
        } else {
            unsafe {
                env::remove_var(self.key);
            }
        }
    }
}

/// Guard that temporarily changes the current working directory and restores the previous one on drop.
///
/// This guard is useful for testing or temporarily changing the working directory in a
/// controlled manner. When the guard is dropped, it automatically restores the previous
/// working directory.
///
/// # Error Handling
///
/// If restoring the previous directory fails during drop, the error is silently ignored
/// (logged via `let _ = ...`). This is intentional to avoid panicking in destructors.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use utils::CwdGuard;
///
/// // Temporarily change to a different directory
/// let new_dir = Path::new("/tmp");
/// let _guard = CwdGuard::set(new_dir)?;
/// // Current working directory is changed here
/// // When _guard is dropped, the previous directory is restored
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct CwdGuard {
    prev: PathBuf,
}

impl CwdGuard {
    pub fn set(new_dir: &Path) -> std::io::Result<Self> {
        let prev = env::current_dir()?;
        env::set_current_dir(new_dir)?;
        Ok(Self { prev })
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = env::set_current_dir(&self.prev);
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;

    #[test]
    #[serial(default_config_env)]
    fn env_var_guard_sets_and_restores() {
        let key = "TEST_ENV_VAR_GUARD";

        // Set an initial value
        unsafe {
            env::set_var(key, "initial");
        }

        {
            let _guard = EnvVarGuard::set(key, "temporary");
            assert_eq!(env::var(key).unwrap(), "temporary");
        }

        // Should be restored to initial value
        assert_eq!(env::var(key).unwrap(), "initial");
    }

    #[test]
    #[serial(default_config_env)]
    fn env_var_guard_restores_none_when_var_did_not_exist() {
        let key = "TEST_ENV_VAR_GUARD_NEW";

        // Ensure it doesn't exist
        unsafe {
            env::remove_var(key);
        }
        assert!(env::var(key).is_err());

        {
            let _guard = EnvVarGuard::set(key, "temporary");
            assert_eq!(env::var(key).unwrap(), "temporary");
        }

        // Should be removed (didn't exist before)
        assert!(env::var(key).is_err());
    }

    #[test]
    #[serial(default_config_env)]
    fn env_var_guard_multiple_guards_same_key() {
        let key = "TEST_ENV_VAR_GUARD_MULTI";

        unsafe {
            env::set_var(key, "initial");
        }

        {
            let _guard1 = EnvVarGuard::set(key, "first");
            assert_eq!(env::var(key).unwrap(), "first");

            {
                let _guard2 = EnvVarGuard::set(key, "second");
                assert_eq!(env::var(key).unwrap(), "second");
            }

            // Should restore to first guard's value
            assert_eq!(env::var(key).unwrap(), "first");
        }

        // Should restore to initial value
        assert_eq!(env::var(key).unwrap(), "initial");
    }

    #[test]
    #[serial(default_config_env)]
    fn cwd_guard_changes_and_restores() {
        let tmp1 = tempdir().unwrap();
        let tmp2 = tempdir().unwrap();

        let original_dir = env::current_dir().unwrap();

        {
            let _guard = CwdGuard::set(tmp1.path()).unwrap();
            assert_eq!(env::current_dir().unwrap(), tmp1.path().canonicalize().unwrap());

            {
                let _guard2 = CwdGuard::set(tmp2.path()).unwrap();
                assert_eq!(env::current_dir().unwrap(), tmp2.path().canonicalize().unwrap());
            }

            // Should restore to tmp1
            assert_eq!(env::current_dir().unwrap(), tmp1.path().canonicalize().unwrap());
        }

        // Should restore to original directory
        assert_eq!(env::current_dir().unwrap(), original_dir);
    }

    #[test]
    #[serial(default_config_env)]
    fn cwd_guard_handles_nonexistent_directory_error() {
        let nonexistent = Path::new("/nonexistent/path/that/does/not/exist");

        let result = CwdGuard::set(nonexistent);
        assert!(result.is_err());
    }
}
