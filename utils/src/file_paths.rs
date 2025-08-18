use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// Guard that sets an env var and restores the previous value on drop.
pub struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    pub fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let prev = env::var(key).ok();
        env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(v) = &self.prev {
            env::set_var(self.key, v);
        } else {
            env::remove_var(self.key);
        }
    }
}

/// Guard that sets the current working directory and restores the previous one on drop.
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

/// Normalize a user-provided path string by expanding `~` and returning an absolute path.
///
/// This function expands a leading `~` or `~user` to the user's home directory and converts
/// the path to an absolute path.
///
/// If either fail, the given result is passed through.  
///
/// Notes:
/// - This function does **not** check that the path exists or resolve symlinks.
///
/// # Examples
///
/// Basic usage (omitting error handling details and environment setup):
///
/// ```ignore
/// // "~" expansion depends on the process environment; this example is ignored in doctests.
/// use utils::normalized_path_from_user_string;
///
/// let p = normalized_path_from_user_string("~/project");
/// // `p` is an absolute path under the user's home directory (if expansion succeeded).
/// ```
///
/// Converting a relative path to absolute:
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use utils::normalized_path_from_user_string;
///
/// // Assuming current directory is "/work"
/// let p = normalized_path_from_user_string("data/file.txt");
/// assert!(p.is_absolute());
/// // Typically, p == PathBuf::from("/work/data/file.txt") on Unix-like systems.
/// ```
pub fn normalized_path_from_user_string(path: impl AsRef<str>) -> PathBuf {
    let path = path.as_ref();

    // Expand out the home directory if needed.
    let expanded_path_s = shellexpand::tilde(path);
    let expanded_path = Path::new(expanded_path_s.as_ref());

    std::path::absolute(expanded_path).unwrap_or_else(|_| expanded_path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;

    #[cfg(unix)]
    const HOME_VAR: &str = "HOME";

    #[test]
    #[serial(default_config_env)]
    fn makes_relative_path_absolute() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().canonicalize().unwrap();
        let _cwd = CwdGuard::set(&base_path).unwrap();

        let rel = "subdir/file.txt";
        let got = normalized_path_from_user_string(rel);
        let expected = std::path::absolute(base_path.join(rel)).unwrap();

        assert!(got.is_absolute(), "result should be absolute");
        assert_eq!(got, expected);
    }

    // No env/CWD mutation; can run in parallel.
    #[test]
    #[serial(default_config_env)]
    fn leaves_absolute_path_absolute() {
        let tmp = tempdir().expect("temp dir");
        let base_path = tmp.path().canonicalize().unwrap();

        let abs_input = base_path.join("a").join("b.txt");
        let expected = std::path::absolute(&abs_input).unwrap();

        let got = normalized_path_from_user_string(abs_input.to_string_lossy());
        assert!(got.is_absolute(), "result should be absolute");
        assert_eq!(got, expected);
    }

    #[cfg(unix)] // Windows doesn't work with HOME_VAR
    #[test]
    #[serial(default_config_env)]
    fn expands_tilde_prefix_using_env_home() {
        let home = tempdir().expect("temp home");
        let _home_guard = EnvVarGuard::set(HOME_VAR, home.path());

        let _cwd = CwdGuard::set(home.path()).expect("set cwd");

        // "~" alone
        let got_home = normalized_path_from_user_string("~");
        assert_eq!(got_home, std::path::absolute(home.path()).unwrap());

        // "~" with a trailing path
        let got_sub = normalized_path_from_user_string("~/projects/demo");
        let expected_sub = home.path().join("projects").join("demo");
        assert_eq!(got_sub, expected_sub);
        assert!(got_sub.is_absolute());
    }

    #[test]
    #[serial(default_config_env)]
    fn nonexistent_paths_are_still_absolutized() {
        let tmp = tempdir().expect("temp dir");
        let base_path = tmp.path().canonicalize().unwrap();

        let _cwd = CwdGuard::set(&base_path).expect("set cwd");

        let rel = "does/not/exist/yet";
        let got = normalized_path_from_user_string(rel);
        let expected = std::path::absolute(base_path.join(rel)).unwrap();

        assert!(got.is_absolute());
        assert_eq!(got, expected);
    }

    // "~no-such-user" stays literal and is made absolute relative to CWD.
    #[test]
    #[serial(default_config_env)]
    fn unknown_tilde_user_is_literal_relative() {
        let tmp = tempfile::tempdir().unwrap();
        let base_path = tmp.path().canonicalize().unwrap();
        let _cwd = CwdGuard::set(&base_path).unwrap();

        let inp = "~user_that_definitely_does_not_exist_1234";
        let got = normalized_path_from_user_string(inp);
        let expected = std::path::absolute(base_path.join(inp)).unwrap();

        assert!(got.is_absolute());
        assert_eq!(got, expected);
    }
}
