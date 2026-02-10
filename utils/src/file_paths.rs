use std::ffi::OsString;
use std::fmt::Display;
use std::path::{Path, PathBuf};

use chrono::{DateTime, FixedOffset, Local};

/// A path buffer that can contain template variables like `{PID}` and `{TIMESTAMP}`.
///
/// This type stores a path template and only allows access to the evaluated path
/// after substituting template variables with actual values.
///
/// Supported placeholders (case-insensitive, any case combination allowed):
/// - `{PID}`, `{pid}`, `{Pid}`, etc.: Process ID of the current process
/// - `{TIMESTAMP}`, `{timestamp}`, `{TimeStamp}`, etc.: ISO 8601 timestamp in local timezone with offset (e.g.,
///   `2024-02-05T14-30-45-0500` for EST, `2024-02-05T19-30-45+0000` for UTC)
///
/// The original template is kept private and cannot be accessed directly.
/// Use the `evaluate()` method to get a concrete `PathBuf` with substituted values.
///
/// # Path normalization
///
/// The `evaluate()` method performs the following transformations:
/// - Expands `~` to the user's home directory
/// - Replaces placeholders with actual values (timestamp uses local timezone with offset)
/// - Converts to an absolute path
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TemplatedPathBuf(PathBuf);

impl TemplatedPathBuf {
    /// Creates a new `TemplatedPathBuf` from a path-like value.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        path.into().into()
    }

    /// Convenience function to create and evaluate a template in one step.
    ///
    /// This is equivalent to calling `TemplatedPathBuf::new(path).evaluated()`.
    /// Use this when you don't need to keep the template around.
    pub fn evaluate(path: impl Into<PathBuf>) -> PathBuf {
        Self::new(path).evaluated()
    }

    /// Evaluates the template by replacing all placeholders and expanding paths.
    /// # Examples
    ///
    /// ```
    /// use utils::TemplatedPathBuf;
    ///
    /// let template = TemplatedPathBuf::new("~/logs/app_{PID}_{TIMESTAMP}.txt");
    /// let path = template.evaluated();
    /// // Returns an absolute path like "/home/user/logs/app_12345_2024-01-15T10-30-45-0500.txt"
    /// // (timestamp in local timezone with offset appended)
    /// ```
    pub fn evaluated(&self) -> PathBuf {
        let substitutes: Vec<Box<dyn Substitute>> = vec![
            Box::new(SubstitutePid::default()),
            Box::new(SubstituteTimestamp::default()),
        ];
        self.eval_impl(&substitutes)
    }

    /// Note: Templates can come from environment variables or other non-UTF-8 sources.
    /// We work at the byte level using `into_encoded_bytes()` to preserve all path data
    /// across all platforms (Unix raw bytes, Windows WTF-8), only interpreting ASCII
    /// patterns like {PID} and {TIMESTAMP} which are guaranteed to be ASCII.
    fn eval_impl(&self, substitutes: &[Box<dyn Substitute>]) -> PathBuf {
        // Get platform-specific encoded bytes (Unix: raw bytes, Windows WTF-8)
        let path_bytes = self.0.as_os_str().as_encoded_bytes();

        // One-pass scan to replace placeholders (ASCII patterns)
        let mut result = Vec::with_capacity(path_bytes.len());
        let mut i = 0;

        while i < path_bytes.len() {
            if path_bytes[i] == b'{' {
                // Try to find closing '}'
                if let Some(close_offset) = path_bytes[i + 1..].iter().position(|&b| b == b'}') {
                    let pattern_bytes = &path_bytes[i + 1..i + 1 + close_offset];

                    // Try to match any substitute pattern (ASCII only, case-insensitive)
                    let mut matched = false;
                    if let Ok(pattern_str) = std::str::from_utf8(pattern_bytes) {
                        for sub in substitutes {
                            if sub.matches(pattern_str) {
                                // Found a placeholder, replace it
                                result.extend_from_slice(sub.to_string().as_bytes());
                                i += close_offset + 2; // Skip past {pattern}
                                matched = true;
                                break;
                            }
                        }
                    }

                    if matched {
                        continue;
                    }
                }
            }
            result.push(path_bytes[i]);
            i += 1;
        }

        // Convert back to OsString preserving all bytes
        // SAFETY: The input was a valid OsString, and we only substituted
        // ASCII placeholders with ASCII text, so the output is also a valid OsString.
        let substituted_path = unsafe { OsString::from_encoded_bytes_unchecked(result) };

        // Expand tilde to home directory (preserves non-UTF-8 path data with path feature)
        let expanded = shellexpand::path::tilde(Path::new(&substituted_path));
        let expanded_path = expanded.as_ref();

        // Convert to absolute path
        std::path::absolute(expanded_path).unwrap_or_else(|_| expanded_path.to_path_buf())
    }
}

// Implement From traits for easy conversion
impl From<PathBuf> for TemplatedPathBuf {
    fn from(path: PathBuf) -> Self {
        Self(path)
    }
}

impl From<&Path> for TemplatedPathBuf {
    fn from(path: &Path) -> Self {
        Self(path.to_path_buf())
    }
}

impl From<String> for TemplatedPathBuf {
    fn from(s: String) -> Self {
        Self(PathBuf::from(s))
    }
}

impl From<&str> for TemplatedPathBuf {
    fn from(s: &str) -> Self {
        Self(PathBuf::from(s))
    }
}

trait Substitute: Display {
    fn matches(&self, key: &str) -> bool;
}

struct SubstitutePid(u32);

impl Default for SubstitutePid {
    fn default() -> Self {
        Self(std::process::id())
    }
}

impl Substitute for SubstitutePid {
    fn matches(&self, key: &str) -> bool {
        key.eq_ignore_ascii_case("pid")
    }
}

impl Display for SubstitutePid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

struct SubstituteTimestamp(DateTime<FixedOffset>);

impl Default for SubstituteTimestamp {
    fn default() -> Self {
        Self(Local::now().fixed_offset())
    }
}

impl Substitute for SubstituteTimestamp {
    fn matches(&self, key: &str) -> bool {
        key.eq_ignore_ascii_case("timestamp")
    }
}

impl Display for SubstituteTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format timestamp as ISO 8601 with hyphens instead of colons (filesystem-safe)
        // Format: YYYY-MM-DDTHH-MM-SS+ZZZZ (with timezone offset)
        self.0.format("%Y-%m-%dT%H-%M-%S%z").fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;
    use crate::guards::CwdGuard;
    #[cfg(unix)]
    use crate::guards::EnvVarGuard;

    #[cfg(unix)]
    const HOME_VAR: &str = "HOME";

    #[test]
    fn test_pid_substitution_case_insensitive() {
        // Test that {PID}, {pid}, and {Pid} all work
        for pattern in ["log_{PID}.txt", "log_{pid}.txt", "log_{Pid}.txt"] {
            let template = TemplatedPathBuf::new(pattern);
            let result = template.eval_impl(&[Box::new(SubstitutePid(12345))]);
            assert!(result.ends_with("log_12345.txt"));
        }
    }

    #[test]
    fn test_timestamp_substitution_case_insensitive() {
        // Test that {TIMESTAMP}, {timestamp}, and {TimeStamp} all work
        let timestamp = chrono::DateTime::parse_from_rfc3339("2009-02-13T23:31:30Z").unwrap();
        for pattern in ["log_{TIMESTAMP}.txt", "log_{timestamp}.txt", "log_{TimeStamp}.txt"] {
            let template = TemplatedPathBuf::new(pattern);
            let result = template.eval_impl(&[Box::new(SubstituteTimestamp(timestamp))]);
            assert!(result.ends_with("log_2009-02-13T23-31-30+0000.txt"));
        }
    }

    #[test]
    fn test_multiple_substitutions() {
        // Test that multiple occurrence of placeholders all get substituted
        let template = TemplatedPathBuf::new("/var/log/app_{pid}_{TIMESTAMP}.log");
        let timestamp = chrono::DateTime::parse_from_rfc3339("2009-02-13T23:31:30Z").unwrap();
        let result = template.eval_impl(&[Box::new(SubstitutePid(999)), Box::new(SubstituteTimestamp(timestamp))]);
        #[cfg(unix)]
        assert_eq!(result, PathBuf::from("/var/log/app_999_2009-02-13T23-31-30+0000.log"));
        #[cfg(windows)]
        assert!(result.ends_with("var\\log\\app_999_2009-02-13T23-31-30+0000.log"));

        let template = TemplatedPathBuf::new("/var/log_{pid}/app_{pid}_{TIMESTAMP}.log");
        let timestamp = chrono::DateTime::parse_from_rfc3339("2009-02-13T23:31:30Z").unwrap();
        let result = template.eval_impl(&[Box::new(SubstitutePid(999)), Box::new(SubstituteTimestamp(timestamp))]);
        #[cfg(unix)]
        assert_eq!(result, PathBuf::from("/var/log_999/app_999_2009-02-13T23-31-30+0000.log"));
        #[cfg(windows)]
        assert!(result.ends_with("var\\log_999\\app_999_2009-02-13T23-31-30+0000.log"));
    }

    #[test]
    #[serial(default_config_env)]
    fn makes_relative_path_absolute() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().canonicalize().unwrap();
        let _cwd = CwdGuard::set(&base_path).unwrap();

        let rel = "subdir/{pid}file.txt";
        let got = TemplatedPathBuf::new(rel).eval_impl(&[Box::new(SubstitutePid(2563))]);
        let expected = std::path::absolute(base_path.join("subdir/2563file.txt")).unwrap();

        assert!(got.is_absolute(), "result should be absolute");
        assert_eq!(got, expected);
    }

    #[test]
    fn leaves_absolute_path_absolute() {
        let tmp = tempdir().expect("temp dir");
        let base_path = tmp.path().canonicalize().unwrap();

        let abs_input = base_path.join("a").join("b.txt");
        let expected = std::path::absolute(&abs_input).unwrap();

        let got = TemplatedPathBuf::evaluate(&abs_input);
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
        let got_home = TemplatedPathBuf::evaluate("~");
        assert_eq!(got_home, std::path::absolute(home.path()).unwrap());

        // "~" with a trailing path
        let got_sub = TemplatedPathBuf::new("~/projects/demo_{pid}").eval_impl(&[Box::new(SubstitutePid(123))]);
        let expected_sub = home.path().join("projects").join("demo_123");
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
        let got = TemplatedPathBuf::evaluate(rel);
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
        let got = TemplatedPathBuf::evaluate(inp);
        let expected = std::path::absolute(base_path.join(inp)).unwrap();

        assert!(got.is_absolute());
        assert_eq!(got, expected);
    }
}
