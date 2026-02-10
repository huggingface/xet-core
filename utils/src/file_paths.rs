use std::ffi::OsString;
use std::path::{Path, PathBuf};

use chrono::Local;

/// A path buffer that can contain template variables like `{PID}` and `{TIMESTAMP}`.
///
/// This type stores both the original template and its evaluated form. Templates are
/// evaluated on creation and can be re-evaluated to get fresh dynamic values.
///
/// Supported placeholders (case-insensitive, any case combination allowed):
/// - `{PID}`, `{pid}`, `{Pid}`, etc.: Process ID of the current process
/// - `{TIMESTAMP}`, `{timestamp}`, `{TimeStamp}`, etc.: ISO 8601 timestamp in local timezone with offset (e.g.,
///   `2024-02-05T14-30-45-0500` for EST, `2024-02-05T19-30-45+0000` for UTC)
///
/// # Usage
///
/// - Use `new()` to create a `TemplatedPathBuf` that stores both template and evaluated path
/// - Use `as_path()` to get a reference to the evaluated path
/// - Use `re_evaluate()` to refresh dynamic values like `{TIMESTAMP}`
/// - Use `evaluate()` static method for one-time evaluation without keeping the template
///
/// # Path normalization
///
/// Path evaluation performs the following transformations:
/// - Expands `~` to the user's home directory
/// - Replaces placeholders with actual values (timestamp uses local timezone with offset)
/// - Converts to an absolute path
#[derive(Clone, Debug)]
pub struct TemplatedPathBuf {
    template: PathBuf,
    evaluated: PathBuf,
}

impl TemplatedPathBuf {
    /// Creates a new `TemplatedPathBuf` from a path-like value.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let template = path.into();
        let evaluated = Self::eval_impl(&template, &Self::default_substitutes());

        Self { template, evaluated }
    }

    /// Convenience function to create and evaluate a template in one step.
    ///
    /// This is equivalent to calling `TemplatedPathBuf::new(path).as_path().to_path_buf()`.
    /// Use this when you don't need to keep the template around.
    pub fn evaluate(path: impl Into<PathBuf>) -> PathBuf {
        Self::new(path).as_path().into()
    }

    /// Returns a reference to the evaluated path.
    ///
    /// The evaluated path has all template variables substituted with their actual values,
    /// tilde expansion applied, and is converted to an absolute path.
    pub fn as_path(&self) -> &Path {
        &self.evaluated
    }

    /// Re-evaluates the template by replacing all placeholders with fresh values and expanding paths.
    ///
    /// This method updates the internal evaluated path and returns a reference to it.
    /// Call this if you need to refresh dynamic values like `{PID}` or `{TIMESTAMP}`.
    ///
    /// # Examples
    ///
    /// ```
    /// use utils::TemplatedPathBuf;
    ///
    /// let mut template = TemplatedPathBuf::new("~/logs/app_{PID}_{TIMESTAMP}.txt");
    /// let path = template.as_path();
    /// // Returns an absolute path like "/home/user/logs/app_12345_2024-01-15T10-30-45-0500.txt"
    /// // (timestamp in local timezone with offset appended)
    ///
    /// // Later, re-evaluate to get a fresh timestamp
    /// template.re_evaluate();
    /// let new_path = template.as_path();
    /// ```
    pub fn re_evaluate(&mut self) -> &Path {
        self.evaluated = Self::eval_impl(&self.template, &Self::default_substitutes());
        &self.evaluated
    }

    fn default_substitutes() -> [Substitute; 2] {
        [
            ("pid", Box::new(|| std::process::id().to_string())),
            ("timestamp", Box::new(|| Local::now().fixed_offset().format("%Y-%m-%dT%H-%M-%S%z").to_string())),
        ]
    }

    /// Note: Templates can come from environment variables or other non-UTF-8 sources.
    /// We work at the byte level using `into_encoded_bytes()` to preserve all path data
    /// across all platforms (Unix raw bytes, Windows WTF-8), only interpreting ASCII
    /// patterns like {PID} and {TIMESTAMP} which are guaranteed to be ASCII.
    fn eval_impl(template: &Path, substitutes: &[Substitute]) -> PathBuf {
        // Get platform-specific encoded bytes (Unix: raw bytes, Windows WTF-8)
        let path_bytes = template.as_os_str().as_encoded_bytes();

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
                            if pattern_str.eq_ignore_ascii_case(sub.0) {
                                // Found a placeholder, replace it
                                result.extend_from_slice(sub.1().as_bytes());
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
        Self::new(path)
    }
}

impl From<&Path> for TemplatedPathBuf {
    fn from(path: &Path) -> Self {
        Self::new(path.to_path_buf())
    }
}

impl From<String> for TemplatedPathBuf {
    fn from(s: String) -> Self {
        Self::new(PathBuf::from(s))
    }
}

impl From<&str> for TemplatedPathBuf {
    fn from(s: &str) -> Self {
        Self::new(PathBuf::from(s))
    }
}

type Substitute = (&'static str, Box<dyn Fn() -> String>);

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        let substitutes: [Substitute; 1] = [("pid", Box::new(|| "12345".to_string()))];
        for pattern in ["log_{PID}.txt", "log_{pid}.txt", "log_{Pid}.txt"] {
            let result = TemplatedPathBuf::eval_impl(Path::new(pattern), &substitutes);
            assert!(result.ends_with("log_12345.txt"));
        }
    }

    #[test]
    fn test_timestamp_substitution_case_insensitive() {
        // Test that {TIMESTAMP}, {timestamp}, and {TimeStamp} all work
        let timestamp = chrono::DateTime::parse_from_rfc3339("2009-02-13T23:31:30Z").unwrap();
        let substitutes: [Substitute; 1] =
            [("timestamp", Box::new(move || timestamp.format("%Y-%m-%dT%H-%M-%S%z").to_string()))];
        for pattern in ["log_{TIMESTAMP}.txt", "log_{timestamp}.txt", "log_{TimeStamp}.txt"] {
            let result = TemplatedPathBuf::eval_impl(Path::new(pattern), &substitutes);
            assert!(result.ends_with("log_2009-02-13T23-31-30+0000.txt"));
        }
    }

    #[test]
    fn test_multiple_substitutions() {
        // Test that multiple occurrence of placeholders all get substituted
        let timestamp = chrono::DateTime::parse_from_rfc3339("2009-02-13T23:31:30Z").unwrap();
        let substitutes: [Substitute; 2] = [
            ("pid", Box::new(|| "999".to_string())),
            ("timestamp", Box::new(move || timestamp.format("%Y-%m-%dT%H-%M-%S%z").to_string())),
        ];

        let result = TemplatedPathBuf::eval_impl(Path::new("/var/log/app_{pid}_{TIMESTAMP}.log"), &substitutes);
        #[cfg(unix)]
        assert_eq!(result, PathBuf::from("/var/log/app_999_2009-02-13T23-31-30+0000.log"));
        #[cfg(windows)]
        assert!(result.ends_with("var\\log\\app_999_2009-02-13T23-31-30+0000.log"));

        let result = TemplatedPathBuf::eval_impl(Path::new("/var/log_{pid}/app_{pid}_{TIMESTAMP}.log"), &substitutes);
        #[cfg(unix)]
        assert_eq!(result, PathBuf::from("/var/log_999/app_999_2009-02-13T23-31-30+0000.log"));
        #[cfg(windows)]
        assert!(result.ends_with("var\\log_999\\app_999_2009-02-13T23-31-30+0000.log"));
    }

    #[test]
    fn test_non_ascii_paths_substitutions() {
        let substitutes: [Substitute; 1] = [("pid", Box::new(|| "566".to_string()))];
        let result =
            TemplatedPathBuf::eval_impl(Path::new("-Me {pid} encantan los 🌶️ jalapeños . -我也喜欢"), &substitutes);
        assert!(result.ends_with("-Me 566 encantan los 🌶️ jalapeños . -我也喜欢"));
    }

    #[test]
    fn leaves_unrecognized_patterns_unsubstituted() {
        let template = Path::new("path_with_{unrecognized}_{patterns}.txt");
        let result = TemplatedPathBuf::evaluate(template);
        assert!(result.ends_with(template));

        let template = Path::new("path_with_{未识别的}_{patterns}.txt");
        let result = TemplatedPathBuf::evaluate(template);
        assert!(result.ends_with(template));
    }

    #[test]
    fn test_as_path_and_re_evaluate() {
        // Test as_path() returns the evaluated path with substitutions
        let mut template = TemplatedPathBuf::new("/var/log/app_{PID}_{TIMESTAMP}.log");

        // as_path() should return an absolute path with placeholders substituted
        let path1 = template.as_path();
        assert!(path1.is_absolute(), "Path should be absolute");

        // Verify path contains expected structure
        let path_str = path1.to_string_lossy();
        let pid = std::process::id();
        assert!(path_str.contains(&format!("app_{pid}")));
        assert!(!path_str.contains("{TIMESTAMP}"), "TIMESTAMP placeholder should be substituted");

        // Multiple calls to as_path() without re-evaluate should return same path
        let path2 = template.as_path().to_path_buf();
        assert_eq!(path1, &path2);

        std::thread::sleep(Duration::from_secs(1));

        // re_evaluate() should update the evaluated path and return a reference to it
        let path3 = template.re_evaluate().to_path_buf();
        assert_ne!(path3, path2, "re_evaluate() didn't return new result");

        // as_path() after re_evaluate() should return the updated path
        let path4 = template.as_path();
        assert_eq!(path3, path4);
    }

    #[test]
    #[serial(default_config_env)]
    fn makes_relative_path_absolute() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().canonicalize().unwrap();
        let _cwd = CwdGuard::set(&base_path).unwrap();

        let substitutes: [Substitute; 1] = [("pid", Box::new(|| "2563".to_string()))];
        let got = TemplatedPathBuf::eval_impl(Path::new("subdir/{pid}file.txt"), &substitutes);
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
        let substitutes: [Substitute; 1] = [("pid", Box::new(|| "123".to_string()))];
        let got_sub = TemplatedPathBuf::eval_impl(Path::new("~/projects/demo_{pid}"), &substitutes);
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
