#![allow(dead_code)]

use std::time::{SystemTime, UNIX_EPOCH};

pub fn time_to_epoch_millis(ts: SystemTime) -> u128 {
    ts.duration_since(UNIX_EPOCH)
        .map_or(0, |dur| dur.as_millis())
}

/// Helper fn to be used with `scan()` to early terminate an iterator on
/// the first encounter of an error and be able to store the error in a
/// variable. See tests for an example use case of this.
/// Pulled from 2nd answer of: https://stackoverflow.com/questions/26368288/how-do-i-stop-iteration-and-return-an-error-when-iteratormap-returns-a-result

pub fn until_err<T, E>(err: &mut &mut Result<(), E>, item: Result<T, E>) -> Option<T> {
    match item {
        Ok(item) => Some(item),
        Err(e) => {
            **err = Err(e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan() {
        let res: Vec<Result<i32, &str>> = vec![Ok(1), Ok(2), Err("err"), Ok(3)];

        let mut err = Ok(());

        let valid: Vec<i32> = res
            // Note: this has to be into_iter() and not iter(), since we need the
            // actual values for the until_err to work instead of references. If
            // you need to use iter(), you'll need to provide the closure manually.
            .into_iter()
            .scan(&mut err, until_err)
            .collect();
        assert_eq!(err.err().unwrap().to_string(), "err".to_string());
        assert_eq!(valid.len(), 2);
        assert_eq!(valid[0], 1);
        assert_eq!(valid[1], 2);
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::fs::DirEntry;
    use std::path::Path;
    use std::{fs, str};
    use tempdir::TempDir;

    /// Manages a temporary directory for a test. Will be cleaned up when
    /// the struct is dropped.
    pub struct CacheDirTest {
        dir: TempDir,
    }

    impl CacheDirTest {
        pub fn new(dir_prefix: &str) -> Self {
            CacheDirTest {
                dir: TempDir::new(dir_prefix).unwrap(),
            }
        }

        pub fn get_path(&self) -> &Path {
            self.dir.path()
        }

        pub fn get_path_str(&self) -> &str {
            self.get_path().to_str().unwrap()
        }

        pub fn get_entries(&self) -> Vec<DirEntry> {
            fs::read_dir(self.dir.path())
                .unwrap()
                .map(|e| e.unwrap())
                .collect()
        }
    }
}
