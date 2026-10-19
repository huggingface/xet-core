use std::ffi::OsStr;
use std::io::Read;

use crate::{Result, hook};

/// Runs the chained hook, if any, with Git's arguments and stdin; a failure aborts the push.
/// Uploading staged data is not implemented yet, so otherwise the push proceeds unchanged.
pub(super) fn run(remote: &str, location: &OsStr) -> Result<()> {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    let wd = std::env::current_dir()?;
    hook::run_chained(&wd, &[OsStr::new(remote), location], &input)
}
