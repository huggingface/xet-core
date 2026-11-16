use std::ffi::OsStr;
use std::io::Read;

use crate::Result;

/// Reads Git's ref updates from stdin. Uploading staged data is not implemented yet, so the push
/// proceeds unchanged.
pub(super) fn run(_remote: &str, _location: &OsStr) -> Result<()> {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    Ok(())
}
