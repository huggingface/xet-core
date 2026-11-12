//! Selects one file, then streams it into the caller's shared clean session.
use std::io::Read;

use xet_core_structures::merklehash::MerkleHash;
use xet_data::processing::Sha256Policy;

use crate::clean_session::CleanSession;
use crate::selection::{BINARY_LIMIT, BINARY_SAMPLE_SIZE, FILE_LIMIT, TEXT_LIMIT, is_binary};
use crate::{GitHfError, HexHash, MAX_POINTER_SIZE, Result, XetPointer};

pub(crate) struct SelectingFileCleaner<'a> {
    session: &'a CleanSession,
    pathname: &'a [u8],
}

impl<'a> SelectingFileCleaner<'a> {
    /// `pathname` is Git's request path, recorded only to report a failed finalization.
    pub(crate) fn new(session: &'a CleanSession, pathname: &'a [u8]) -> Self {
        Self { session, pathname }
    }

    pub(crate) fn clean(self, input: &mut impl Read) -> Result<Vec<u8>> {
        // The binary sample covers any pointer; `parse` treats input over MAX_POINTER_SIZE as data.
        const _: () = assert!(BINARY_SAMPLE_SIZE > MAX_POINTER_SIZE);
        let mut prefix = Vec::new();
        input.take(BINARY_SAMPLE_SIZE as u64).read_to_end(&mut prefix)?;
        if XetPointer::parse(&prefix)?.is_some() {
            return Ok(prefix);
        }
        let threshold = if is_binary(&prefix) { BINARY_LIMIT } else { TEXT_LIMIT };
        let remaining = threshold + 1 - prefix.len();
        input.take(remaining as u64).read_to_end(&mut prefix)?;
        if prefix.len() <= threshold {
            return Ok(prefix);
        }
        let state = self.session.state()?;
        let handle = self.session.ctx.runtime.handle();
        handle.block_on(state.dedup.refresh())?;
        // Any error below leaves this file's background work running; the caller must exit
        // without finalizing the shared session.
        let (_, mut cleaner) = state.session.start_clean(None, None, Sha256Policy::Skip)?;
        handle.block_on(cleaner.add_data(&prefix))?;
        let mut size = prefix.len() as u64;
        drop(prefix);
        let mut buffer = [0; 65536];
        loop {
            let n = input.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            size += n as u64;
            if size > FILE_LIMIT {
                return Err(GitHfError::NotSupported("File exceeds 500 GB".into()));
            }
            handle.block_on(cleaner.add_data(&buffer[..n]))?;
        }
        let (info, _) = handle.block_on(cleaner.finish())?;
        let hash = MerkleHash::from_hex(&info.hash).map_err(|e| GitHfError::Internal(e.to_string()))?;
        let pointer_hash = HexHash::from_hex(&hash.hex())
            .ok_or_else(|| GitHfError::Internal(format!("file hash {} isn't a pointer hash", info.hash)))?;
        state.record_file(hash, self.pathname)?;
        Ok(XetPointer::new(pointer_hash, size).to_string().into_bytes())
    }
}
