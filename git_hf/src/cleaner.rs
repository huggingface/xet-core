//! Selects one file, then streams it into the caller's shared clean session.
use tokio::io::AsyncRead;
use xet_core_structures::merklehash::MerkleHash;
use xet_data::processing::Sha256Policy;

use crate::clean_session::CleanSession;
use crate::filter_protocol::Content;
use crate::selection::{BINARY_LIMIT, BINARY_SAMPLE_SIZE, FILE_LIMIT, TEXT_LIMIT, is_binary};
use crate::{GitHfError, HexHash, MAX_POINTER_SIZE, PointerError, Result, XetPointer};

pub(crate) struct SelectingFileCleaner<'a> {
    session: &'a CleanSession,
    pathname: &'a [u8],
}

impl<'a> SelectingFileCleaner<'a> {
    /// `pathname` is Git's request path, recorded only to report a failed finalization.
    pub(crate) fn new(session: &'a CleanSession, pathname: &'a [u8]) -> Self {
        Self { session, pathname }
    }

    pub(crate) async fn clean(self, input: &mut Content<'_, impl AsyncRead + Unpin>) -> Result<Vec<u8>> {
        // The binary sample covers any pointer; `parse` treats input over MAX_POINTER_SIZE as data.
        const _: () = assert!(BINARY_SAMPLE_SIZE > MAX_POINTER_SIZE);
        let mut prefix = Vec::new();
        input.read_up_to(&mut prefix, BINARY_SAMPLE_SIZE).await?;
        let invalid = |source| GitHfError::InvalidPointer {
            path: String::from_utf8_lossy(self.pathname).into_owned(),
            source,
        };
        let pointer = match XetPointer::parse(&prefix) {
            Ok(pointer) => pointer.map(|_| prefix.clone()),
            // Git's EOL conversion on checkout leaves CRLF in a pointer; undo it as Git's `crlf_to_git`
            // would, so the stored pointer is correct even where Git doesn't convert clean's output.
            Err(PointerError::InvalidEncoding) if prefix.windows(2).any(|w| w == b"\r\n") => {
                let lf = crlf_to_lf(&prefix);
                XetPointer::parse(&lf).map_err(invalid)?.map(|_| lf)
            },
            Err(e) => return Err(invalid(e)),
        };
        if let Some(pointer) = pointer {
            return Ok(pointer);
        }
        let threshold = if is_binary(&prefix) { BINARY_LIMIT } else { TEXT_LIMIT };
        input.read_up_to(&mut prefix, threshold + 1).await?;
        if prefix.len() <= threshold {
            return Ok(prefix);
        }
        let state = self.session.state().await?;
        state.dedup.refresh().await?;
        // Any error below leaves this file's background work running; the caller must exit
        // without finalizing the shared session.
        let (_, mut cleaner) = state.session.start_clean(None, None, Sha256Policy::Skip)?;
        cleaner.add_data(&prefix).await?;
        let mut size = prefix.len() as u64;
        drop(prefix);
        let mut buffer = [0; 65536];
        loop {
            let n = input.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            size += n as u64;
            if size > FILE_LIMIT {
                return Err(GitHfError::NotSupported("File exceeds 500 GB".into()));
            }
            cleaner.add_data(&buffer[..n]).await?;
        }
        let (info, _) = cleaner.finish().await?;
        let hash = MerkleHash::from_hex(&info.hash).map_err(|e| GitHfError::Internal(e.to_string()))?;
        let pointer_hash = HexHash::from_hex(&hash.hex())
            .ok_or_else(|| GitHfError::Internal(format!("file hash {} isn't a pointer hash", info.hash)))?;
        state.record_file(hash, self.pathname)?;
        Ok(XetPointer::new(pointer_hash, size).to_string().into_bytes())
    }
}

/// Replaces each CRLF pair with LF, keeping lone CRs, like Git's `crlf_to_git`.
fn crlf_to_lf(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    for (i, &byte) in data.iter().enumerate() {
        if byte != b'\r' || data.get(i + 1) != Some(&b'\n') {
            out.push(byte);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crlf_to_lf_keeps_lone_carriage_returns() {
        assert_eq!(crlf_to_lf(b"a\r\nb\rc\r\r\n\r"), b"a\nb\rc\r\n\r");
    }
}
