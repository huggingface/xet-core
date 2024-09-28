use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{prelude::BASE64_STANDARD_NO_PAD, Engine};
use blake3::Hash;
use error_printer::ErrorPrinter;

use crate::error::ChunkCacheError;

/// A file name is represented as the start index and end index of chunks for the given xorb
/// and a timestamp of last successful access or put
#[derive(Debug)]
pub struct FileName {
    pub start_idx: u32,
    pub end_idx: u32,
    pub timestamp: SystemTime,
    pub hash: blake3::Hash,
}

const FILE_NAME_LEN: usize = size_of::<u32>() * 2 + size_of::<u64>() + size_of::<blake3::Hash>();

impl FileName {
    pub fn new(start_idx: u32, end_idx: u32, timestamp: SystemTime, hash: Hash) -> Self {
        Self {
            start_idx,
            end_idx,
            timestamp,
            hash,
        }
    }

    pub fn try_parse<T: AsRef<[u8]>>(file_name: T) -> Result<FileName, ChunkCacheError> {
        let mut buf = Vec::with_capacity(FILE_NAME_LEN);
        BASE64_STANDARD_NO_PAD
            .decode_slice(file_name, &mut buf)
            .map_err(ChunkCacheError::parse)?;
        if buf.len() != FILE_NAME_LEN {
            return Err(ChunkCacheError::parse("invalid size of decoded buffer"));
        }
        let start_idx = u32::from_le_bytes(buf[0..4].try_into()?);
        let end_idx = u32::from_le_bytes(buf[4..8].try_into()?);
        let timestamp =
            UNIX_EPOCH + Duration::from_millis(u64::from_le_bytes(buf[8..16].try_into()?));
        let hash = blake3::Hash::from_bytes(buf[16..].try_into()?);

        Ok(FileName {
            start_idx,
            end_idx,
            timestamp,
            hash,
        })
    }
}

impl Into<String> for &FileName {
    fn into(self) -> String {
        let timestamp = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .log_error("filename has an invalid timestamp")
            .unwrap_or_default()
            .as_millis() as u64;
        let mut buf = [0u8; FILE_NAME_LEN];
        buf[0..4].copy_from_slice(&self.start_idx.to_le_bytes());
        buf[4..8].copy_from_slice(&self.end_idx.to_le_bytes());
        buf[8..16].copy_from_slice(&timestamp.to_le_bytes());
        buf[16..].copy_from_slice(self.hash.as_bytes());
        BASE64_STANDARD_NO_PAD.encode(buf)
    }
}

impl ToString for FileName {
    fn to_string(&self) -> String {
        self.into()
    }
}

impl Into<PathBuf> for &FileName {
    fn into(self) -> PathBuf {
        PathBuf::from(self.to_string())
    }
}
