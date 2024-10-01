use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::GeneralPurpose, prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use blake3::Hash;
use error_printer::ErrorPrinter;

use crate::error::ChunkCacheError;

const BASE64_ENGINE: GeneralPurpose = BASE64_URL_SAFE_NO_PAD;
/// A file name is represented as the start index and end index of chunks for the given xorb
/// and a timestamp of last successful access or put
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct FileName {
    pub start_idx: u32,
    pub end_idx: u32,
    pub timestamp: SystemTime,
    pub hash: blake3::Hash,
}

/// length of the total data making up the file name
/// start_index, end_index, timestamp (unix u64), hash of the content
const FILE_NAME_LEN_PRE_BAS64: usize =
    size_of::<u32>() + size_of::<u32>() + size_of::<u64>() + size_of::<blake3::Hash>();

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
        let mut buf = Vec::with_capacity(FILE_NAME_LEN_PRE_BAS64);
        BASE64_ENGINE
            .decode_slice(file_name, &mut buf)
            .map_err(ChunkCacheError::parse)?;
        if buf.len() != FILE_NAME_LEN_PRE_BAS64 {
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

    pub fn as_path_buf(&self) -> PathBuf {
        PathBuf::from(self.to_string())
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
        let mut buf = [0u8; FILE_NAME_LEN_PRE_BAS64];
        buf[0..4].copy_from_slice(&self.start_idx.to_le_bytes());
        buf[4..8].copy_from_slice(&self.end_idx.to_le_bytes());
        buf[8..16].copy_from_slice(&timestamp.to_le_bytes());
        buf[16..].copy_from_slice(self.hash.as_bytes());
        BASE64_ENGINE.encode(buf)
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
