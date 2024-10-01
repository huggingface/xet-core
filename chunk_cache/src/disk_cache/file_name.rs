use std::{mem::size_of, path::PathBuf};

use base64::{engine::GeneralPurpose, prelude::BASE64_URL_SAFE, Engine};
use blake3::Hash;

use crate::error::ChunkCacheError;

const BASE64_ENGINE: GeneralPurpose = BASE64_URL_SAFE;
/// A file name is represented as the start index and end index of chunks for the given xorb
/// and a timestamp of last successful access or put
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct FileName {
    pub start_idx: u32,
    pub end_idx: u32,
    pub hash: blake3::Hash,
}

/// length of the total data making up the file name
/// start_index, end_index, hash of the content
const FILE_NAME_LEN_PRE_BAS64: usize =
    size_of::<u32>() + size_of::<u32>() + size_of::<blake3::Hash>();

impl FileName {
    pub fn new(start_idx: u32, end_idx: u32, hash: Hash) -> Self {
        Self {
            start_idx,
            end_idx,
            hash,
        }
    }

    pub fn try_parse<T: AsRef<[u8]>>(file_name: T) -> Result<FileName, ChunkCacheError> {
        let buf = BASE64_ENGINE
            .decode(file_name)
            .map_err(ChunkCacheError::parse)?;
        if buf.len() != FILE_NAME_LEN_PRE_BAS64 {
            return Err(ChunkCacheError::parse("invalid size of decoded buffer"));
        }
        let start_idx = u32::from_le_bytes(buf[0..4].try_into()?);
        let end_idx = u32::from_le_bytes(buf[4..8].try_into()?);
        let hash = blake3::Hash::from_bytes(buf[8..].try_into()?);

        Ok(FileName {
            start_idx,
            end_idx,
            hash,
        })
    }
}

impl From<&FileName> for String {
    fn from(value: &FileName) -> Self {
        let mut buf = [0u8; FILE_NAME_LEN_PRE_BAS64];
        buf[0..4].copy_from_slice(&value.start_idx.to_le_bytes());
        buf[4..8].copy_from_slice(&value.end_idx.to_le_bytes());
        buf[8..].copy_from_slice(value.hash.as_bytes());
        BASE64_ENGINE.encode(buf)
    }
}

impl From<&FileName> for PathBuf {
    fn from(value: &FileName) -> Self {
        PathBuf::from(String::from(value))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use blake3::{Hash, OUT_LEN};

    use super::FileName;

    #[test]
    fn test_encode_decode() {
        let f = FileName {
            start_idx: 100,
            end_idx: 1000,
            hash: Hash::from_bytes([6u8; OUT_LEN]),
        };
        let encoded: PathBuf = (&f).into();
        let decoded_result = FileName::try_parse(encoded.file_name().unwrap().as_encoded_bytes());
        assert!(decoded_result.is_ok());
        assert_eq!(decoded_result.unwrap(), f);
    }
}
