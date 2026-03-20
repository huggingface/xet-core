use serde::{Deserialize, Serialize};
use xet_core_structures::merklehash::{DataHashHexParseError, MerkleHash};
use xet_runtime::error_printer::ErrorPrinter;

/// A struct that wraps a the Xet file information.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct XetFileInfo {
    /// The Merkle hash of the file
    pub hash: String,

    /// The size of the file, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,

    /// The SHA-256 hash of the file, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

impl XetFileInfo {
    /// Creates a new `XetFileInfo` instance with a known size.
    ///
    /// # Arguments
    ///
    /// * `hash` - The Xet hash of the file. This is a Merkle hash string.
    /// * `file_size` - The size of the file.
    pub fn new(hash: String, file_size: u64) -> Self {
        Self {
            hash,
            file_size: Some(file_size),
            sha256: None,
        }
    }

    /// Creates a new `XetFileInfo` instance with a SHA-256 hash and known size.
    pub fn new_with_sha256(hash: String, file_size: u64, sha256: String) -> Self {
        Self {
            hash,
            file_size: Some(file_size),
            sha256: Some(sha256),
        }
    }

    /// Creates a new `XetFileInfo` with only a hash and no known size.
    pub fn new_hash_only(hash: String) -> Self {
        Self {
            hash,
            file_size: None,
            sha256: None,
        }
    }

    /// Returns the Merkle hash of the file.
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// Returns the parsed merkle hash of the file.
    pub fn merkle_hash(&self) -> std::result::Result<MerkleHash, DataHashHexParseError> {
        MerkleHash::from_hex(&self.hash).log_error("Error parsing hash value for file info")
    }

    /// Returns the size of the file, if known.
    pub fn file_size(&self) -> Option<u64> {
        self.file_size
    }

    /// Returns the SHA-256 hash of the file, if available.
    pub fn sha256(&self) -> Option<&str> {
        self.sha256.as_deref()
    }

    pub fn as_pointer_file(&self) -> std::result::Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}
