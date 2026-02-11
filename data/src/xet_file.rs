use error_printer::ErrorPrinter;
use merklehash::{DataHashHexParseError, MerkleHash};
use serde::{Deserialize, Serialize};

/// A struct that wraps a the Xet file information.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct XetFileInfo {
    /// The Merkle hash of the file
    hash: String,

    /// The size of the file
    file_size: u64,

    /// The SHA256 hash of the file
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
}

impl XetFileInfo {
    /// Creates a new `XetFileInfo` instance.
    ///
    /// # Arguments
    ///
    /// * `hash` - The Xet hash of the file. This is a Merkle hash string.
    /// * `file_size` - The size of the file.
    pub fn new(hash: String, file_size: u64) -> Self {
        Self {
            hash,
            file_size,
            sha256: None,
        }
    }

    /// Creates a new `XetFileInfo` instance with SHA256.
    ///
    /// # Arguments
    ///
    /// * `hash` - The Xet hash of the file. This is a Merkle hash string.
    /// * `file_size` - The size of the file.
    /// * `sha256` - The SHA256 hash of the file.
    pub fn with_sha256(hash: String, file_size: u64, sha256: String) -> Self {
        Self {
            hash,
            file_size,
            sha256: Some(sha256),
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

    /// Returns the size of the file.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the SHA256 hash of the file, if available.
    pub fn sha256(&self) -> Option<&str> {
        self.sha256.as_deref()
    }

    pub fn as_pointer_file(&self) -> std::result::Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}
