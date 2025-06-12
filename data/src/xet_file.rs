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
}

impl XetFileInfo {
    /// Creates a new `XetFileInfo` instance.
    ///
    /// # Arguments
    ///
    /// * `hash` - The Xet hash of the file. This is a Merkle hash string.
    /// * `file_size` - The size of the file.
    pub fn new(hash: String, file_size: u64) -> Self {
        Self { hash, file_size }
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

    pub fn as_pointer_file(&self) -> std::result::Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}
