use merklehash::{DataHashHexParseError, MerkleHash};
use serde::{Deserialize, Serialize};
use tracing::error;

/// A struct that wraps a Xet pointer file.
/// Xet pointer file format is a TOML file,
/// and the first line must be of the form "# xet version <x.y>"
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct XetFileInfo {
    /// The Merkle hash of the file pointed to by this pointer file
    pub hash: String,

    /// The size of the file pointed to by this pointer file
    file_size: usize,
}

impl XetFileInfo {
    /// Creates a new `XetFileInfo` instance.
    ///
    /// # Arguments
    ///
    /// * `hash` - The Merkle hash of the file.
    /// * `filesize` - The size of the file.
    pub fn new(hash: String, file_size: usize) -> Self {
        Self { hash, file_size }
    }

    /// Returns the Merkle hash of the file.
    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn hash_string(&self) -> String {
        self.hash.clone()
    }

    pub fn merkle_hash(&self) -> std::result::Result<MerkleHash, DataHashHexParseError> {
        MerkleHash::from_hex(&self.hash).map_err(|e| {
            error!("Error parsing hash value for file info {e:?}");
            e
        })
    }

    /// Returns the size of the file.
    pub fn file_size(&self) -> usize {
        self.file_size
    }
}
