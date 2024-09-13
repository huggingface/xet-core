use std::fmt::{Display, Formatter};

use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};

/// A Key indicates a prefixed merkle hash for some data stored in the CAS DB.
#[derive(Debug, PartialEq, Default, Serialize, Deserialize, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct Key {
    pub prefix: String,
    pub hash: MerkleHash,
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:x}", self.prefix, self.hash)
    }
}