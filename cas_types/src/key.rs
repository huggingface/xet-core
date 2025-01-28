use std::fmt::{Display, Formatter};
use std::str::FromStr;

use merklehash::data_hash::hex;
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};

use crate::error::CasTypesError;

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

impl FromStr for Key {
    type Err = CasTypesError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.rsplit_once('/');
        let Some((prefix, hash)) = parts else {
            return Err(CasTypesError::InvalidKey(s.to_owned()));
        };

        let hash = MerkleHash::from_hex(hash).map_err(|_| CasTypesError::InvalidKey(s.to_owned()))?;

        Ok(Key {
            prefix: prefix.to_owned(),
            hash,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
pub struct HexMerkleHash(#[serde(with = "hex::serde")] pub MerkleHash);

impl Display for HexMerkleHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.hex())
    }
}

impl From<MerkleHash> for HexMerkleHash {
    fn from(value: MerkleHash) -> Self {
        HexMerkleHash(value)
    }
}

impl From<HexMerkleHash> for MerkleHash {
    fn from(value: HexMerkleHash) -> Self {
        value.0
    }
}

impl From<&HexMerkleHash> for MerkleHash {
    fn from(value: &HexMerkleHash) -> Self {
        value.0
    }
}

impl From<&MerkleHash> for HexMerkleHash {
    fn from(value: &MerkleHash) -> Self {
        HexMerkleHash(*value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Hash, PartialEq, Eq)]
pub struct HexKey {
    pub prefix: String,
    #[serde(with = "hex::serde")]
    pub hash: MerkleHash,
}

impl From<HexKey> for Key {
    fn from(HexKey { prefix, hash }: HexKey) -> Self {
        Key { prefix, hash }
    }
}

impl From<Key> for HexKey {
    fn from(Key { prefix, hash }: Key) -> Self {
        HexKey { prefix, hash }
    }
}
