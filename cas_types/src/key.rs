use crate::error::CasTypesError;
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

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

        let hash =
            MerkleHash::from_hex(hash).map_err(|_| CasTypesError::InvalidKey(s.to_owned()))?;

        Ok(Key {
            prefix: prefix.to_owned(),
            hash,
        })
    }
}

pub mod hex {
    pub mod serde {
        use merklehash::MerkleHash;
        use serde::de::{self, Visitor};
        use serde::{Deserializer, Serializer};
        use std::fmt;

        pub fn serialize<S>(value: &MerkleHash, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let hex = value.hex();
            serializer.serialize_str(&hex)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<MerkleHash, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(HexVisitor)
        }

        // Visitor for deserialization
        struct HexVisitor;

        impl<'de> Visitor<'de> for HexVisitor {
            type Value = MerkleHash;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a merklehash")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                MerkleHash::from_hex(v).map_err(|e| serde::de::Error::custom(e))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct HexMerkleHash(#[serde(with = "hex::serde")] pub MerkleHash);

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
