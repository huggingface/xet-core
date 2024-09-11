use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use merklehash::MerkleHash;

use crate::errors::KeyError;

/// A Key indicates a prefixed merkle hash for some data stored in the CAS DB.
#[derive(Debug, PartialEq, Default, Serialize, Deserialize, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct Key {
    pub prefix: String,
    pub hash: MerkleHash,
}

impl TryFrom<&crate::common::Key> for Key {
    type Error = KeyError;

    fn try_from(proto_key: &crate::common::Key) -> Result<Self, Self::Error> {
        let hash = MerkleHash::try_from(proto_key.hash.as_slice())
            .map_err(|e| KeyError::UnparsableKey(format!("{e:?}")))?;
        Ok(Key {
            prefix: proto_key.prefix.clone(),
            hash,
        })
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:x}", self.prefix, self.hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_proto() {
        let proto = crate::common::Key {
            prefix: "abc".to_string(),
            hash: [1u8; 32].to_vec(),
        };
        let key = Key::try_from(&proto).unwrap();
        assert_eq!(proto.prefix, key.prefix);
        assert_eq!(&proto.hash, key.hash.as_bytes());
    }

    #[test]
    fn test_from_invalid_proto() {
        let proto = crate::common::Key {
            prefix: "".to_string(),
            hash: vec![1, 2, 3],
        };
        let res = Key::try_from(&proto);
        assert!(res.is_err());
        assert!(matches!(res, Err(KeyError::UnparsableKey(..))))
    }

    #[test]
    fn test_display() {
        let proto = crate::common::Key {
            prefix: "abc".to_string(),
            hash: [1u8; 32].to_vec(),
        };
        let key = Key::try_from(&proto).unwrap();
        assert_eq!(
            "abc/0101010101010101010101010101010101010101010101010101010101010101",
            key.to_string()
        );
    }

    #[test]
    fn test_equality() {
        let proto1 = crate::common::Key {
            prefix: "abc".to_string(),
            hash: [1u8; 32].to_vec(),
        };
        let key1 = Key::try_from(&proto1).unwrap();
        assert_eq!(key1, key1);

        let proto2 = crate::common::Key {
            prefix: "abc".to_string(),
            hash: [1u8; 32].to_vec(),
        };
        let key2 = Key::try_from(&proto2).unwrap();
        assert_eq!(key2, key1);
        assert_eq!(key1, key2); // symmetry

        let proto3 = crate::common::Key {
            prefix: "abc".to_string(),
            hash: [1u8; 32].to_vec(),
        };
        let key3 = Key::try_from(&proto3).unwrap();
        assert_eq!(key1, key3);
        assert_eq!(key2, key3); // transitive
    }

    #[test]
    fn test_inequality() {
        let proto1 = crate::common::Key {
            prefix: "abc".to_string(),
            hash: [1u8; 32].to_vec(),
        };
        let key1 = Key::try_from(&proto1).unwrap();

        let proto2 = crate::common::Key {
            prefix: "def".to_string(),
            hash: [1u8; 32].to_vec(),
        };
        let key2 = Key::try_from(&proto2).unwrap();
        assert_ne!(key2, key1);

        let proto3 = crate::common::Key {
            prefix: "abc".to_string(),
            hash: [2u8; 32].to_vec(),
        };
        let key3 = Key::try_from(&proto3).unwrap();
        assert_ne!(key1, key3);
        assert_ne!(key2, key3);
    }
}
