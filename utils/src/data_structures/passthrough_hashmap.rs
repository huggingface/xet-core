use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use super::passthrough_hasher::{U64DirectHasher, U64HashExtractable};

/// A HashMap wrapper optimized for keys that implement `U64HashExtractable`.
///
/// This structure uses `U64DirectHasher` which avoids extra hash computation
/// by directly using the u64 value extracted from the key. This is particularly
/// efficient for cryptographic hashes (like `MerkleHash`) where the first 8 bytes
/// already provide excellent distribution.
///
/// # Behavior
///
/// This type implements `Deref` and `DerefMut` to the underlying `HashMap`, so it
/// behaves exactly like a standard `HashMap`. All `HashMap` methods are available
/// directly on this type:
///
/// ```ignore
/// let mut map: PassThroughHashMap<MerkleHash, String> = PassThroughHashMap::new();
///
/// // All HashMap methods work directly:
/// map.insert(key, "value".to_string());
/// map.get(&key);
/// map.contains_key(&key);
/// map.remove(&key);
/// map.len();
/// map.is_empty();
/// map.iter();
/// map.entry(key).or_insert("default".to_string());
/// // ... and all other HashMap methods
/// ```
///
/// # Type Parameters
/// - `Key`: The key type (must implement `U64HashExtractable + Hash + Eq`)
/// - `Value`: The value type stored in the map
pub struct PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    inner: HashMap<Key, Value, U64DirectHasher<Key>>,
}

impl<Key, Value> PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    pub fn new() -> Self {
        Self {
            inner: HashMap::with_hasher(U64DirectHasher::default()),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: HashMap::with_capacity_and_hasher(capacity, U64DirectHasher::default()),
        }
    }
}

impl<Key, Value> Default for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Key, Value> std::fmt::Debug for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq + std::fmt::Debug,
    Value: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.inner.iter()).finish()
    }
}

impl<Key, Value> Clone for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq + Clone,
    Value: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Key, Value> std::iter::FromIterator<(Key, Value)> for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    fn from_iter<I: IntoIterator<Item = (Key, Value)>>(iter: I) -> Self {
        let mut map = Self::new();
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

impl<Key, Value> IntoIterator for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    type Item = (Key, Value);
    type IntoIter = std::collections::hash_map::IntoIter<Key, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

/// Provides immutable access to all `HashMap` methods.
///
/// This allows using `PassThroughHashMap` exactly like a `HashMap`:
/// `get`, `contains_key`, `len`, `is_empty`, `iter`, `keys`, `values`, etc.
impl<Key, Value> Deref for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    type Target = HashMap<Key, Value, U64DirectHasher<Key>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Provides mutable access to all `HashMap` methods.
///
/// This allows using `PassThroughHashMap` exactly like a `HashMap`:
/// `insert`, `remove`, `clear`, `get_mut`, `entry`, `iter_mut`, etc.
impl<Key, Value> DerefMut for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Key, Value> From<HashMap<Key, Value, U64DirectHasher<Key>>> for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    fn from(inner: HashMap<Key, Value, U64DirectHasher<Key>>) -> Self {
        Self { inner }
    }
}

impl<Key, Value> From<PassThroughHashMap<Key, Value>> for HashMap<Key, Value, U64DirectHasher<Key>>
where
    Key: U64HashExtractable + Hash + Eq,
{
    fn from(val: PassThroughHashMap<Key, Value>) -> Self {
        val.inner
    }
}

impl<Key, Value, const N: usize> From<[(Key, Value); N]> for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq,
{
    fn from(arr: [(Key, Value); N]) -> Self {
        arr.into_iter().collect()
    }
}

impl<Key, Value> Serialize for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq + Serialize,
    Value: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.inner.len()))?;
        for (key, value) in self.inner.iter() {
            seq.serialize_element(&(key, value))?;
        }
        seq.end()
    }
}

impl<'de, Key, Value> Deserialize<'de> for PassThroughHashMap<Key, Value>
where
    Key: U64HashExtractable + Hash + Eq + Deserialize<'de>,
    Value: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PassThroughVisitor<Key, Value> {
            _marker: std::marker::PhantomData<(Key, Value)>,
        }

        impl<'de, Key, Value> serde::de::Visitor<'de> for PassThroughVisitor<Key, Value>
        where
            Key: U64HashExtractable + Hash + Eq + Deserialize<'de>,
            Value: Deserialize<'de>,
        {
            type Value = PassThroughHashMap<Key, Value>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence of key-value pairs")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let capacity = seq.size_hint().unwrap_or(0);
                let mut map = PassThroughHashMap::with_capacity(capacity);

                while let Some((key, value)) = seq.next_element()? {
                    map.insert(key, value);
                }

                Ok(map)
            }
        }

        deserializer.deserialize_seq(PassThroughVisitor {
            _marker: std::marker::PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use merklehash::{DataHash, compute_data_hash};

    use super::*;

    type MerkleHashMap<Value> = PassThroughHashMap<DataHash, Value>;

    #[test]
    fn test_new() {
        let table: MerkleHashMap<String> = MerkleHashMap::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_default() {
        let table: MerkleHashMap<i32> = MerkleHashMap::default();
        assert!(table.is_empty());
    }

    #[test]
    fn test_with_capacity() {
        let table: MerkleHashMap<u64> = MerkleHashMap::with_capacity(100);
        assert!(table.is_empty());
    }

    #[test]
    fn test_insert_and_get() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        assert_eq!(table.insert(hash1, "value1"), None);
        assert_eq!(table.insert(hash2, "value2"), None);
        assert_eq!(table.len(), 2);

        assert_eq!(table.get(&hash1), Some(&"value1"));
        assert_eq!(table.get(&hash2), Some(&"value2"));
    }

    #[test]
    fn test_insert_overwrite() {
        let mut table = MerkleHashMap::new();
        let hash = compute_data_hash(b"test");

        assert_eq!(table.insert(hash, "value1"), None);
        assert_eq!(table.insert(hash, "value2"), Some("value1"));
        assert_eq!(table.get(&hash), Some(&"value2"));
    }

    #[test]
    fn test_contains_key() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        table.insert(hash1, 42);
        assert!(table.contains_key(&hash1));
        assert!(!table.contains_key(&hash2));
    }

    #[test]
    fn test_remove() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        table.insert(hash1, "value1");
        table.insert(hash2, "value2");
        assert_eq!(table.len(), 2);

        assert_eq!(table.remove(&hash1), Some("value1"));
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&hash1), None);
        assert_eq!(table.get(&hash2), Some(&"value2"));
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut table: MerkleHashMap<&str> = MerkleHashMap::new();
        let hash = compute_data_hash(b"test");

        assert_eq!(table.remove(&hash), None);
    }

    #[test]
    fn test_clear() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        table.insert(hash1, "value1");
        table.insert(hash2, "value2");
        assert_eq!(table.len(), 2);

        table.clear();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_iter() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        table.insert(hash1, 10);
        table.insert(hash2, 20);

        let items: Vec<_> = table.iter().collect();
        assert_eq!(items.len(), 2);

        let values: Vec<_> = items.iter().map(|(_, v)| **v).collect();
        assert!(values.contains(&10));
        assert!(values.contains(&20));
    }

    #[test]
    fn test_iter_mut() {
        let mut table = MerkleHashMap::new();
        let hash = compute_data_hash(b"test");

        table.insert(hash, 10);
        *table.get_mut(&hash).unwrap() = 20;
        assert_eq!(table.get(&hash), Some(&20));
    }

    #[test]
    fn test_keys() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        table.insert(hash1, "value1");
        table.insert(hash2, "value2");

        let keys: Vec<_> = table.keys().collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&&hash1));
        assert!(keys.contains(&&hash2));
    }

    #[test]
    fn test_values() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        table.insert(hash1, "value1");
        table.insert(hash2, "value2");

        let values: Vec<_> = table.values().collect();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&&"value1"));
        assert!(values.contains(&&"value2"));
    }

    #[test]
    fn test_entry() {
        let mut table = MerkleHashMap::new();
        let hash = compute_data_hash(b"test");

        table.entry(hash).or_insert("default");
        assert_eq!(table.get(&hash), Some(&"default"));

        table.entry(hash).and_modify(|v| *v = "modified");
        assert_eq!(table.get(&hash), Some(&"modified"));
    }

    #[test]
    fn test_from_hashmap() {
        let mut hashmap = HashMap::with_hasher(U64DirectHasher::<DataHash>::default());
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        hashmap.insert(hash1, 10);
        hashmap.insert(hash2, 20);

        let table: MerkleHashMap<i32> = MerkleHashMap::from(hashmap);
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&hash1), Some(&10));
        assert_eq!(table.get(&hash2), Some(&20));
    }

    #[test]
    fn test_into_hashmap() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");

        table.insert(hash1, 10);
        table.insert(hash2, 20);

        let hashmap: HashMap<DataHash, i32, U64DirectHasher<DataHash>> = table.into();
        assert_eq!(hashmap.len(), 2);
        assert_eq!(hashmap.get(&hash1), Some(&10));
        assert_eq!(hashmap.get(&hash2), Some(&20));
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut table = MerkleHashMap::new();
        let hash1 = compute_data_hash(b"test1");
        let hash2 = compute_data_hash(b"test2");
        let hash3 = compute_data_hash(b"test3");

        table.insert(hash1, 10);
        table.insert(hash2, 20);
        table.insert(hash3, 30);

        let serialized = bincode::serialize(&table).unwrap();
        let deserialized: MerkleHashMap<i32> = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.len(), 3);
        assert_eq!(deserialized.get(&hash1), Some(&10));
        assert_eq!(deserialized.get(&hash2), Some(&20));
        assert_eq!(deserialized.get(&hash3), Some(&30));
    }

    #[test]
    fn test_serialize_deserialize_empty() {
        let table: MerkleHashMap<i32> = MerkleHashMap::new();

        let serialized = bincode::serialize(&table).unwrap();
        let deserialized: MerkleHashMap<i32> = bincode::deserialize(&serialized).unwrap();

        assert!(deserialized.is_empty());
        assert_eq!(deserialized.len(), 0);
    }

    #[test]
    fn test_u64_key_hashmap() {
        type TruncatedHashMap<Value> = PassThroughHashMap<u64, Value>;

        let mut table: TruncatedHashMap<String> = TruncatedHashMap::new();
        table.insert(12345, "value1".to_string());
        table.insert(67890, "value2".to_string());

        assert_eq!(table.get(&12345), Some(&"value1".to_string()));
        assert_eq!(table.get(&67890), Some(&"value2".to_string()));
        assert_eq!(table.len(), 2);
    }
}
