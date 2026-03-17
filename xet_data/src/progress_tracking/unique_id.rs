use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UniqueID(u64);

impl UniqueID {
    pub fn new() -> Self {
        Self(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for UniqueID {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for UniqueID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_unique_id_basics() {
        let id1 = UniqueID::new();
        let id2 = UniqueID::new();
        assert_ne!(id1, id2);

        let cloned = id1;
        assert_eq!(id1, cloned);
    }

    #[test]
    fn test_unique_id_display() {
        let id = UniqueID::new();
        let s = id.to_string();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_unique_id_hash() {
        let id = UniqueID::new();
        let mut map = HashMap::new();
        map.insert(id, 42);
        assert_eq!(map[&id], 42);
    }
}
