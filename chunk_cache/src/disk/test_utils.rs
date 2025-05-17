use std::path::Path;

use cas_types::{ChunkRange, Key};
use merklehash::MerkleHash;
use rand::rngs::{StdRng, ThreadRng};
use rand::seq::SliceRandom;
use rand::{rng, Rng, SeedableRng};

#[cfg(test)]
pub const RANGE_LEN: u32 = 16 << 10;
#[cfg(not(test))]
pub const RANGE_LEN: u32 = 16 << 19;

pub fn print_directory_contents(path: &Path) {
    // Read the contents of the directory
    match std::fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        // Print the path
                        println!("{}", path.display());

                        // If it's a directory, call this function recursively
                        if path.is_dir() {
                            print_directory_contents(&path);
                        }
                    },
                    Err(e) => eprintln!("Error reading entry: {e}"),
                }
            }
        },
        Err(e) => eprintln!("Error reading directory: {e}"),
    }
}

pub fn random_key(rng: &mut impl Rng) -> Key {
    Key {
        prefix: "default".to_string(),
        hash: MerkleHash::from_slice(&rng.random::<[u8; 32]>()).unwrap(),
    }
}

pub fn random_range(rng: &mut impl Rng) -> ChunkRange {
    let start = rng.random::<u32>() % 1000;
    let end = start + 1 + rng.random::<u32>() % (1024 - start);
    ChunkRange::new(start, end)
}

pub fn random_bytes(rng: &mut impl Rng, range: &ChunkRange, len: u32) -> (Vec<u32>, Vec<u8>) {
    let random_vec: Vec<u8> = (0..len).map(|_| rng.random()).collect();
    if range.end - range.start == 0 {
        return (vec![0, len], random_vec);
    }

    let mut offsets = Vec::with_capacity((range.end - range.start + 1) as usize);
    offsets.push(0);
    let mut candidates: Vec<u32> = (1..len).collect();
    candidates.shuffle(rng);
    candidates
        .into_iter()
        .take((range.end - range.start - 1) as usize)
        .for_each(|v| offsets.push(v));
    offsets.sort();
    offsets.push(len);

    (offsets.to_vec(), random_vec)
}

#[derive(Debug)]
pub struct RandomEntryIterator<T: Rng> {
    rng: T,
    range_len: u32,
    one_chunk_ranges: bool,
}

impl<T: Rng> RandomEntryIterator<T> {
    pub fn new(rng: T) -> Self {
        Self {
            rng,
            range_len: RANGE_LEN,
            one_chunk_ranges: false,
        }
    }

    pub fn with_range_len(mut self, len: u32) -> Self {
        self.range_len = len;
        self
    }

    // default is false, only use to set to true
    pub fn with_one_chunk_ranges(mut self, one_chunk_ranges: bool) -> Self {
        self.one_chunk_ranges = one_chunk_ranges;
        self
    }

    pub fn next_key_range(&mut self) -> (Key, ChunkRange) {
        (random_key(&mut self.rng), random_range(&mut self.rng))
    }
}

impl<T: SeedableRng + Rng> RandomEntryIterator<T> {
    pub fn from_seed(seed: u64) -> Self {
        Self::new(T::seed_from_u64(seed))
    }
}

impl RandomEntryIterator<StdRng> {
    pub fn std_from_seed(seed: u64) -> Self {
        Self::from_seed(seed)
    }
}

impl Default for RandomEntryIterator<ThreadRng> {
    fn default() -> Self {
        Self::new(rng())
    }
}

impl<T: Rng> Iterator for RandomEntryIterator<T> {
    type Item = (Key, ChunkRange, Vec<u32>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let key = random_key(&mut self.rng);
        let range = if self.one_chunk_ranges {
            let start = self.rng.random();
            ChunkRange::new(start, start + 1)
        } else {
            random_range(&mut self.rng)
        };
        let (offsets, data) = random_bytes(&mut self.rng, &range, self.range_len);
        Some((key, range, offsets, data))
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;

    use super::RandomEntryIterator;

    #[test]
    fn test_iter() {
        let mut it = RandomEntryIterator::default();
        for _ in 0..100 {
            let (_key, range, chunk_byte_indices, data) = it.next().unwrap();
            assert!(range.start < range.end, "invalid range: {range:?}");
            assert_eq!(
                chunk_byte_indices.len(),
                (range.end - range.start + 1) as usize,
                "chunk_byte_indices len mismatch, range: {range:?}, cbi len: {}",
                chunk_byte_indices.len()
            );
            assert_eq!(chunk_byte_indices[0], 0, "chunk_byte_indices[0] != 0, is instead {}", chunk_byte_indices[0]);
            assert_eq!(
                *chunk_byte_indices.last().unwrap() as usize,
                data.len(),
                "chunk_byte_indices last value does not equal data.len() ({}), is instead {}",
                data.len(),
                chunk_byte_indices.last().unwrap()
            );
        }
    }

    #[test]
    fn test_iter_with_seed() {
        const SEED: u64 = 500555;
        let mut it1: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED);
        let mut it2: RandomEntryIterator<StdRng> = RandomEntryIterator::from_seed(SEED);

        for _ in 0..10 {
            let v1 = it1.next().unwrap();
            let v2 = it2.next().unwrap();
            assert_eq!(v1, v2);
        }

        for _ in 0..10 {
            let v1 = it1.next_key_range();
            let v2 = it2.next_key_range();
            assert_eq!(v1, v2);
        }
    }
}
