use std::path::Path;

use cas_types::{Key, Range};
use merklehash::MerkleHash;
use rand::{rngs::ThreadRng, seq::SliceRandom, thread_rng, Rng, SeedableRng};

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
                    }
                    Err(e) => eprintln!("Error reading entry: {}", e),
                }
            }
        }
        Err(e) => eprintln!("Error reading directory: {}", e),
    }
}

pub fn random_key(rng: &mut impl Rng) -> Key {
    Key {
        prefix: "default".to_string(),
        hash: MerkleHash::from_slice(&rng.gen::<[u8; 32]>()).unwrap(),
    }
}

pub fn random_range(rng: &mut impl Rng) -> Range {
    let start = rng.gen::<u32>() % 1000;
    let end = start + 1 + rng.gen::<u32>() % (1024 - start);
    Range { start, end }
}

pub fn random_bytes(rng: &mut impl Rng, range: &Range) -> (Vec<u32>, Vec<u8>) {
    let random_vec: Vec<u8> = (0..RANGE_LEN).map(|_| rng.gen()).collect();

    let mut offsets = Vec::with_capacity((range.end - range.start + 1) as usize);
    offsets.push(0);
    let mut candidates: Vec<u32> = (1..RANGE_LEN).collect();
    candidates.shuffle(rng);
    candidates
        .into_iter()
        .take((range.end - range.start - 1) as usize)
        .for_each(|v| offsets.push(v));
    offsets.sort();
    offsets.push(RANGE_LEN);

    (offsets.to_vec(), random_vec)
}

#[derive(Debug)]
pub struct RandomEntryIterator<T: Rng> {
    rng: T,
    range_len: u32,
}

impl<T: Rng> RandomEntryIterator<T> {
    pub fn new(rng: T) -> Self {
        Self {
            rng,
            range_len: RANGE_LEN,
        }
    }

    pub fn with_range_len(mut self, len: u32) -> Self {
        self.range_len = len;
        self
    }

    pub fn next_key_range(&mut self) -> (Key, Range) {
        (random_key(&mut self.rng), random_range(&mut self.rng))
    }
}

impl<T: SeedableRng + Rng> RandomEntryIterator<T> {
    pub fn from_seed(seed: u64) -> Self {
        Self::new(T::seed_from_u64(seed))
    }
}

impl Default for RandomEntryIterator<ThreadRng> {
    fn default() -> Self {
        Self {
            rng: thread_rng(),
            range_len: RANGE_LEN,
        }
    }
}

impl<T: Rng> Iterator for RandomEntryIterator<T> {
    type Item = (Key, Range, Vec<u32>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let key = random_key(&mut self.rng);
        let range = random_range(&mut self.rng);
        let (offsets, data) = random_bytes(&mut self.rng, &range);
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
            let (_key, range, chunk_byte_indicies, data) = it.next().unwrap();
            assert!(range.start < range.end, "invalid range: {range:?}");
            assert!(
                chunk_byte_indicies.len() == (range.end - range.start + 1) as usize,
                "chunk_byte_indicies len mismatch, range: {range:?}, cbi len: {}",
                chunk_byte_indicies.len()
            );
            assert!(
                chunk_byte_indicies[0] == 0,
                "chunk_byte_indicies[0] != 0, is instead {}",
                chunk_byte_indicies[0]
            );
            assert!(
                *chunk_byte_indicies.last().unwrap() as usize == data.len(),
                "chunk_byte_indicies last value does not equal data.len() ({}), is instead {}",
                data.len(),
                chunk_byte_indicies.last().unwrap()
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
