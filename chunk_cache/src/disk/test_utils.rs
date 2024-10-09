use std::path::Path;

use cas_types::{Key, Range};
use merklehash::MerkleHash;
use rand::{seq::SliceRandom, Rng};

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

pub fn random_key() -> Key {
    Key {
        prefix: "default".to_string(),
        hash: MerkleHash::from_slice(&rand::random::<[u8; 32]>()).unwrap(),
    }
}

pub fn random_range() -> Range {
    let start = rand::random::<u32>() % 1000;
    let end = start + 1 + rand::random::<u32>() % 20;
    Range { start, end }
}

pub fn random_bytes(range: &Range) -> (Vec<u32>, Vec<u8>) {
    let mut rng = rand::thread_rng();
    let random_vec: Vec<u8> = (0..RANGE_LEN).map(|_| rng.gen()).collect();

    let mut offsets = Vec::with_capacity((range.end - range.start + 1) as usize);
    offsets.push(0);
    let mut candidates: Vec<u32> = (1..RANGE_LEN).collect();
    candidates.shuffle(&mut rng);
    candidates
        .into_iter()
        .take((range.end - range.start - 1) as usize)
        .for_each(|v| offsets.push(v));
    offsets.sort();
    offsets.push(RANGE_LEN);

    (offsets.to_vec(), random_vec)
}

pub struct RandomEntryIterator;

impl Iterator for RandomEntryIterator {
    type Item = (Key, Range, Vec<u32>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let key = random_key();
        let range = random_range();
        let (offsets, data) = random_bytes(&range);
        Some((key, range, offsets, data))
    }
}
