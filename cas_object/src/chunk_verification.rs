use merklehash::MerkleHash;

/// The hash key used for generating chunk range hash for shard verification
pub const VERIFICATION_KEY: [u8; 32] = [
    127, 24, 87, 214, 206, 86, 237, 102, 18, 127, 249, 19, 231, 165, 195, 243, 164, 205, 38, 213, 181, 219, 73, 230,
    65, 36, 152, 127, 40, 251, 148, 195,
];

pub fn range_hash_from_chunks(chunks: &[MerkleHash]) -> MerkleHash {
    let combined: Vec<u8> = chunks.iter().flat_map(|hash| hash.as_bytes().to_vec()).collect();

    // now apply hmac to hashes and return
    let range_hash = blake3::keyed_hash(&VERIFICATION_KEY, combined.as_slice());

    MerkleHash::from(range_hash.as_bytes())
}
