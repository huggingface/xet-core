use merklehash::MerkleHash;

pub fn range_hash_from_chunks(chunks: &[MerkleHash], key: &[u8; 32]) -> MerkleHash {
    let combined: Vec<u8> = chunks
        .iter()
        .flat_map(|hash| hash.as_bytes().to_vec())
        .collect();

    // now apply hmac to hashes and return
    let range_hash = blake3::keyed_hash(key, combined.as_slice());

    MerkleHash::from(range_hash.as_bytes())
}
