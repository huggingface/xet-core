use blake3;
use merklehash::MerkleHash;

use crate::error::{MerkleDBError, Result};
use crate::merkledb_highlevel_v2::MerkleDBHighLevelMethodsV2;
use crate::merkledbbase::MerkleDBBase;
use crate::{MerkleMemDB, MerkleNode};

// Given a list of hashes and sizes, compute the aggregate hash for a cas node.
pub fn cas_node_hash(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
    // Create an ephemeral MDB.
    if chunks.is_empty() {
        return MerkleHash::default();
    }

    let mut mdb = MerkleMemDB::default();

    let nodes: Vec<MerkleNode> = chunks
        .iter()
        .map(|(h, len)| mdb.maybe_add_node(h, *len, Vec::default()).0)
        .collect();

    let m = mdb.merge_to_cas(&nodes[..]);

    *m.hash()
}

// Given a list of hashes and sizes, compute the aggregate hash for a file node.
pub fn file_node_hash(chunks: &[(MerkleHash, usize)], salt: &[u8; 32]) -> Result<MerkleHash> {
    // Create an ephemeral MDB.
    if chunks.is_empty() {
        return Ok(MerkleHash::default());
    }

    let mut mdb = MerkleMemDB::default();

    let nodes: Vec<MerkleNode> = chunks
        .iter()
        .map(|(h, size)| mdb.maybe_add_node(h, *size, Vec::default()).0)
        .collect();

    let m = mdb.merge_to_file(&nodes[..]);

    with_salt(m.hash(), salt)
}

pub fn with_salt(hash: &MerkleHash, salt: &[u8; 32]) -> Result<MerkleHash> {
    let salted_hash = blake3::keyed_hash(salt, hash.as_bytes());

    let salted_hash = MerkleHash::try_from(salted_hash.as_bytes().as_slice())
        .map_err(|_| MerkleDBError::Other("fail to salt a MerkleHash".to_owned()))?;

    Ok(salted_hash)
}
