//! Native Xet V2 Git client.

mod auth;
mod cas_token;
mod clean_session;
mod cleaner;
mod cli;
mod constants;
mod error;
mod filter_protocol;
mod hook;
mod push;
mod selection;
mod staging;
mod utils;

pub use cli::GitHfApp;
pub use constants::GIT_HF_PROGRAM;
pub use error::{GitHfError, Result};
pub use xet_pointer_file::{HexHash, MAX_POINTER_SIZE, PointerError, XetPointer};

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;

    // Pointer hashes and MerkleHash hex must convert both ways without loss.
    #[test]
    fn pointer_hashes_round_trip_through_merkle_hash() {
        for hash in [
            MerkleHash::default(),
            MerkleHash::from([1, 2, 3, 4]),
            MerkleHash::from([u64::MAX; 4]),
        ] {
            let hex = HexHash::from_hex(&hash.hex()).unwrap();
            assert_eq!(MerkleHash::from_hex(hex.as_str()).unwrap(), hash);
        }
    }
}
