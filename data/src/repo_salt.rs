use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;

use crate::constants::REPO_SALT_LEN;

pub type RepoSalt = [u8; REPO_SALT_LEN];

pub fn generate_repo_salt() -> RepoSalt {
    let mut rng = ChaCha20Rng::from_entropy();
    let mut salt = [0u8; REPO_SALT_LEN];
    rng.fill_bytes(&mut salt);

    salt
}
