use rand::{rng, RngCore};
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha20Rng;

// Salt is 256-bit.
pub const REPO_SALT_LEN: usize = 32;

pub type RepoSalt = [u8; REPO_SALT_LEN];

pub fn generate_repo_salt() -> RepoSalt {
    let mut rng = ChaCha20Rng::seed_from_u64(rng().next_u64());
    let mut salt = [0u8; REPO_SALT_LEN];
    rng.fill_bytes(&mut salt);

    salt
}
