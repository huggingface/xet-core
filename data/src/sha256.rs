use mdb_shard::Sha256;
use sha2::{Digest, Sha256 as sha2Sha256};
use tokio::task::{JoinError, JoinHandle};

pub enum ShaGenerator {
    Generate(Sha256Generator),
    ProvidedValue(Sha256),
}

impl ShaGenerator {
    pub async fn update(&mut self, new_data: impl AsRef<[u8]> + Send + Sync + 'static) -> Result<(), JoinError> {
        match self {
            Self::Generate(generator) => generator.update(new_data).await,
            Self::ProvidedValue(_) => Ok(()),
        }
    }

    pub async fn finalize(self) -> Result<Sha256, JoinError> {
        match self {
            Self::Generate(generator) => generator.finalize().await,
            Self::ProvidedValue(hash) => Ok(hash),
        }
    }

    pub fn generate() -> Self {
        Self::Generate(Sha256Generator::default())
    }
}

/// Helper struct to generate a sha256 hash as a MerkleHash.
#[derive(Debug, Default)]
pub struct Sha256Generator {
    hasher: Option<JoinHandle<Result<sha2Sha256, JoinError>>>,
}

impl Sha256Generator {
    /// Complete the last block, then hand off the new chunks to the new hasher.
    pub async fn update(&mut self, new_data: impl AsRef<[u8]> + Send + Sync + 'static) -> Result<(), JoinError> {
        let mut hasher = match self.hasher.take() {
            Some(jh) => jh.await??,
            None => sha2Sha256::default(),
        };

        // The previous task returns the hasher; we consume that and pass it on.
        // Use the compute background thread for this process.
        self.hasher = Some(tokio::task::spawn_blocking(move || {
            hasher.update(&new_data);

            Ok(hasher)
        }));

        Ok(())
    }

    /// Generates a sha256 from the current state of the variant.
    pub async fn finalize(mut self) -> Result<Sha256, JoinError> {
        let current_state = self.hasher.take();

        let hasher = match current_state {
            Some(jh) => jh.await??,
            None => return Ok(Sha256::default()),
        };

        let sha256 = hasher.finalize();
        let hex_str = format!("{sha256:x}");
        Ok(Sha256::from_hex(&hex_str).expect("Converting sha256 to merklehash."))
    }
}

#[cfg(test)]
mod sha_tests {
    use rand::{Rng, rng};

    use super::*;

    const TEST_DATA: &str = "some data";

    // use `echo -n "..." | sha256sum` with the `TEST_DATA` contents to get the sha to compare against
    const TEST_SHA: &str = "1307990e6ba5ca145eb35e99182a9bec46531bc54ddf656a602c780fa0240dee";

    #[tokio::test]
    async fn test_sha_generation_builder() {
        let mut sha_generator = Sha256Generator::default();
        sha_generator.update(TEST_DATA.as_bytes()).await.unwrap();
        let hash = sha_generator.finalize().await.unwrap();

        assert_eq!(TEST_SHA.to_string(), hash.hex());
    }

    #[tokio::test]
    async fn test_sha_generation_build_multiple_chunks() {
        let mut sha_generator = Sha256Generator::default();
        let td = TEST_DATA.as_bytes();
        sha_generator.update(&td[0..4]).await.unwrap();
        sha_generator.update(&td[4..td.len()]).await.unwrap();
        let hash = sha_generator.finalize().await.unwrap();

        assert_eq!(TEST_SHA.to_string(), hash.hex());
    }

    #[tokio::test]
    async fn test_sha_multiple_updates() {
        // Test multiple versions.

        // Generate 4096 bytes of random data
        let mut rand_data = [0u8; 4096];
        rng().fill(&mut rand_data[..]);

        let mut sha_generator = Sha256Generator::default();

        // Add in random chunks.
        let mut pos = 0;
        while pos < rand_data.len() {
            let l = rng().random_range(0..32);
            let next_pos = (pos + l).min(rand_data.len());
            sha_generator.update(rand_data[pos..next_pos].to_vec()).await.unwrap();
            pos = next_pos;
        }

        let out_hash = sha_generator.finalize().await.unwrap();

        let ref_hash = format!("{:x}", sha2Sha256::digest(rand_data));

        assert_eq!(out_hash.hex(), ref_hash);
    }
}
