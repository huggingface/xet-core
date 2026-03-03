use mdb_shard::Sha256;
use sha2::{Digest, Sha256 as sha2Sha256};
use tokio::task::{JoinError, JoinHandle};
use xet_runtime::XetRuntime;

/// Controls how SHA-256 is handled during file cleaning.
pub enum Sha256Policy {
    /// Compute SHA-256 from the file data.
    Compute,
    /// Use a pre-computed SHA-256 value.
    Provided(Sha256),
    /// Skip SHA-256 entirely; no metadata_ext is written to the shard.
    Skip,
}

impl From<Option<Sha256>> for Sha256Policy {
    fn from(sha256: Option<Sha256>) -> Self {
        match sha256 {
            Some(hash) => Self::Provided(hash),
            None => Self::Compute,
        }
    }
}

pub(crate) enum ShaGenerator {
    Generate(Sha256Generator),
    ProvidedValue(Sha256),
    /// Skip SHA-256 computation entirely. `finalize()` returns `None`.
    Skip,
}

impl From<Sha256Policy> for ShaGenerator {
    fn from(policy: Sha256Policy) -> Self {
        match policy {
            Sha256Policy::Compute => Self::generate(),
            Sha256Policy::Provided(hash) => Self::ProvidedValue(hash),
            Sha256Policy::Skip => Self::Skip,
        }
    }
}

impl ShaGenerator {
    pub async fn update(&mut self, new_data: impl AsRef<[u8]> + Send + Sync + 'static) -> Result<(), JoinError> {
        match self {
            Self::Generate(generator) => generator.update(new_data).await,
            Self::ProvidedValue(_) | Self::Skip => Ok(()),
        }
    }

    pub async fn finalize(self) -> Result<Option<Sha256>, JoinError> {
        match self {
            Self::Generate(generator) => generator.finalize().await.map(Some),
            Self::ProvidedValue(hash) => Ok(Some(hash)),
            Self::Skip => Ok(None),
        }
    }

    pub fn generate() -> Self {
        Self::Generate(Sha256Generator::default())
    }
}

/// Helper struct to generate a sha256 hash.
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
        let rt = XetRuntime::current();
        self.hasher = Some(rt.spawn_blocking(move || {
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
    async fn test_sha_skip() {
        let mut sha_gen = ShaGenerator::Skip;
        sha_gen.update(b"some data").await.unwrap();
        assert!(sha_gen.finalize().await.unwrap().is_none());
    }

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
