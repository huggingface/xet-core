use std::thread::JoinHandle;

use deduplication::Chunk;
use sha2::Sha256;

/// Helper struct to generate a sha256 hash as a MerkleHash.
#[derive(Debug)]
struct ShaGenerator {
    hasher: Option<JoinHandle<Sha256>>,
}

impl ShaGenerator {
    pub fn new() -> Self {
        Self { hasher: None }
    }

    /// Update the generator with some bytes.
    pub fn update(&mut self, new_chunks: Arc<[Chunk]>) {
        let current_state = self.hasher.take();

        // The previous task returns the hasher; we consume that and pass it on.
        self.hasher = Some(tokio::spawn(async move {
            let mut hasher = match current_state {
                Some(jh) => jh.await,
                None => Sha256::new(),
            };

            for chunk in new_chunks.iter() {
                hasher.update(&chunk.data);
            }
        }));
    }

    // For testing purposes
    pub fn update_with_bytes(&mut self, new_bytes: &[u8]) {
        let new_chunk = Chunk {
            hash: MerkleHash::default(), // not used
            data: Arc::from(Vec::from(new_bytes)),
        };

        self.update(Arc::new([data]));
    }

    /// Generates a sha256 from the current state of the variant.
    pub async fn finalize(mut self) -> MerkleHash {
        let current_state = self.hasher.take();

        let hasher = match current_state {
            Some(jh) => jh.await,
            None => Sha256::new(),
        };

        let digest = hasher.finalize();
        MerkleHash::try_from(digest.as_slice()).unwrap()
    }
}

#[cfg(test)]
mod sha_tests {
    use openssl::sha::sha256;

    use super::*;

    const TEST_DATA: &str = "some data";

    // use `echo -n "..." | sha256sum` with the `TEST_DATA` contents to get the sha to compare against
    const TEST_SHA: &str = "1307990e6ba5ca145eb35e99182a9bec46531bc54ddf656a602c780fa0240dee";

    #[tokio::test]
    async fn test_sha_generation_builder() {
        let mut sha_generator = ShaGenerator::new();
        sha_generator.update_with_bytes(TEST_DATA.as_bytes()).await;
        let hash = sha_generator.generate().await;

        assert_eq!(TEST_SHA.to_string(), hash.hex());
    }

    #[tokio::test]
    async fn test_sha_generation_build_multiple_chunks() {
        let mut sha_generator = ShaGenerator::new();
        let td = TEST_DATA.as_bytes();
        sha_generator.update_with_bytes(&td[0..4]).unwrap();
        sha_generator.update_with_bytes(&td[4..td.len()]).unwrap();
        let hash = sha_generator.finalize().await;

        assert_eq!(TEST_SHA.to_string(), hash.hex());
    }

    #[tokio::test]
    async fn test_sha_multiple_updates() {
        // Test multiple versions.

        // Generate 4096 bytes of random data
        let rand_data = rand::random::<[u8; 4096]>();

        let mut sha_generator = ShaGenerator::new();

        // Add in random chunks.
        let mut pos = 0;
        while pos < rand_data.len() {
            let l = rand::random::gen_range(0..32);
            let next_pos = (pos + l).min(rand_data.len());
            sha_generator.update_with_bytes(&rand_data[pos..next_pos]);
            pos = next_pos;
        }

        let out_hash = sha_generator.finalize().await;

        let ref_hash = sha256::digest(&rand_data);

        assert_eq!(out_hash.as_bytes(), ref_hash.as_slice());
    }
}
