//! Clean/smudge integration tests with `enable_multirange_fetching = true`.
//!
//! This test binary is a separate copy of a subset of the clean/smudge tests
//! that runs with `enable_multirange_fetching` enabled, exercising the
//! multirange HTTP request path rather than the default single-range splitting.

use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_data::processing::test_utils::*;
use xet_runtime::{test_set_config, test_set_constants};

test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 5 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 8;
}

test_set_config! {
    client {
        enable_multirange_fetching = true;
    }
}

#[cfg(test)]
mod testing_clean_smudge_multirange {
    use super::*;

    pub async fn check_clean_smudge_files(file_list: &[(impl AsRef<str> + Clone, usize)]) {
        #[cfg(feature = "smoke-test")]
        let modes: &[HydrationMode] = &[HydrationMode::DirectClient, HydrationMode::ServerV2];
        #[cfg(not(feature = "smoke-test"))]
        let modes: &[HydrationMode] = HydrationMode::all();

        #[cfg(feature = "smoke-test")]
        let sequential_options: &[bool] = &[false];
        #[cfg(not(feature = "smoke-test"))]
        let sequential_options: &[bool] = &[true, false];

        for &mode in modes {
            for &sequential in sequential_options {
                eprintln!("Testing mode={mode}, sequential={sequential} (forced multirange)");

                let mut ts = HydrateDehydrateTest::for_mode(mode);
                create_random_files(&ts.src_dir, file_list, 0);

                ts.dehydrate(sequential).await;
                ts.apply_hydration_mode(mode).await;
                ts.hydrate().await;
                ts.verify_src_dest_match();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_simple_directory() {
        check_clean_smudge_files(&[("a", 16)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple() {
        check_clean_smudge_files(&[("a", 16), ("b", 8)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_large() {
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES + 1)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_large() {
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES + 1), ("b", *MAX_XORB_BYTES + 2)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_small_multiple_xorbs() {
        let n = 16;
        let size = *MAX_XORB_BYTES / 8 + 1;

        let files: Vec<_> = (0..n).map(|idx| (format!("f_{idx}"), size)).collect();
        check_clean_smudge_files(&files).await;
    }
}
