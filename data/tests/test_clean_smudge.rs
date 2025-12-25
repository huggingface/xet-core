use data::test_utils::*;
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use utils::test_set_constants;

// Runs this test suite with small chunks and xorbs so that we can make sure that all the different edge
// cases are hit.
test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 5 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 8;
}

/// Runs clean/smudge test with all combinations of (use_v1_reconstructor, use_test_server, sequential).
/// Each combination runs sequentially with its own HydrateDehydrateTest instance to avoid
/// too many open files.
pub async fn check_clean_smudge_files(file_list: &[(impl AsRef<str> + Clone, usize)]) {
    for use_v1 in [false, true] {
        for use_server in [false, true] {
            for sequential in [true, false] {
                eprintln!(
                    "Testing use_v1_reconstructor={use_v1}, use_test_server={use_server}, sequential={sequential}"
                );

                let mut ts = HydrateDehydrateTest::new(use_v1, use_server);
                create_random_files(&ts.src_dir, file_list, 0);

                ts.dehydrate(sequential).await;
                ts.hydrate().await;
                ts.verify_src_dest_match();
            }
        }
    }
}

/// Helper for multipart tests:
///  - takes a slice of `(String, Vec<(u64, u64)>)` which fully specifies each file.
///  - for each file, calls `create_random_multipart_file` with the given segments.
async fn check_clean_smudge_files_multipart(file_specs: &[(String, Vec<(usize, u64)>)]) {
    for use_v1 in [false, true] {
        for use_server in [false, true] {
            for sequential in [true, false] {
                eprintln!(
                    "Testing use_v1_reconstructor={use_v1}, use_test_server={use_server}, sequential={sequential}"
                );

                let mut ts = HydrateDehydrateTest::new(use_v1, use_server);

                for (file_name, segments) in file_specs {
                    create_random_multipart_file(ts.src_dir.join(file_name), segments);
                }

                ts.dehydrate(sequential).await;
                ts.hydrate().await;
                ts.verify_src_dest_match();
            }
        }
    }
}

#[cfg(test)]
mod testing_clean_smudge {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_simple_directory() {
        check_clean_smudge_files(&[("a", 16)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple() {
        check_clean_smudge_files(&[("a", 16), ("b", 8)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_with_empty_file() {
        check_clean_smudge_files(&[("a", 16), ("b", 8), ("c", 0)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_with_all_empty_files() {
        check_clean_smudge_files(&[("a", 0), ("b", 0), ("c", 0)]).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_many_small() {
        let files: Vec<_> = (0..3).map(|idx| (format!("f_{idx}"), idx % 2)).collect();
        check_clean_smudge_files(&files).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_large() {
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES + 1)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_two_small_multiple_xorbs() {
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES / 2 + 1), ("b", *MAX_XORB_BYTES / 2 + 1)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_large() {
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES + 1), ("b", *MAX_XORB_BYTES + 2)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_small_multiple_xorbs() {
        let n = 16;
        let size = *MAX_XORB_BYTES / 8 + 1; // Will need 3 xorbs.

        let files: Vec<_> = (0..n).map(|idx| (format!("f_{idx}"), size)).collect();
        check_clean_smudge_files(&files).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_file_with_common_xorbs() {
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES / 2 + 1), ("b", *MAX_XORB_BYTES / 2 + 1)]).await;
    }

    /// 1) Several identical files, each smaller than MAX_XORB_BYTES.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_several_identical_multipart() {
        // Let's make 16 files, each identical and smaller than a xorb
        let file_specs: Vec<(String, Vec<(usize, u64)>)> = (0..16)
            .map(|i| (format!("identical_{i}"), vec![(*MAX_XORB_BYTES / 2, 123)]))
            .collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }

    /// 2) many identical files, each larger than MAX_XORB_BYTES.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_identical_files_slightly_larger_than_max_xorb() {
        // single segment that exceeds MAX_XORB_BYTES
        let big_size = *MAX_XORB_BYTES + 1;
        let segments = vec![(big_size, 9999)];

        let file_specs: Vec<(String, Vec<(usize, u64)>)> =
            (0..2).map(|i| (format!("big_identical_{i}"), segments.clone())).collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }

    /// 3) many files, each with a unique portion plus a large common portion bigger than MAX_XORB_BYTES/2.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_files_unique_plus_small_common() {
        let block_size = *MAX_XORB_BYTES / 2;
        // Each file has two segments: (i, 2048) -> unique seed, (999, half) -> common chunk
        let file_specs: Vec<(String, Vec<(usize, u64)>)> = (0..32)
            .map(|i| (format!("file_{i}"), vec![(block_size, i), (block_size, 999)]))
            .collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }

    /// 4) many files, each with a unique portion plus a large common portion bigger than MAX_XORB_BYTES/2.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_files_unique_plus_large_common() {
        let block_size = *MAX_XORB_BYTES + 10;
        // Each file has two segments: (i, 2048) -> unique seed, (999, half) -> common chunk
        let file_specs: Vec<(String, Vec<(usize, u64)>)> = (0..32)
            .map(|i| (format!("file_{i}"), vec![(block_size, i), (block_size, 999)]))
            .collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }
}
