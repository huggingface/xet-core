use std::fs::{create_dir_all, read_dir, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use cas_client::{FileProvider, OutputProvider};
use data::configurations::TranslatorConfig;
use data::data_client::clean_file;
use data::{FileDownloader, FileUploadSession, XetFileInfo};
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use rand::prelude::*;
use tempfile::TempDir;
use tokio::task::JoinSet;
use utils::test_set_globals;

// Runs this test suite with small chunks and xorbs so that we can make sure that all the different edge
// cases are hit.
test_set_globals! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 5 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 8;
}

/// Creates or overwrites a single file in `dir` with `size` bytes of random data.
/// Panics on any I/O error. Returns the total number of bytes written (=`size`).
pub fn create_random_file(dir: impl AsRef<Path>, file_name: &str, size: usize, seed: u64) -> usize {
    // Make sure the directory exists, or create it.
    create_dir_all(&dir).unwrap();

    let mut rng = StdRng::seed_from_u64(seed);

    // Build the path to the file, create the file, and write random data.
    let path = dir.as_ref().join(file_name);
    let mut file = File::create(&path).unwrap();

    let mut buffer = vec![0_u8; size];
    rng.fill_bytes(&mut buffer);
    file.write_all(&buffer).unwrap();

    size
}

/// Calls `clean_file` for each (filename, size) entry in `files`, returning
/// the total number of bytes written for all files combined.
pub fn create_random_files(dir: impl AsRef<Path>, files: &[(impl AsRef<str>, usize)], seed: u64) -> usize {
    let mut total_bytes = 0;
    let mut rng = SmallRng::seed_from_u64(seed);
    for (file_name, size) in files {
        total_bytes += create_random_file(&dir, file_name.as_ref(), *size, rng.gen());
    }
    total_bytes
}

/// Creates or overwrites a single file in `dir` with consecutive segments determined by the list of [(seed, size)].  
/// Panics on any I/O error. Returns the total number of bytes written (=`size`).
pub fn create_random_multipart_file(dir: impl AsRef<Path>, file_name: &str, segments: &[(u64, u64)]) -> usize {
    // Make sure the directory exists, or create it.
    create_dir_all(&dir).unwrap();

    // Build the path to the file, create the file, and write random data.
    let path = dir.as_ref().join(file_name);
    let mut file = File::create(&path).unwrap();

    let mut total_size = 0;
    for &(seed, size) in segments {
        let mut rng = StdRng::seed_from_u64(seed);

        let mut buffer = vec![0_u8; size as usize];
        rng.fill_bytes(&mut buffer);
        file.write_all(&buffer).unwrap();
        total_size += size;
    }
    total_size as usize
}

/// Panics if `dir1` and `dir2` differ in terms of files or file contents.
/// Uses `unwrap()` everywhere; intended for test-only use.
pub fn check_directories_match(dir1: &Path, dir2: &Path) {
    let mut files_in_dir1 = Vec::new();
    for entry in read_dir(dir1).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file());
        files_in_dir1.push(entry.file_name());
    }

    let mut files_in_dir2 = Vec::new();
    for entry in read_dir(dir2).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file());
        files_in_dir2.push(entry.file_name());
    }

    files_in_dir1.sort();
    files_in_dir2.sort();

    if files_in_dir1 != files_in_dir2 {
        panic!(
            "Directories differ: file sets are not the same.\n \
             dir1: {:?}\n dir2: {:?}",
            files_in_dir1, files_in_dir2
        );
    }

    // Compare file contents byte-for-byte
    for file_name in &files_in_dir1 {
        let path1 = dir1.join(file_name);
        let path2 = dir2.join(file_name);

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();

        File::open(&path1).unwrap().read_to_end(&mut buf1).unwrap();
        File::open(&path2).unwrap().read_to_end(&mut buf2).unwrap();

        if buf1 != buf2 {
            panic!(
                "File contents differ for {:?}\n \
                 dir1 path: {:?}\n dir2 path: {:?}",
                file_name, path1, path2
            );
        }
    }
}

async fn dehydrate_directory(cas_dir: &Path, src_dir: &Path, ptr_dir: &Path) {
    let config = TranslatorConfig::local_config(cas_dir).unwrap();

    create_dir_all(ptr_dir).unwrap();

    let upload_session = FileUploadSession::new(config.clone(), None).await.unwrap();

    let mut upload_tasks = JoinSet::new();

    for entry in read_dir(src_dir).unwrap() {
        let entry = entry.unwrap();
        let out_file = ptr_dir.join(entry.file_name());
        let upload_session = upload_session.clone();

        upload_tasks.spawn(async move {
            let (xf, metrics) = clean_file(upload_session.clone(), entry.path()).await.unwrap();
            assert_eq!(metrics.total_bytes as u64, entry.metadata().unwrap().len());
            std::fs::write(out_file, serde_json::to_string(&xf).unwrap()).unwrap();
        });
    }

    upload_tasks.join_all().await;

    upload_session.finalize().await.unwrap();
}

async fn hydrate_directory(cas_dir: &Path, ptr_dir: &Path, dest_dir: &Path) {
    let config = TranslatorConfig::local_config(cas_dir).unwrap();

    create_dir_all(dest_dir).unwrap();

    let downloader = FileDownloader::new(config).await.unwrap();

    for entry in read_dir(ptr_dir).unwrap() {
        let entry = entry.unwrap();

        let out_filename = dest_dir.join(entry.file_name());

        // Create an output file for writing
        let file_out = OutputProvider::File(FileProvider::new(out_filename.clone()));

        // Pointer file.
        let xf: XetFileInfo = serde_json::from_reader(File::open(entry.path()).unwrap()).unwrap();

        downloader
            .smudge_file_from_hash(
                &xf.merkle_hash().unwrap(),
                out_filename.to_string_lossy().into(),
                &file_out,
                None,
                None,
            )
            .await
            .unwrap();
    }
}

struct TestSetup {
    _temp_dir: TempDir,
    cas_dir: PathBuf,
    src_dir: PathBuf,
    ptr_dir: PathBuf,
    dest_dir: PathBuf,
}

impl TestSetup {
    fn new() -> Self {
        let _temp_dir = TempDir::new().unwrap();
        let temp_path = _temp_dir.path();

        Self {
            cas_dir: temp_path.join("cas"),
            src_dir: temp_path.join("src"),
            ptr_dir: temp_path.join("pointers"),
            dest_dir: temp_path.join("dest"),
            _temp_dir,
        }
    }
}
/// Variant of `dehydrate_directory` that calls `upload_session.checkpoint()`
/// after every few file uploads.  This ensures that cross-file deduplication happens and
/// gets test coverage.
async fn dehydrate_directory_sequential(cas_dir: &Path, src_dir: &Path, ptr_dir: &Path) {
    let config = TranslatorConfig::local_config(cas_dir).unwrap();
    std::fs::create_dir_all(ptr_dir).unwrap();

    let upload_session = FileUploadSession::new(config.clone(), None).await.unwrap();

    // Process files in a simple for-loop (no concurrency)
    for entry in read_dir(src_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let out_file = ptr_dir.join(entry.file_name());

        let (pf, _metrics) = clean_file(upload_session.clone(), path).await.unwrap();
        std::fs::write(out_file, pf.as_pointer_file().unwrap().as_bytes()).unwrap();

        // Force a checkpoint after every file.
        upload_session.checkpoint().await.unwrap();
    }

    upload_session.finalize().await.unwrap();
}

async fn check_clean_smudge_files_impl(file_list: &[(impl AsRef<str>, usize)], sequential: bool) {
    let ts = TestSetup::new();

    create_random_files(&ts.src_dir, file_list, 0);
    if sequential {
        dehydrate_directory_sequential(&ts.cas_dir, &ts.src_dir, &ts.ptr_dir).await;
    } else {
        dehydrate_directory(&ts.cas_dir, &ts.src_dir, &ts.ptr_dir).await;
    }
    hydrate_directory(&ts.cas_dir, &ts.ptr_dir, &ts.dest_dir).await;

    check_directories_match(&ts.src_dir, &ts.dest_dir);
}

// Check both sequential and all together
async fn check_clean_smudge_files(file_list: &[(impl AsRef<str>, usize)]) {
    check_clean_smudge_files_impl(file_list, true).await;
    check_clean_smudge_files_impl(file_list, false).await;
}

/// Helper for multipart tests:
///  - takes a slice of `(String, Vec<(u64, u64)>)` which fully specifies each file.
///  - for each file, calls `create_random_multipart_file` with the given segments.
async fn check_clean_smudge_files_multipart_impl(file_specs: &[(String, Vec<(u64, u64)>)], sequential: bool) {
    let ts = TestSetup::new();

    // Create each file from the given vector of segments
    for (file_name, segments) in file_specs {
        // We call `segments.clone()` because `create_random_multipart_file`
        // takes ownership of the Vec<(u64,u64)>.
        create_random_multipart_file(&ts.src_dir, file_name, &segments);
    }
    if sequential {
        // Dehydrate (upload) files, but checkpoint after each upload
        dehydrate_directory_sequential(&ts.cas_dir, &ts.src_dir, &ts.ptr_dir).await;
    } else {
        // Dehydrate
        dehydrate_directory(&ts.cas_dir, &ts.src_dir, &ts.ptr_dir).await;
    }

    // Hydrate
    hydrate_directory(&ts.cas_dir, &ts.ptr_dir, &ts.dest_dir).await;
    // Check
    check_directories_match(&ts.src_dir, &ts.dest_dir);
}

async fn check_clean_smudge_files_multipart(file_specs: &[(String, Vec<(u64, u64)>)]) {
    check_clean_smudge_files_multipart_impl(file_specs, true).await;
    eprintln!("Successfully completed sequential upload; trying in parallel.");
    check_clean_smudge_files_multipart_impl(file_specs, false).await;
}

#[cfg(test)]
mod tests {
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
        let file_specs: Vec<(String, Vec<(u64, u64)>)> = (0..16)
            .map(|i| (format!("identical_{i}"), vec![(123, *MAX_XORB_BYTES as u64 / 2)]))
            .collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }

    /// 2) many identical files, each larger than MAX_XORB_BYTES.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_identical_files_slightly_larger_than_max_xorb() {
        // single segment that exceeds MAX_XORB_BYTES
        let big_size = (*MAX_XORB_BYTES as u64) + 1;
        let segments = vec![(9999, big_size)];

        let file_specs: Vec<(String, Vec<(u64, u64)>)> =
            (0..2).map(|i| (format!("big_identical_{i}"), segments.clone())).collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }

    /// 3) many files, each with a unique portion plus a large common portion bigger than MAX_XORB_BYTES/2.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_files_unique_plus_small_common() {
        let block_size = (*MAX_XORB_BYTES as u64) / 2;
        // Each file has two segments: (i, 2048) -> unique seed, (999, half) -> common chunk
        let file_specs: Vec<(String, Vec<(u64, u64)>)> = (0..32)
            .map(|i| (format!("file_{i}"), vec![(i, block_size), (999, block_size)]))
            .collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }

    /// 3) many files, each with a unique portion plus a large common portion bigger than MAX_XORB_BYTES/2.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_files_unique_plus_large_common() {
        let block_size = (*MAX_XORB_BYTES as u64) + 10;
        // Each file has two segments: (i, 2048) -> unique seed, (999, half) -> common chunk
        let file_specs: Vec<(String, Vec<(u64, u64)>)> = (0..32)
            .map(|i| (format!("file_{i}"), vec![(i, block_size), (999, block_size)]))
            .collect();

        check_clean_smudge_files_multipart(&file_specs).await;
    }
}
