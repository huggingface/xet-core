use std::fs::{create_dir_all, read_dir, File};
use std::io::{Read, Write};
use std::path::Path;

use cas_client::{FileProvider, OutputProvider};
use data::configurations::TranslatorConfig;
use data::data_client::clean_file;
use data::{FileDownloader, FileUploadSession, PointerFile};
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use rand::rngs::StdRng;
// rand crates
use rand::RngCore;
use rand::SeedableRng;
use tempfile::TempDir;
use tokio::task::JoinSet;
use utils::test_set_globals;
use xet_threadpool::ThreadPool;

// Runt this test suite with small chunks and xorbs so that we can make sure that all the different edge
// cases are hit.
test_set_globals! {
    TARGET_CHUNK_SIZE = 8 * 1024;
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
    for (file_name, size) in files {
        total_bytes += create_random_file(&dir, file_name.as_ref(), *size, seed);
    }
    total_bytes
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

    let upload_session = FileUploadSession::new(config.clone(), ThreadPool::from_current_runtime(), None)
        .await
        .unwrap();

    let mut upload_tasks = JoinSet::new();

    for entry in read_dir(src_dir).unwrap() {
        let entry = entry.unwrap();
        let out_file = ptr_dir.join(entry.file_name());
        let upload_session = upload_session.clone();

        upload_tasks.spawn(async move {
            let (pf, _metrics) = clean_file(upload_session.clone(), entry.path()).await.unwrap();
            std::fs::write(out_file, pf.to_string()).unwrap();
        });
    }

    upload_tasks.join_all().await;

    upload_session.finalize().await.unwrap();
}

async fn hydrate_directory(cas_dir: &Path, ptr_dir: &Path, out_dir: &Path) {
    let config = TranslatorConfig::local_config(cas_dir).unwrap();

    create_dir_all(out_dir).unwrap();

    let downloader = FileDownloader::new(config, ThreadPool::from_current_runtime()).await.unwrap();

    for entry in read_dir(ptr_dir).unwrap() {
        let entry = entry.unwrap();

        let out_filename = out_dir.join(entry.file_name());

        // Create an output file for writing
        let file_out = OutputProvider::File(FileProvider::new(out_filename));

        // Pointer file.
        let pf = PointerFile::init_from_path(entry.path());
        assert!(pf.is_valid());

        downloader.smudge_file_from_pointer(&pf, &file_out, None, None).await.unwrap();
    }
}

async fn check_clean_smudge_files(file_list: &[(impl AsRef<str>, usize)]) {
    let _temp_dir = TempDir::new().unwrap();
    let temp_path = _temp_dir.path();

    let cas_dir = temp_path.join("cas");
    let src_dir = temp_path.join("src");
    let ptr_dir = temp_path.join("pointers");
    let dest_dir = temp_path.join("dest");

    create_random_files(&src_dir, file_list, 0);

    dehydrate_directory(&cas_dir, &src_dir, &ptr_dir).await;
    hydrate_directory(&cas_dir, &ptr_dir, &dest_dir).await;

    check_directories_match(&src_dir, &dest_dir);
}

fn setup_env() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_simple_directory() {
        setup_env();
        check_clean_smudge_files(&[("a", 16)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple() {
        setup_env();
        check_clean_smudge_files(&[("a", 16), ("b", 8)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_with_empty_file() {
        setup_env();
        check_clean_smudge_files(&[("a", 16), ("b", 8), ("c", 0)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_with_all_empty_files() {
        setup_env();
        check_clean_smudge_files(&[("a", 0), ("b", 0), ("c", 0)]).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_many_small() {
        setup_env();
        let files: Vec<_> = (0..32).map(|idx| (format!("f_{idx}"), idx % 8)).collect();
        check_clean_smudge_files(&files).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_large() {
        setup_env();
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES + 1)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_two_small_multiple_xorbs() {
        setup_env();
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES / 2 + 1), ("b", *MAX_XORB_BYTES / 2 + 1)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_large() {
        setup_env();
        check_clean_smudge_files(&[("a", *MAX_XORB_BYTES + 1), ("b", *MAX_XORB_BYTES + 2)]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_small_multiple_xorbs() {
        let n = 16;
        let size = *MAX_XORB_BYTES / 8 + 1; // Will need 3 xorbs.

        let files: Vec<_> = (0..n).map(|idx| (format!("f_{idx}"), size)).collect();
        setup_env();
        check_clean_smudge_files(&files).await;
    }
}
