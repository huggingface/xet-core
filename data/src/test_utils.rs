use std::fs::{File, create_dir_all, read_dir};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cas_client::{Client, LocalClient, LocalTestServer};
use itertools::multizip;
use progress_tracking::TrackingProgressUpdater;
use rand::prelude::*;
use tempfile::TempDir;
use ulid::Ulid;

use crate::configurations::TranslatorConfig;
use crate::data_client::clean_file;
use crate::{FileDownloadSession, FileUploadSession, XetFileInfo};

/// Creates or overwrites a single file in `dir` with `size` bytes of random data.
/// Panics on any I/O error. Returns the total number of bytes written (=`size`).
pub fn create_random_file(path: impl AsRef<Path>, size: usize, seed: u64) -> usize {
    let path = path.as_ref();

    let dir = path.parent().unwrap();

    // Make sure the directory exists, or create it.
    create_dir_all(dir).unwrap();

    let mut rng = StdRng::seed_from_u64(seed);

    // Build the path to the file, create the file, and write random data.
    let mut file = File::create(path).unwrap();

    let mut buffer = vec![0_u8; size];
    rng.fill_bytes(&mut buffer);

    file.write_all(&buffer).unwrap();

    size
}

/// Creates a collection of random files, each with a deterministic seed.  
/// the total number of bytes written for all files combined.
pub fn create_random_files(dir: impl AsRef<Path>, files: &[(impl AsRef<str>, usize)], seed: u64) -> usize {
    let dir = dir.as_ref();

    let mut total_bytes = 0;
    let mut rng = SmallRng::seed_from_u64(seed);

    for (file_name, size) in files {
        total_bytes += create_random_file(dir.join(file_name.as_ref()), *size, rng.random());
    }
    total_bytes
}

/// Creates or overwrites a single file in `dir` with consecutive segments determined by the list of [(size, seed)].
/// Panics on any I/O error. Returns the total number of bytes written (=`size`).
pub fn create_random_multipart_file(path: impl AsRef<Path>, segments: &[(usize, u64)]) -> usize {
    let path = path.as_ref();
    let dir = path.parent().unwrap();

    // Make sure the directory exists, or create it.
    create_dir_all(dir).unwrap();

    // Build the path to the file, create the file, and write random data.
    let mut file = File::create(path).unwrap();

    let mut total_size = 0;
    for &(size, seed) in segments {
        let mut rng = StdRng::seed_from_u64(seed);

        let mut buffer = vec![0_u8; size];
        rng.fill_bytes(&mut buffer);
        file.write_all(&buffer).unwrap();
        total_size += size;
    }
    total_size
}

/// Panics if `dir1` and `dir2` differ in terms of files or file contents.
/// Uses `unwrap()` everywhere; intended for test-only use.
pub fn verify_directories_match(dir1: impl AsRef<Path>, dir2: impl AsRef<Path>) {
    let dir1 = dir1.as_ref();
    let dir2 = dir2.as_ref();

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
             dir1: {files_in_dir1:?}\n dir2: {files_in_dir2:?}"
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
                "File contents differ for {file_name:?}\n \
                 dir1 path: {path1:?}\n dir2 path: {path2:?}"
            );
        }
    }
}

pub struct HydrateDehydrateTest {
    _temp_dir: TempDir,
    pub cas_dir: PathBuf,
    pub src_dir: PathBuf,
    pub ptr_dir: PathBuf,
    pub dest_dir: PathBuf,
    use_test_server: bool,
    /// Kept alive so the test server stays running for the duration of the test.
    test_server: Option<LocalTestServer>,
}

impl Default for HydrateDehydrateTest {
    fn default() -> Self {
        Self::new(false)
    }
}

impl HydrateDehydrateTest {
    /// Creates a new test harness with the specified options.
    ///
    /// # Arguments
    /// * `use_test_server` - If true, uses a LocalTestServer (RemoteClient over HTTP); otherwise uses LocalClient
    ///   directly.
    pub fn new(use_test_server: bool) -> Self {
        let _temp_dir = TempDir::new().unwrap();
        let temp_path = _temp_dir.path();

        let cas_dir = temp_path.join("cas");
        let src_dir = temp_path.join("src");
        let ptr_dir = temp_path.join("pointers");
        let dest_dir = temp_path.join("dest");

        std::fs::create_dir_all(&cas_dir).unwrap();
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::create_dir_all(&ptr_dir).unwrap();
        std::fs::create_dir_all(&dest_dir).unwrap();

        Self {
            cas_dir,
            src_dir,
            ptr_dir,
            dest_dir,
            _temp_dir,
            use_test_server,
            test_server: None,
        }
    }

    /// Lazily initializes the test server (if needed) and returns a CAS client.
    async fn get_or_create_client(&mut self) -> Arc<dyn Client> {
        if self.use_test_server {
            if self.test_server.is_none() {
                let local_client = LocalClient::new(self.cas_dir.join("xet/xorbs")).await.unwrap();
                self.test_server = Some(LocalTestServer::start_with_client(local_client).await);
            }
            self.test_server.as_ref().unwrap().remote_client().clone() as Arc<dyn Client>
        } else {
            LocalClient::new(self.cas_dir.join("xet/xorbs")).await.unwrap() as Arc<dyn Client>
        }
    }

    pub async fn new_upload_session(
        &self,
        progress_tracker: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Arc<FileUploadSession> {
        let config = Arc::new(TranslatorConfig::local_config(&self.cas_dir).unwrap());
        FileUploadSession::new(config.clone(), progress_tracker).await.unwrap()
    }

    pub async fn clean_all_files(&self, upload_session: &Arc<FileUploadSession>, sequential: bool) {
        create_dir_all(&self.ptr_dir).unwrap();

        if sequential {
            for entry in read_dir(&self.src_dir).unwrap() {
                let entry = entry.unwrap();
                let out_file = self.ptr_dir.join(entry.file_name());
                let upload_session = upload_session.clone();

                if sequential {
                    let (pf, metrics) = clean_file(upload_session.clone(), entry.path(), "").await.unwrap();
                    assert_eq!({ metrics.total_bytes }, entry.metadata().unwrap().len());
                    std::fs::write(out_file, pf.as_pointer_file().unwrap().as_bytes()).unwrap();

                    // Force a checkpoint after every file.
                    upload_session.checkpoint().await.unwrap();
                }
            }
        } else {
            let files: Vec<PathBuf> = read_dir(&self.src_dir)
                .unwrap()
                .map(|entry| self.src_dir.join(entry.unwrap().file_name()))
                .collect();

            let files_sha256_and_tracking_ids =
                multizip((files.iter(), std::iter::repeat(None), std::iter::repeat_with(Ulid::new)));

            let clean_results = upload_session.upload_files(files_sha256_and_tracking_ids).await.unwrap();

            for (i, xf) in clean_results.into_iter().enumerate() {
                std::fs::write(self.ptr_dir.join(files[i].file_name().unwrap()), serde_json::to_string(&xf).unwrap())
                    .unwrap();
            }
        }
    }

    pub async fn dehydrate(&mut self, sequential: bool) {
        let upload_session = self.new_upload_session(None).await;
        self.clean_all_files(&upload_session, sequential).await;

        upload_session.finalize().await.unwrap();
    }

    pub async fn hydrate(&mut self) {
        let client = self.get_or_create_client().await;
        let session = FileDownloadSession::from_client(client, None);

        for entry in read_dir(&self.ptr_dir).unwrap() {
            let entry = entry.unwrap();
            let out_filename = self.dest_dir.join(entry.file_name());

            let xf: XetFileInfo = serde_json::from_reader(File::open(entry.path()).unwrap()).unwrap();
            session.download_file(&xf, &out_filename, Ulid::new()).await.unwrap();
        }
    }

    pub async fn hydrate_partitioned_writers(&mut self, partitions: usize) {
        let client = self.get_or_create_client().await;
        let session = FileDownloadSession::from_client(client, None);

        for entry in read_dir(&self.ptr_dir).unwrap() {
            let entry = entry.unwrap();
            let out_filename = self.dest_dir.join(entry.file_name());
            let xf: XetFileInfo = serde_json::from_reader(File::open(entry.path()).unwrap()).unwrap();
            let file_size = xf.file_size();

            let out_file = File::create(&out_filename).unwrap();
            out_file.set_len(file_size).unwrap();

            if file_size == 0 {
                continue;
            }

            let partition_count = partitions.max(1) as u64;
            let mut tasks = Vec::new();

            for idx in 0..partition_count {
                let start = (idx * file_size) / partition_count;
                let end = ((idx + 1) * file_size) / partition_count;

                if start == end {
                    continue;
                }

                let session = session.clone();
                let xf = xf.clone();
                let out_filename = out_filename.clone();
                tasks.push(tokio::spawn(async move {
                    let mut writer = std::fs::OpenOptions::new().write(true).open(out_filename).unwrap();
                    writer.seek(SeekFrom::Start(start)).unwrap();
                    session.download_to_writer(&xf, start..end, writer, Ulid::new()).await
                }));
            }

            for task in tasks {
                task.await.unwrap().unwrap();
            }
        }
    }

    pub async fn hydrate_stream(&mut self) {
        let client = self.get_or_create_client().await;
        let session = FileDownloadSession::from_client(client, None);

        for entry in read_dir(&self.ptr_dir).unwrap() {
            let entry = entry.unwrap();
            let out_filename = self.dest_dir.join(entry.file_name());

            let xf: XetFileInfo = serde_json::from_reader(File::open(entry.path()).unwrap()).unwrap();
            let mut stream = session.download_stream(&xf, Ulid::new()).unwrap();

            let mut file = File::create(&out_filename).unwrap();
            while let Some(chunk) = stream.next().await.unwrap() {
                file.write_all(&chunk).unwrap();
            }
        }
    }

    pub fn verify_src_dest_match(&self) {
        verify_directories_match(&self.src_dir, &self.dest_dir);
    }
}
