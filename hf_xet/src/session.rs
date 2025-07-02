use std::fs::File;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::Arc;

use data::configurations::TranslatorConfig;
use data::constants::INGESTION_BLOCK_SIZE;
use data::data_client::default_config;
use data::errors::DataProcessingError;
use data::{FileDownloader, FileUploadSession, XetFileInfo};
use merklehash::MerkleHash;
use progress_tracking::item_tracking::ItemProgressUpdater;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult};
use utils::auth::TokenRefresher;

use crate::log_buffer::HF_DEFAULT_ENDPOINT;
use crate::token_refresh::WrappedTokenRefresher;
use crate::{
    convert_data_processing_error, try_parse_progress_updater, DestinationPath, PyXetDownloadInfo, PyXetUploadInfo,
};

/// A Python-accessible session for managing file uploads and downloads in the XET system.
///
/// `XetSession` provides a high-level interface for interacting with XET's content-addressable
/// storage system. It manages authentication, file upload sessions, and download operations
/// through a unified API that can be called from Python.
///
/// The session handles:
/// - Authentication via tokens and optional token refresh mechanisms
/// - File uploads from both file paths and raw byte data
/// - File downloads with optional progress tracking
/// - Connection management to XET endpoints
///
/// # Example Usage (from Python)
/// ```python
/// session = XetSession(endpoint="https://example.com", token_info=("token", expiry))
/// upload_info = await session.upload_file("/path/to/file.txt")
/// download_path = await session.download_file_to_path(download_info)
/// ```
#[pyclass]
pub struct XetSession {
    config: Arc<TranslatorConfig>,
    file_upload_session: async_once_cell::OnceCell<Result<Arc<FileUploadSession>, Arc<DataProcessingError>>>,
    file_downloader: async_once_cell::OnceCell<Result<Arc<FileDownloader>, Arc<DataProcessingError>>>,
}

#[pymethods]
impl XetSession {
    /// Creates a new XET session with optional configuration parameters.
    ///
    /// # Arguments
    /// * `endpoint` - Optional custom endpoint URL. If not provided, uses the default HF endpoint.
    /// * `token_info` - Optional tuple containing (token_string, expiry_timestamp) for authentication. (token_string is
    ///   a string of the token, expiry_timestamp is a unix timestamp of when the token expires)
    /// * `token_refresher` - Optional Python callable that can refresh tokens when they expire. (function that returns
    ///   a tuple of (token_string, expiry_timestamp))
    ///
    /// The endpoint and token information should be provided by a xet-token api from the huggingface hub.
    ///
    /// # Returns
    /// A new `XetSession` instance configured with the provided parameters.
    ///
    /// # Errors
    /// Returns a `PyResult` error if:
    /// - The token refresher function is invalid or cannot be wrapped
    /// - The configuration cannot be created with the provided parameters
    ///
    /// # Example
    /// ```python
    /// # Basic session with default endpoint
    /// session = XetSession()
    ///
    /// # Session with custom endpoint and token
    /// session = XetSession(
    ///     endpoint="https://custom.endpoint.com",
    ///     token_info=("my_token", 1234567890)
    /// )
    ///
    /// # Session with token refresher
    /// def refresh_token():
    ///     token_info = requests.get("https://huggingface.co/some-api-for-xet-token")
    ///     return (token_info["token"], token_info["new_expiry"])
    ///
    /// session = XetSession(endpoint="https://custom.endpoint.com", token_info=refresh_token(), token_refresher=refresh_token)
    /// ```
    #[new]
    #[pyo3(signature = (endpoint=None, token_info=None, token_refresher=None))]
    pub fn new(
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let token_refresher: Option<Arc<dyn TokenRefresher>> = token_refresher
            .map(WrappedTokenRefresher::from_func)
            .transpose()?
            .map(|tr| Arc::new(tr) as Arc<dyn TokenRefresher>);
        let config =
            default_config(endpoint.unwrap_or(HF_DEFAULT_ENDPOINT.to_string()), None, token_info, token_refresher)
                .map_err(convert_data_processing_error)?;
        Ok(Self {
            config,
            file_upload_session: async_once_cell::OnceCell::new(),
            file_downloader: async_once_cell::OnceCell::new(),
        })
    }

    /// Initiates a file upload from the local filesystem to XET storage.
    ///
    /// This method reads a file from the specified path, processes it through XET's
    /// deduplication and chunking system, and begins to upload it to the configured endpoint.
    ///
    /// # Arguments
    /// * `file_path` - Path to the local file to upload. Must be a valid, readable file.
    ///
    /// # Returns
    /// `PyXetUploadInfo` containing details about the uploaded file, including:
    /// - File hash for content addressing
    /// - Size of the file in bytes
    ///
    /// # Errors
    /// Returns a `PyResult` error if:
    /// - The file cannot be opened or read
    /// - File metadata cannot be accessed
    /// - The upload session cannot be initialized
    /// - Network or server errors occur during upload
    /// - Data processing errors occur during chunking/deduplication
    ///
    /// # Example
    /// ```python
    /// upload_info = await session.upload_file("/path/to/document.pdf")
    /// print(f"File uploading with hash: {upload_info.hash}")
    ///
    /// # to ensure the upload is complete, call `complete_upload_files`
    /// ```
    #[pyo3(signature = (file_path))]
    pub async fn upload_file(&self, file_path: String) -> PyResult<PyXetUploadInfo> {
        let mut reader = File::open(&file_path)?;
        let file_size = reader.metadata()?.len();

        self.clean_reader(&mut reader, Some(file_path.into()), file_size as usize).await
    }

    /// Initiates a file upload from raw byte data (e.g. `bytes` or `bytearray`) to XET storage.
    ///
    /// This method takes a byte vector and uploads it directly to XET storage without
    /// requiring a temporary file. The data is processed through the same deduplication
    /// and chunking system as file uploads.
    ///
    /// # Arguments
    /// * `file_contents` - Vector of bytes representing the data to upload.
    /// * `file_path` - Optional string argument unique to this upload operation. Used for progress tracking.
    ///
    /// # Returns
    /// `PyXetUploadInfo` containing details about the uploaded data, including:
    /// - Content hash for addressing the data
    /// - Upload metadata and statistics
    /// - Size and chunk information
    ///
    /// # Errors
    /// Returns a `PyResult` error if:
    /// - The upload session cannot be initialized
    /// - Network or server errors occur during upload
    /// - Data processing errors occur during chunking/deduplication
    ///
    /// # Example
    /// ```python
    /// data = b"Hello, XET storage!"
    /// upload_info = await session.upload_bytes(data)
    /// print(f"Data uploading with hash: {upload_info.hash}")
    ///
    /// # to ensure the upload is complete, call `complete_upload_files`
    /// ```
    #[pyo3(signature = (file_contents, file_path = None))]
    pub async fn upload_bytes(&self, file_contents: Vec<u8>, file_path: Option<String>) -> PyResult<PyXetUploadInfo> {
        let contents_size = file_contents.len();
        let mut reader = Cursor::new(file_contents);

        self.clean_reader(&mut reader, file_path.map(|f| f.into()), contents_size).await
    }

    /// Completes all pending file uploads and ensures they are finalized.
    ///
    /// This method waits for all files that have been submitted for upload through
    /// `upload_file()` or `upload_bytes()` to finish uploading to XET storage.
    /// It finalizes the upload of all files submitted before being called,
    /// ensuring that all data has been properly transmitted and committed to the storage system.
    ///
    /// This is typically called after a whole batch of files have been queued for upload to
    /// ensure that the upload process is completely finished before the session
    /// is closed or before proceeding with other operations.
    ///
    /// # Returns
    /// Returns `Ok(())` when all uploads have been successfully completed and finalized.
    /// After this function returns all files that have been queued for upload will have been
    /// completely uploaded to XET storage.
    ///
    /// # Errors
    /// Returns a `PyResult` error if:
    /// - The upload session cannot be accessed or initialized
    /// - Network errors occur during the upload or finalization processes
    /// - Server errors prevent successful completion of uploads
    /// - Data processing errors occur during upload finalization
    ///
    /// # Example
    /// ```python
    /// # Upload multiple files
    /// upload_info1 = await session.upload_file("/path/to/file1.txt")
    /// upload_info2 = await session.upload_file("/path/to/file2.txt")
    ///
    /// # Wait for all uploads to complete
    /// await session.complete_upload_files()
    /// print("All uploads completed successfully")
    /// ```
    #[pyo3(signature = ())]
    pub async fn complete_upload_files(&self) -> PyResult<()> {
        let file_upload_session = self.file_upload_session().await.map_err(convert_data_processing_error)?;
        file_upload_session.finalize().await.map_err(convert_data_processing_error)?;
        Ok(())
    }

    /// Downloads a file from XET storage to a local path.
    ///
    /// This method retrieves a file from XET storage using its content hash and
    /// reconstructs it at the specified destination path. The download process
    /// handles chunk retrieval and reassembly automatically.
    ///
    /// # Arguments
    /// * `file` - `PyXetDownloadInfo` containing the file hash and destination path information.
    /// * `progress_updater` - Optional Python callable for receiving download progress updates. The callback will be
    ///   called with progress information during the download.
    ///
    /// # Returns
    /// A string containing the path where the file was downloaded.
    ///
    /// # Errors
    /// Returns a `PyResult` error if:
    /// - The file downloader cannot be initialized
    /// - The progress updater function is invalid
    /// - The file hash is malformed or invalid
    /// - Network errors occur during chunk retrieval
    /// - File system errors occur during file creation
    /// - The destination path is invalid or inaccessible
    ///
    /// # Example
    /// ```python
    /// def progress_callback(bytes_downloaded):
    ///     print(f"Downloaded {bytes_downloaded} bytes")
    ///
    /// download_info = PyXetDownloadInfo(hash="abc123...", path="/output/file.txt")
    /// result_path = await session.download_file_to_path(download_info, progress_callback)
    /// print(f"File downloaded to: {result_path}") # will match "/output/file.txt"
    /// ```
    #[pyo3(signature = (file, progress_updater = None))]
    pub async fn download_file_to_path(
        &self,
        file: PyXetDownloadInfo,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<String> {
        let downloader = self.file_downloader().await.map_err(convert_data_processing_error)?;

        let progress_updater = progress_updater
            .map(try_parse_progress_updater)
            .transpose()?
            .map(ItemProgressUpdater::new);

        let (file_info, dest_path): (XetFileInfo, DestinationPath) = file.into();
        let hash = MerkleHash::from_hex(file_info.hash())
            .map_err(data::errors::DataProcessingError::from)
            .map_err(convert_data_processing_error)?;
        let dest_path_buf: PathBuf = dest_path.as_str().into();
        let output_provider = cas_client::OutputProvider::new_file_provider(dest_path_buf);
        downloader
            .smudge_file_from_hash(&hash, dest_path.as_str().into(), &output_provider, None, progress_updater)
            .await
            .map_err(convert_data_processing_error)?;
        Ok(dest_path)
    }
}

impl XetSession {
    // utility function to remove duplicate code, pass data from a reader through a single file cleaner
    async fn clean_reader(
        &self,
        reader: &mut impl Read,
        file_path: Option<Arc<str>>,
        file_size: usize,
    ) -> PyResult<PyXetUploadInfo> {
        let file_upload_session = self.file_upload_session().await.map_err(convert_data_processing_error)?;
        let mut handle = file_upload_session.start_clean(file_path, file_size as u64).await;

        let ingestion_block_size = file_size.min(*INGESTION_BLOCK_SIZE);
        let mut buffer = vec![0u8; ingestion_block_size];

        loop {
            let bytes = reader.read(&mut buffer)?;
            if bytes == 0 {
                break;
            }

            handle
                .add_data(&buffer[0..bytes])
                .await
                .map_err(convert_data_processing_error)?;
        }

        let (info, _) = handle.finish().await.map_err(convert_data_processing_error)?;

        Ok(info.into())
    }

    async fn file_downloader(&self) -> Result<Arc<FileDownloader>, Arc<DataProcessingError>> {
        self.file_downloader
            .get_or_init(async { FileDownloader::new(self.config.clone()).await.map(Arc::new).map_err(Arc::new) })
            .await
            .clone()
    }

    async fn file_upload_session(&self) -> Result<Arc<FileUploadSession>, Arc<DataProcessingError>> {
        self.file_upload_session
            .get_or_init(async { FileUploadSession::new(self.config.clone(), None).await.map_err(Arc::new) })
            .await
            .clone()
    }
}
