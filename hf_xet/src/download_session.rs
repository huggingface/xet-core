use std::sync::Arc;

use data::constants::MAX_CONCURRENT_DOWNLOADS;
use data::data_client::default_config;
use data::{FileDownloader, XetFileInfo};
use merklehash::MerkleHash;
use progress_tracking::item_tracking::ItemProgressUpdater;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};
use utils::auth::TokenRefresher;
use xet_threadpool::exports::tokio::sync::Semaphore;

use crate::log_buffer::HF_DEFAULT_ENDPOINT;
use crate::runtime::async_run;
use crate::token_refresh::WrappedTokenRefresher;
use crate::{convert_data_processing_error, try_parse_progress_updater, DestinationPath, PyXetDownloadInfo};

/// A Python-accessible session for managing file downloads in the XET system.
///
/// `XetDownloadSession` provides a high-level interface for downloading files from XET's
/// content-addressable storage system. It manages authentication and file download
/// operations through a unified API that can be called from Python.
///
/// The session handles:
/// - Authentication via tokens and optional token refresh mechanisms
/// - File downloads with optional progress tracking
/// - Connection management to XET endpoints
/// - Content hash-based file retrieval
///
/// # Example Usage (from Python)
/// ```python
/// session = XetDownloadSession(endpoint="https://example.com", token_info=("token", expiry))
/// download_path = await session.download_file_to_path(download_info)
/// ```
#[pyclass]
pub struct XetDownloadSession {
    semaphore: Arc<Semaphore>,
    file_downloader: Arc<FileDownloader>,
}

#[pymethods]
impl XetDownloadSession {
    /// Creates a new XET download session with optional configuration parameters.
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
    /// A new `XetDownloadSession` instance configured with the provided parameters.
    ///
    /// # Errors
    /// Returns a `PyResult` error if:
    /// - The token refresher function is invalid or cannot be wrapped
    /// - The configuration cannot be created with the provided parameters
    ///
    /// # Example
    /// ```python
    /// # Basic session with default endpoint
    /// session = XetDownloadSession()
    ///
    /// # Session with custom endpoint and token
    /// session = XetDownloadSession(
    ///     endpoint="https://custom.endpoint.com",
    ///     token_info=("my_token", 1234567890)
    /// )
    ///
    /// # Session with token refresher
    /// def refresh_token():
    ///     token_info = requests.get("https://huggingface.co/some-api-for-xet-token")
    ///     return (token_info["token"], token_info["new_expiry"])
    ///
    /// session = XetDownloadSession(endpoint="https://custom.endpoint.com", token_info=refresh_token(), token_refresher=refresh_token)
    /// ```
    #[new]
    #[pyo3(signature = (endpoint=None, token_info=None, token_refresher=None))]
    pub fn new(
        py: Python,
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
        let file_downloader = async_run(py, async move {
            FileDownloader::new(config)
                .await
                .map(Arc::new)
                .map_err(convert_data_processing_error)
        })?;
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(*MAX_CONCURRENT_DOWNLOADS)),
            file_downloader,
        })
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
    /// * `progress_updater` - Optional Python callable to update progress of a download, takes 1 parameter, an
    ///   increment number of bytes downloaded
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
    pub fn download_file_to_path(
        &self,
        py: Python,
        file: PyXetDownloadInfo,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<String> {
        let progress_updater = progress_updater
            .map(try_parse_progress_updater)
            .transpose()?
            .map(ItemProgressUpdater::new);

        let (file_info, dest_path): (XetFileInfo, DestinationPath) = file.into();
        let hash = MerkleHash::from_hex(file_info.hash())
            .map_err(data::errors::DataProcessingError::from)
            .map_err(convert_data_processing_error)?;
        let output_provider = cas_client::OutputProvider::new_file_provider(dest_path.as_str());
        let file_name = dest_path.as_str().into();
        let file_downloader = self.file_downloader.clone();
        let semaphore = self.semaphore.clone();
        async_run(py, async move {
            let _permit = semaphore.acquire().await;
            file_downloader
                .smudge_file_from_hash(&hash, file_name, &output_provider, None, progress_updater)
                .await
                .map_err(convert_data_processing_error)
        })?;
        Ok(dest_path)
    }
}
