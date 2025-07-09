use std::fs::File;
use std::io::{Cursor, Read};
use std::sync::Arc;

use data::constants::{INGESTION_BLOCK_SIZE, MAX_CONCURRENT_FILE_INGESTION};
use data::data_client::default_config;
use data::errors::DataProcessingError;
use data::{FileUploadSession, XetFileInfo};
use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};
use utils::auth::TokenRefresher;
use xet_threadpool::exports::tokio::sync::{Mutex, Semaphore};
use xet_threadpool::exports::tokio::task::JoinSet;

use crate::convert_data_processing_error;
use crate::log_buffer::HF_DEFAULT_ENDPOINT;
use crate::progress_update::WrappedProgressUpdater;
use crate::runtime::async_run;
use crate::token_refresh::WrappedTokenRefresher;

/// A Python-accessible session for managing file uploads in the XET system.
///
/// `XetUploadSession` provides a high-level interface for uploading files to XET's
/// content-addressable storage system. It manages authentication and file upload
/// operations through a unified API that can be called from Python.
///
/// The session handles:
/// - Authentication via tokens and optional token refresh mechanisms
/// - File uploads from both file paths and raw byte data
/// - Connection management to XET endpoints
/// - Upload session management and finalization
///
/// # Example Usage (from Python)
/// ```python
/// session = XetUploadSession(endpoint="https://example.com", token_info=("token", expiry))
/// upload_info = await session.upload_file("/path/to/file.txt")
/// await session.complete_upload_files()
/// ```
#[pyclass]
pub struct XetUploadSession {
    semaphore: Arc<Semaphore>,
    file_upload_session: Arc<FileUploadSession>,
    cleaning_handles: Arc<Mutex<JoinSet<Result<XetFileInfo, DataProcessingError>>>>,
}

#[pymethods]
impl XetUploadSession {
    /// Creates a new XET upload session with optional configuration parameters.
    ///
    /// # Arguments
    /// * `endpoint` - Optional custom endpoint URL. If not provided, uses the default HF endpoint.
    /// * `token_info` - Optional tuple containing (token_string, expiry_timestamp) for authentication. (token_string is
    ///   a string of the token, expiry_timestamp is a unix timestamp of when the token expires)
    /// * `token_refresher` - Optional Python callable that can refresh tokens when they expire. (function that returns
    ///   a tuple of (token_string, expiry_timestamp))
    /// * `progress_updater` - Optional Python callable to update progress of uploads, see spec for this function in
    ///   hf_xet/src/progress_update.rs
    ///
    /// The endpoint and token information should be provided by a xet-token api from the huggingface hub.
    ///
    /// # Returns
    /// A new `XetUploadSession` instance configured with the provided parameters.
    ///
    /// # Errors
    /// Returns a `PyResult` error if:
    /// - The token refresher function is invalid or cannot be wrapped
    /// - The configuration cannot be created with the provided parameters
    ///
    /// # Example
    /// ```python
    /// # Basic session with default endpoint
    /// session = XetUploadSession()
    ///
    /// # Session with custom endpoint and token
    /// session = XetUploadSession(
    ///     endpoint="https://custom.endpoint.com",
    ///     token_info=("my_token", 1234567890)
    /// )
    ///
    /// # Session with token refresher
    /// def refresh_token():
    ///     token_info = requests.get("https://huggingface.co/some-api-for-xet-token")
    ///     return (token_info["token"], token_info["new_expiry"])
    ///
    /// session = XetUploadSession(endpoint="https://custom.endpoint.com", token_info=refresh_token(), token_refresher=refresh_token)
    /// ```
    #[new]
    #[pyo3(signature = (endpoint=None, token_info=None, token_refresher=None, progress_updater=None)
    )]
    pub fn new(
        py: Python,
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Py<PyAny>>,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let token_refresher: Option<Arc<dyn TokenRefresher>> = token_refresher
            .map(WrappedTokenRefresher::from_func)
            .transpose()?
            .map(|tr| Arc::new(tr) as Arc<dyn TokenRefresher>);
        let config =
            default_config(endpoint.unwrap_or(HF_DEFAULT_ENDPOINT.to_string()), None, token_info, token_refresher)
                .map_err(convert_data_processing_error)?;
        let updater = progress_updater
            .map(WrappedProgressUpdater::new)
            .transpose()?
            .map(|v| Arc::new(v) as Arc<_>);

        let file_upload_session = async_run(py, async move {
            FileUploadSession::new(config, updater)
                .await
                .map_err(convert_data_processing_error)
        })?;
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(*MAX_CONCURRENT_FILE_INGESTION)),
            file_upload_session,
            cleaning_handles: Arc::new(Mutex::new(JoinSet::new())),
        })
    }

    /// Initiates a file upload from the local filesystem to XET storage.
    ///
    /// This method reads a file from the specified path, processes it through XET's
    /// deduplication and chunking system, and begins to upload it to the configured endpoint.
    ///
    /// # Arguments
    /// * `file_paths` - List of string paths to the local files to upload. Must be valid, readable files.
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
    /// session.upload_file(["/path/to/document.pdf"])
    ///
    /// # to ensure the upload is complete, call `complete_upload_files`
    /// ```
    #[pyo3(signature = (file_paths))]
    pub fn upload_files(&self, py: Python, file_paths: Vec<String>) -> PyResult<()> {
        let cleaning_handles = self.cleaning_handles.clone();
        let file_upload_session = self.file_upload_session.clone();
        let semaphore = self.semaphore.clone();
        async_run(py, async move {
            let mut guard = cleaning_handles.lock().await;
            for file_path in file_paths {
                let reader = File::open(&file_path)?;
                let file_size = reader.metadata()?.len();

                guard.spawn(clean_reader(
                    file_upload_session.clone(),
                    semaphore.clone(),
                    reader,
                    Some(file_path.into()),
                    file_size as usize,
                ));
            }
            Ok(())
        })
    }

    /// Initiates a file upload from raw byte data (e.g. `bytes` or `bytearray`) to XET storage.
    ///
    /// This method takes a byte vector and uploads it directly to XET storage without
    /// requiring a temporary file. The data is processed through the same deduplication
    /// and chunking system as file uploads.
    ///
    /// # Arguments
    /// * `file_contents` - List of byte arrays representing the data to upload.
    /// * `file_path` - Optional List of string argument unique to this upload operation. Used for progress tracking.
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
    /// session.upload_bytes([data])
    ///
    /// # to ensure the upload is complete, call `complete_upload_files`
    /// ```
    #[pyo3(signature = (file_contents, file_paths = None))]
    pub fn upload_bytes(
        &self,
        py: Python,
        file_contents: Vec<Vec<u8>>,
        file_paths: Option<Vec<String>>,
    ) -> PyResult<()> {
        if file_paths.as_ref().is_some_and(|fp| fp.len() != file_contents.len()) {
            return Err(PyValueError::new_err(
                "file paths supplied but number of file paths does not match number of byte slices",
            ));
        }
        let cleaning_handles = self.cleaning_handles.clone();
        let file_upload_session = self.file_upload_session.clone();
        let semaphore = self.semaphore.clone();
        async_run(py, async move {
            let mut guard = cleaning_handles.lock().await;
            for (i, contents) in file_contents.into_iter().enumerate() {
                let file_path = file_paths.as_ref().map(|file_paths| file_paths[i].clone().into());

                let contents_size = contents.len();
                let reader = Cursor::new(contents);
                guard.spawn(clean_reader(
                    file_upload_session.clone(),
                    semaphore.clone(),
                    reader,
                    file_path,
                    contents_size,
                ));
            }
            Ok(())
        })
    }

    /// Completes all pending file uploads and ensures they are finalized.
    ///
    /// This method waits for all files that have been submitted for upload through
    /// `upload_file()` or `upload_bytes()` to finish uploading to XET storage.
    /// It finalizes the upload of all files submitted before being called,
    /// ensuring that all data has been properly transmitted and committed to the storage system.
    ///
    /// This is typically called after a whole batch of files has been queued for upload to
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
    /// session.upload_files(["/path/to/file1.txt", "/path/to/file2.txt"])
    /// session.upload_files(["/path/to/file3.txt"])
    ///
    /// # Wait for all uploads to complete
    /// session.complete_upload_files()
    /// print("All uploads completed successfully")
    /// ```
    #[pyo3(signature = ())]
    pub fn complete_upload_files(&self, py: Python) -> PyResult<()> {
        let file_upload_session = self.file_upload_session.clone();
        let cleaning_handles = self.cleaning_handles.clone();
        async_run(py, async move {
            let mut joinset_guard = cleaning_handles.lock().await;
            while let Some(result) = joinset_guard.join_next().await {
                result
                    .map_err(|e| convert_data_processing_error(DataProcessingError::from(e)))?
                    .map_err(convert_data_processing_error)?;
            }
            file_upload_session
                .clone()
                .finalize()
                .await
                .map_err(convert_data_processing_error)
        })?;
        Ok(())
    }
}

// utility function to remove duplicate code, pass data from a reader through a single file cleaner
async fn clean_reader<R: Read + Send + 'static>(
    file_upload_session: Arc<FileUploadSession>,
    semaphore: Arc<Semaphore>,
    mut reader: R,
    file_path: Option<Arc<str>>,
    file_size: usize,
) -> Result<XetFileInfo, DataProcessingError> {
    let _permit = semaphore.acquire().await;
    let mut handle = file_upload_session.start_clean(file_path, file_size as u64).await;

    let ingestion_block_size = file_size.min(*INGESTION_BLOCK_SIZE);
    let mut buffer = vec![0u8; ingestion_block_size];

    loop {
        let bytes = reader.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }

        handle.add_data(&buffer[0..bytes]).await?;
    }

    let (info, _) = handle.finish().await?;

    Ok(info)
}
