//! Python bindings for the session-based file upload/download API

use std::sync::Arc;
use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use xet_session::{
    XetSession, UploadCommit, DownloadGroup,
    UploadProgress, DownloadProgress,
    FileMetadata, DownloadResult, TaskStatus,
};
use xet_config::XetConfig;
use crate::token_refresh::WrappedTokenRefresher;
use xet_runtime::sync_primatives::spawn_os_thread;

/// Python wrapper for XetSession
#[pyclass(name = "XetSession")]
pub struct PyXetSession {
    inner: Arc<XetSession>,
}

#[pymethods]
impl PyXetSession {
    #[new]
    #[pyo3(signature = (config=None, endpoint=None, token_info=None, token_refresher=None))]
    pub fn new(
        config: Option<PyXetConfig>,
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let rust_config = match config {
            Some(c) => c.into_rust(),
            None => XetConfig::new(),
        };

        let refresher = match token_refresher {
            Some(func) => {
                let wrapped = WrappedTokenRefresher::from_func(func)?;
                Some(Arc::new(wrapped) as Arc<_>)
            }
            None => None,
        };

        let session = XetSession::new_with_auth(
            rust_config,
            endpoint,
            token_info,
            refresher,
        ).map_err(|e| PyRuntimeError::new_err(format!("Failed to create session: {:?}", e)))?;

        Ok(Self { inner: session })
    }

    /// Create a new upload commit
    pub fn new_upload_commit(&self) -> PyResult<PyUploadCommit> {
        let commit = self.inner.new_upload_commit()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create upload commit: {:?}", e)))?;
        Ok(PyUploadCommit {
            inner: commit
        })
    }

    /// Create a new download group
    pub fn new_download_group(&self) -> PyResult<PyDownloadGroup> {
        let group = self.inner.new_download_group()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create download group: {:?}", e)))?;
        Ok(PyDownloadGroup {
            inner: group
        })
    }

    /// End the session - wait for all active commits/groups to finish
    pub fn end(&self, py: Python) -> PyResult<()> {
        let session = self.inner.clone();
        let runtime = session.runtime().clone();

        py.detach(|| {
            spawn_os_thread(move || {
                runtime.external_run_async_task(async move {
                    session.end().await
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to end session: {:?}", e)))
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {:?}", e)))?
            })
            .join()
            .map_err(|e| PyRuntimeError::new_err(format!("Thread join error: {:?}", e)))?
        })
    }
}

/// Python wrapper for UploadCommit
#[pyclass(name = "UploadCommit")]
pub struct PyUploadCommit {
    inner: UploadCommit,
}

#[pymethods]
impl PyUploadCommit {
    /// Upload a file - returns task ID
    pub fn upload_file(&self, py: Python, file_path: String) -> PyResult<String> {
        let commit = self.inner.clone();
        let runtime = commit.session().runtime().clone();

        py.detach(|| {
            spawn_os_thread(move || {
                runtime.external_run_async_task(async move {
                    commit.upload_file(file_path).await
                        .map(|id| id.to_string())
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to upload file: {:?}", e)))
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {:?}", e)))?
            })
            .join()
            .map_err(|e| PyRuntimeError::new_err(format!("Thread join error: {:?}", e)))?
        })
    }

    /// Upload bytes - returns task ID
    pub fn upload_bytes(&self, py: Python, bytes: Vec<u8>) -> PyResult<String> {
        let commit = self.inner.clone();
        let runtime = commit.session().runtime().clone();

        py.detach(|| {
            spawn_os_thread(move || {
                runtime.external_run_async_task(async move {
                    commit.upload_bytes(bytes).await
                        .map(|id| id.to_string())
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to upload bytes: {:?}", e)))
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {:?}", e)))?
            })
            .join()
            .map_err(|e| PyRuntimeError::new_err(format!("Thread join error: {:?}", e)))?
        })
    }

    /// Get progress for all uploads in this commit - fast, GIL-free
    pub fn get_progress(&self) -> PyResult<Vec<PyUploadProgress>> {
        let progress = self.inner.get_progress();
        Ok(progress.into_iter().map(PyUploadProgress::from).collect())
    }

    /// Commit - finalize all uploads, push to remote, return metadata
    pub fn commit(&self, py: Python) -> PyResult<Vec<PyFileMetadata>> {
        let commit = self.inner.clone();
        let runtime = commit.session().runtime().clone();

        py.detach(|| {
            spawn_os_thread(move || {
                runtime.external_run_async_task(async move {
                    commit.commit().await
                        .map(|metadata| metadata.into_iter().map(PyFileMetadata::from).collect())
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to commit: {:?}", e)))
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {:?}", e)))?
            })
            .join()
            .map_err(|e| PyRuntimeError::new_err(format!("Thread join error: {:?}", e)))?
        })
    }
}

/// Python wrapper for DownloadGroup
#[pyclass(name = "DownloadGroup")]
pub struct PyDownloadGroup {
    inner: DownloadGroup,
}

#[pymethods]
impl PyDownloadGroup {
    /// Download a file - returns task ID
    pub fn download_file(
        &self,
        py: Python,
        file_hash: String,
        file_size: u64,
        dest_path: String,
    ) -> PyResult<String> {
        let group = self.inner.clone();
        let runtime = group.session().runtime().clone();

        py.detach(|| {
            spawn_os_thread(move || {
                runtime.external_run_async_task(async move {
                    group.download_file(file_hash, file_size, dest_path).await
                        .map(|id| id.to_string())
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to download file: {:?}", e)))
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {:?}", e)))?
            })
            .join()
            .map_err(|e| PyRuntimeError::new_err(format!("Thread join error: {:?}", e)))?
        })
    }

    /// Download bytes - returns task ID and bytes
    pub fn download_bytes(
        &self,
        py: Python,
        file_hash: String,
        file_size: u64,
    ) -> PyResult<(String, Vec<u8>)> {
        let group = self.inner.clone();
        let runtime = group.session().runtime().clone();

        py.detach(|| {
            spawn_os_thread(move || {
                runtime.external_run_async_task(async move {
                    group.download_bytes(file_hash, file_size).await
                        .map(|(id, bytes)| (id.to_string(), bytes))
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to download bytes: {:?}", e)))
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {:?}", e)))?
            })
            .join()
            .map_err(|e| PyRuntimeError::new_err(format!("Thread join error: {:?}", e)))?
        })
    }

    /// Get progress for all downloads in this group - fast, GIL-free
    pub fn get_progress(&self) -> PyResult<Vec<PyDownloadProgress>> {
        let progress = self.inner.get_progress();
        Ok(progress.into_iter().map(PyDownloadProgress::from).collect())
    }

    /// Finish - wait for all downloads, return results
    pub fn finish(&self, py: Python) -> PyResult<Vec<PyDownloadResult>> {
        let group = self.inner.clone();
        let runtime = group.session().runtime().clone();

        py.detach(|| {
            spawn_os_thread(move || {
                runtime.external_run_async_task(async move {
                    group.finish().await
                        .map(|results| results.into_iter().map(PyDownloadResult::from).collect())
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to finish: {:?}", e)))
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {:?}", e)))?
            })
            .join()
            .map_err(|e| PyRuntimeError::new_err(format!("Thread join error: {:?}", e)))?
        })
    }
}

/// Progress for a single upload task
#[pyclass(name = "UploadProgress")]
#[derive(Clone)]
pub struct PyUploadProgress {
    #[pyo3(get)]
    pub task_id: String,
    #[pyo3(get)]
    pub file_name: Option<String>,
    #[pyo3(get)]
    pub bytes_completed: u64,
    #[pyo3(get)]
    pub bytes_total: u64,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub speed_bps: f64,
}

impl From<UploadProgress> for PyUploadProgress {
    fn from(p: UploadProgress) -> Self {
        Self {
            task_id: p.task_id.to_string(),
            file_name: p.file_name,
            bytes_completed: p.bytes_completed,
            bytes_total: p.bytes_total,
            status: task_status_to_string(p.status),
            speed_bps: p.speed_bps,
        }
    }
}

#[pymethods]
impl PyUploadProgress {
    fn __repr__(&self) -> String {
        format!(
            "UploadProgress(task_id={}, file_name={:?}, bytes_completed={}, bytes_total={}, status={}, speed_bps={})",
            self.task_id,
            self.file_name,
            self.bytes_completed,
            self.bytes_total,
            self.status,
            self.speed_bps,
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Metadata returned after commit
#[pyclass(name = "FileMetadata")]
#[derive(Clone)]
pub struct PyFileMetadata {
    #[pyo3(get)]
    pub file_name: Option<String>,
    #[pyo3(get)]
    pub hash: String,
    #[pyo3(get)]
    pub file_size: u64,
}

impl From<FileMetadata> for PyFileMetadata {
    fn from(m: FileMetadata) -> Self {
        Self {
            file_name: m.file_name,
            hash: m.hash,
            file_size: m.file_size,
        }
    }
}

#[pymethods]
impl PyFileMetadata {
    fn __repr__(&self) -> String {
        format!(
            "FileMetadata(file_name={:?}, hash={}, file_size={})",
            self.file_name, self.hash, self.file_size
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Progress for a single download task
#[pyclass(name = "DownloadProgress")]
#[derive(Clone)]
pub struct PyDownloadProgress {
    #[pyo3(get)]
    pub task_id: String,
    #[pyo3(get)]
    pub dest_path: String,
    #[pyo3(get)]
    pub file_hash: String,
    #[pyo3(get)]
    pub bytes_completed: u64,
    #[pyo3(get)]
    pub bytes_total: u64,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub speed_bps: f64,
}

impl From<DownloadProgress> for PyDownloadProgress {
    fn from(p: DownloadProgress) -> Self {
        Self {
            task_id: p.task_id.to_string(),
            dest_path: p.dest_path,
            file_hash: p.file_hash,
            bytes_completed: p.bytes_completed,
            bytes_total: p.bytes_total,
            status: task_status_to_string(p.status),
            speed_bps: p.speed_bps,
        }
    }
}

#[pymethods]
impl PyDownloadProgress {
    fn __repr__(&self) -> String {
        format!(
            "DownloadProgress(task_id={}, dest_path={}, file_hash={}, bytes_completed={}, bytes_total={}, status={}, speed_bps={})",
            self.task_id,
            self.dest_path,
            self.file_hash,
            self.bytes_completed,
            self.bytes_total,
            self.status,
            self.speed_bps,
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Result returned after finish
#[pyclass(name = "DownloadResult")]
#[derive(Clone)]
pub struct PyDownloadResult {
    #[pyo3(get)]
    pub dest_path: String,
    #[pyo3(get)]
    pub file_hash: String,
    #[pyo3(get)]
    pub file_size: u64,
}

impl From<DownloadResult> for PyDownloadResult {
    fn from(r: DownloadResult) -> Self {
        Self {
            dest_path: r.dest_path,
            file_hash: r.file_hash,
            file_size: r.file_size,
        }
    }
}

#[pymethods]
impl PyDownloadResult {
    fn __repr__(&self) -> String {
        format!(
            "DownloadResult(dest_path={}, file_hash={}, file_size={})",
            self.dest_path, self.file_hash, self.file_size
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Python wrapper for XetConfig
#[pyclass(name = "XetConfig")]
#[derive(Clone)]
pub struct PyXetConfig {
    inner: XetConfig,
}

#[pymethods]
impl PyXetConfig {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: XetConfig::new(),
        }
    }

    // TODO: Add methods to configure specific fields
    // For now, users will use environment variables
}

impl PyXetConfig {
    pub fn into_rust(self) -> XetConfig {
        self.inner
    }
}

// Helper function to convert TaskStatus to string
fn task_status_to_string(status: TaskStatus) -> String {
    match status {
        TaskStatus::Queued => "Queued".to_string(),
        TaskStatus::Running => "Running".to_string(),
        TaskStatus::Completed => "Completed".to_string(),
        TaskStatus::Failed => "Failed".to_string(),
        TaskStatus::Cancelled => "Cancelled".to_string(),
    }
}
