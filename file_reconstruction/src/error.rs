use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum FileReconstructionError {
    #[error("CAS Client Error: {0}")]
    CasClientError(Arc<cas_client::CasClientError>),

    #[error("IO Error: {0}")]
    IoError(Arc<std::io::Error>),

    #[error("Task Runtime Error: {0}")]
    TaskRuntimeError(Arc<utils::RwTaskLockError>),

    #[error("Corrupted Reconstruction: {0}")]
    CorruptedReconstruction(String),

    #[error("Configuration Error: {0}")]
    ConfigurationError(String),

    #[error("Internal Writer Error: {0}")]
    InternalWriterError(String),
}

pub type Result<T> = std::result::Result<T, FileReconstructionError>;

impl From<std::io::Error> for FileReconstructionError {
    fn from(err: std::io::Error) -> Self {
        FileReconstructionError::IoError(Arc::new(err))
    }
}

impl From<cas_client::CasClientError> for FileReconstructionError {
    fn from(err: cas_client::CasClientError) -> Self {
        FileReconstructionError::CasClientError(Arc::new(err))
    }
}

impl From<utils::RwTaskLockError> for FileReconstructionError {
    fn from(err: utils::RwTaskLockError) -> Self {
        FileReconstructionError::TaskRuntimeError(Arc::new(err))
    }
}

pub struct ErrorState {
    has_error: AtomicBool,
    stored_error: Mutex<Option<FileReconstructionError>>,
}

impl ErrorState {
    pub fn new() -> Self {
        Self {
            has_error: AtomicBool::new(false),
            stored_error: Mutex::new(None),
        }
    }

    pub fn check(&self) -> Result<()> {
        if self.has_error.load(AtomicOrdering::Acquire) {
            let error_guard = self.stored_error.lock().unwrap();
            if let Some(err) = error_guard.as_ref() {
                return Err(err.clone());
            }
            return Err(FileReconstructionError::InternalWriterError(
                "Unknown error occurred in background writer".to_string(),
            ));
        }
        Ok(())
    }

    pub fn set(&self, error: FileReconstructionError) {
        let mut error_guard = self.stored_error.lock().unwrap();
        if error_guard.is_none() {
            *error_guard = Some(error);
            self.has_error.store(true, AtomicOrdering::Release);
        }
    }
}
