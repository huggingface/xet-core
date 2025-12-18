use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum FileReconstructionError {
    #[error("CAS Client Error: {0}")]
    CasClientError(#[from] cas_client::CasClientError),

    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Task Runtime Error: {0}")]
    TaskRuntimeError(#[from] utils::RwTaskLockError),

    #[error("Corrupted Reconstruction: {0}")]
    CorruptedReconstruction(String),

    #[error("Configuration Error: {0}")]
    ConfigurationError(String),
}

pub type Result<T> = std::result::Result<T, FileReconstructionError>;
