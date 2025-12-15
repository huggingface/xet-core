use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum FileReconstructionError {
    #[error("CAS Client Error: {0}")]
    CasClientError(#[from] cas_client::CasClientError),

    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, FileReconstructionError>;
