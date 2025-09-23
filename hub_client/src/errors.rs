use thiserror::Error;

#[derive(Error, Debug)]
pub enum HubClientError {
    #[error("Cas client error: {0}")]
    CasClient(#[from] cas_client::CasClientError),

    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Reqwest middleware error: {0}")]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),

    #[error("Credential helper error: {0}")]
    CredentialHelper(anyhow::Error),

    #[error("Invalid repo type: {0}")]
    InvalidRepoType(String),
}

pub type Result<T> = std::result::Result<T, HubClientError>;

impl HubClientError {
    pub fn credential_helper_error(e: impl std::error::Error + Send + Sync + 'static) -> HubClientError {
        HubClientError::CredentialHelper(e.into())
    }
}
