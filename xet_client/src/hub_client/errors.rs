pub use crate::error::ClientError as HubClientError;
pub type Result<T> = std::result::Result<T, HubClientError>;

impl HubClientError {
    pub fn credential_helper_error(e: impl std::fmt::Display) -> HubClientError {
        HubClientError::CredentialHelper(e.to_string())
    }
}
