use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CasTypesError {
    #[error("Invalid key: {0}")]
    InvalidKey(String),
}
