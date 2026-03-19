use std::fmt::Debug;
use std::num::TryFromIntError;

use http::StatusCode;
use thiserror::Error;
use tokio::sync::AcquireError;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;
use xet_core_structures::merklehash::MerkleHash;

use crate::cas_client::auth::AuthError;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Format error: {0}")]
    FormatError(#[from] xet_core_structures::CoreError),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Invalid range")]
    InvalidRange,

    #[error("Invalid arguments")]
    InvalidArguments,

    #[error("File not found for hash: {0}")]
    FileNotFound(MerkleHash),

    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Invalid shard key: {0}")]
    InvalidShardKey(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("{0}")]
    Other(String),

    #[error("URL parse error: {0}")]
    ParseError(#[from] url::ParseError),

    #[error("Request middleware error: {0}")]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("Request error: {0}, domain: {1}")]
    ReqwestError(reqwest::Error, String),

    #[error("LMDB error: {0}")]
    ShardDedupDBError(String),

    #[error("CAS object not found for hash: {0}")]
    XORBNotFound(MerkleHash),

    #[error("Presigned URL expired")]
    PresignedUrlExpirationError,

    #[error("Cloned error: {0}")]
    Cloned(String),

    #[error("Auth error: {0}")]
    AuthError(#[from] AuthError),

    #[error("Credential helper error: {0}")]
    CredentialHelper(String),

    #[error("Invalid repo type: {0}")]
    InvalidRepoType(String),

    #[error("Invalid key: {0}")]
    InvalidKey(String),

    #[error("Cache error: {0}")]
    CacheError(String),
}

impl Clone for ClientError {
    fn clone(&self) -> Self {
        match self {
            ClientError::Cloned(s) => ClientError::Cloned(s.clone()),
            other => ClientError::Cloned(format!("{other:?}")),
        }
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(mut value: reqwest::Error) -> Self {
        let url = if let Some(url) = value.url_mut() {
            url.set_query(None);
            url.to_string()
        } else {
            "no-url".to_string()
        };
        let value = value.without_url();
        ClientError::ReqwestError(value, url)
    }
}

impl ClientError {
    pub fn internal(value: impl Debug) -> Self {
        ClientError::InternalError(format!("{value:?}"))
    }

    pub fn credential_helper_error(e: impl std::fmt::Display) -> Self {
        ClientError::CredentialHelper(e.to_string())
    }

    pub fn status(&self) -> Option<StatusCode> {
        match self {
            ClientError::ReqwestMiddlewareError(e) => e.status(),
            ClientError::ReqwestError(e, _) => e.status(),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;

impl PartialEq for ClientError {
    fn eq(&self, other: &ClientError) -> bool {
        match (self, other) {
            (ClientError::XORBNotFound(a), ClientError::XORBNotFound(b)) => a == b,
            (e1, e2) => std::mem::discriminant(e1) == std::mem::discriminant(e2),
        }
    }
}

#[cfg(not(target_family = "wasm"))]
impl From<xet_runtime::utils::singleflight::SingleflightError<ClientError>> for ClientError {
    fn from(value: xet_runtime::utils::singleflight::SingleflightError<ClientError>) -> Self {
        match value {
            xet_runtime::utils::singleflight::SingleflightError::InternalError(e) => e,
            e => ClientError::Other(format!("single flight error: {e}")),
        }
    }
}

impl<T> From<std::sync::PoisonError<T>> for ClientError {
    fn from(value: std::sync::PoisonError<T>) -> Self {
        Self::internal(value)
    }
}

impl From<AcquireError> for ClientError {
    fn from(value: AcquireError) -> Self {
        Self::internal(value)
    }
}

impl<T> From<SendError<T>> for ClientError {
    fn from(value: SendError<T>) -> Self {
        Self::internal(value)
    }
}

impl From<JoinError> for ClientError {
    fn from(value: JoinError) -> Self {
        Self::internal(value)
    }
}

impl From<TryFromIntError> for ClientError {
    fn from(value: TryFromIntError) -> Self {
        Self::internal(value)
    }
}

impl From<crate::chunk_cache::error::ChunkCacheError> for ClientError {
    fn from(e: crate::chunk_cache::error::ChunkCacheError) -> Self {
        ClientError::CacheError(e.to_string())
    }
}
