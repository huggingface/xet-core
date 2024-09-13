use xet_error::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ShardClientError {
    #[error("File I/O error")]
    IOError(#[from] std::io::Error),

    #[error("LMDB Error: {0}")]
    ShardDedupDBError(String),

    #[error("Data Parsing Error: {0}")]
    DataParsingError(#[from] serde_json::Error),

    #[error("Error : {0}")]
    Other(String),

    #[error("MerkleDB Shard Error : {0}")]
    MDBShardError(#[from] mdb_shard::error::MDBShardError),

    #[error("Client connection error: {0}")]
    HttpClientError(#[from] reqwest::Error),

    #[error("Bad endpoint: {0}")]
    UrlError(#[from] url::ParseError),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, ShardClientError>;
