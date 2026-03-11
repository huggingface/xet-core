#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Unknown config field: '{0}'")]
    UnknownField(String),

    #[error("Unknown config group: '{0}'")]
    UnknownGroup(String),

    #[error("Invalid config path '{0}': expected 'group.field' format")]
    InvalidPath(String),

    #[error("Failed to parse value '{value}' for field '{field}'")]
    ParseError { field: String, value: String },
}
