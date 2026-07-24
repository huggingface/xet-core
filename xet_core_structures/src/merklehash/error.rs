use thiserror::Error;

/// Errors that can occur when parsing a [`super::DataHash`].
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum DataHashError {
    /// Returned when [`super::DataHash::from_hex`] fails.
    #[error("Invalid hex input for DataHash (got '{input}')")]
    InvalidHex {
        /// The input string that failed to parse.
        input: String,
    },

    /// Returned when converting bytes (or base64) to a [`super::DataHash`] fails.
    #[error("Invalid bytes input for DataHash (got '{input}')")]
    InvalidBytes {
        /// Human-readable form of the bad input (hex for raw bytes, or the original string).
        input: String,
    },
}

impl DataHashError {
    /// The bad hex input when this is [`Self::InvalidHex`], otherwise `""`.
    pub fn hex_input(&self) -> &str {
        match self {
            Self::InvalidHex { input } => input,
            Self::InvalidBytes { .. } => "",
        }
    }

    /// Formats raw bytes as a lowercase hex string for error messages.
    pub(crate) fn format_bytes(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
