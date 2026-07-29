use thiserror::Error;

/// Max chars of bad input embedded in error messages (logs / Python exceptions).
const MAX_EMBEDDED_INPUT_CHARS: usize = 72;

/// Errors that can occur when parsing a [`super::DataHash`].
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum DataHashError {
    /// Returned when [`super::DataHash::from_hex`] fails.
    #[error("Invalid hex input for DataHash (got '{input}')")]
    InvalidHex {
        /// The input string that failed to parse (possibly truncated).
        input: String,
    },

    /// Returned when converting bytes (or base64) to a [`super::DataHash`] fails.
    #[error("Invalid bytes input for DataHash (got '{input}')")]
    InvalidBytes {
        /// Human-readable form of the bad input (hex for raw bytes, or the original
        /// string; possibly truncated).
        input: String,
    },
}

impl DataHashError {
    /// Returns the bad input string for either variant (possibly truncated).
    pub fn input(&self) -> &str {
        match self {
            Self::InvalidHex { input } => input,
            Self::InvalidBytes { input } => input,
        }
    }

    pub(crate) fn invalid_hex(input: &str) -> Self {
        Self::InvalidHex {
            input: Self::truncate_for_display(input),
        }
    }

    pub(crate) fn invalid_bytes_str(input: &str) -> Self {
        Self::InvalidBytes {
            input: Self::truncate_for_display(input),
        }
    }

    pub(crate) fn invalid_bytes_slice(bytes: &[u8]) -> Self {
        Self::InvalidBytes {
            input: Self::truncate_for_display(&Self::format_bytes(bytes)),
        }
    }

    /// Truncate oversized inputs before embedding them in error messages.
    fn truncate_for_display(input: &str) -> String {
        // Inputs are ASCII (hex / base64); truncate by byte length.
        if input.len() <= MAX_EMBEDDED_INPUT_CHARS {
            return input.to_owned();
        }
        let mut truncated = input[..MAX_EMBEDDED_INPUT_CHARS].to_owned();
        truncated.push_str("...");
        truncated
    }

    /// Formats raw bytes as a lowercase hex string for error messages.
    fn format_bytes(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
