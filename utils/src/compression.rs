use crate::common::CompressionScheme;
use anyhow::anyhow;
use std::str::FromStr;

pub const CAS_CONTENT_ENCODING_HEADER: &str = "xet-cas-content-encoding";
pub const CAS_ACCEPT_ENCODING_HEADER: &str = "xet-cas-content-encoding";
pub const CAS_INFLATED_SIZE_HEADER: &str = "xet-cas-inflated-size";

// officially speaking, string representations of the CompressionScheme enum values
// are dictated by prost for generating `as_str_name` and `from_str_name`, and since
// we cannot guarantee no one will use them, we will accept them as the string
// representations instead of CompressionScheme.
// These functions follow protobuf style, so the output strings are uppercase snake case,
// however we will still try to convert lower case/mixed case to CompressionScheme
// as well as count the empty string as CompressionScheme::None.
// we will also attempt to avoid as_str_name/from_str_name in favor of more rusty
// trait usage (From<CompressionScheme>/FromStr)
impl From<&CompressionScheme> for &'static str {
    fn from(value: &CompressionScheme) -> Self {
        value.as_str_name()
    }
}

impl From<CompressionScheme> for &'static str {
    fn from(value: CompressionScheme) -> Self {
        From::from(&value)
    }
}

impl FromStr for CompressionScheme {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(CompressionScheme::None);
        }
        Self::from_str_name(s.to_uppercase().as_str())
            .ok_or_else(|| anyhow!("could not convert &str to CompressionScheme"))
    }
}

// in the header value, we will consider
pub fn multiple_accepted_encoding_header_value(list: Vec<CompressionScheme>) -> String {
    let as_strs: Vec<&str> = list.iter().map(Into::into).collect();
    as_strs.join(";").to_string()
}

#[cfg(test)]
mod tests {
    use crate::compression::{multiple_accepted_encoding_header_value, CompressionScheme};
    use std::str::FromStr;

    #[test]
    fn test_from_str() {
        assert_eq!(
            CompressionScheme::from_str("LZ4").unwrap(),
            CompressionScheme::Lz4
        );
        assert_eq!(
            CompressionScheme::from_str("NONE").unwrap(),
            CompressionScheme::None
        );
        assert_eq!(
            CompressionScheme::from_str("NoNE").unwrap(),
            CompressionScheme::None
        );
        assert_eq!(
            CompressionScheme::from_str("none").unwrap(),
            CompressionScheme::None
        );
        assert_eq!(
            CompressionScheme::from_str("").unwrap(),
            CompressionScheme::None
        );
        assert!(CompressionScheme::from_str("not-scheme").is_err());
    }

    #[test]
    fn test_to_str() {
        assert_eq!(Into::<&str>::into(CompressionScheme::Lz4), "LZ4");
        assert_eq!(Into::<&str>::into(CompressionScheme::None), "NONE");
    }

    #[test]
    fn test_multiple_accepted_encoding_header_value() {
        let multi = vec![CompressionScheme::Lz4, CompressionScheme::None];
        assert_eq!(
            multiple_accepted_encoding_header_value(multi),
            "LZ4;NONE".to_string()
        );

        let singular = vec![CompressionScheme::Lz4];
        assert_eq!(
            multiple_accepted_encoding_header_value(singular),
            "LZ4".to_string()
        );
    }
}
