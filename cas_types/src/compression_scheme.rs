use anyhow::anyhow;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CompressionScheme {
    None,
    LZ4,
}

impl From<&CompressionScheme> for &'static str {
    fn from(value: &CompressionScheme) -> Self {
        match value {
            CompressionScheme::None => "none",
            CompressionScheme::LZ4 => "lz4",
        }
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
        match s.to_lowercase().as_str() {
            "" | "none" => Ok(CompressionScheme::None),
            "lz4" => Ok(CompressionScheme::LZ4),
            _ => Err(anyhow!("invalid value for compression scheme: {s}")),
        }
    }
}

// in the header value, we will consider
pub fn multiple_accepted_encoding_header_value(list: Vec<CompressionScheme>) -> String {
    let as_strs: Vec<&str> = list.iter().map(Into::into).collect();
    as_strs.join(";").to_string()
}

#[cfg(test)]
mod tests {
    use super::{multiple_accepted_encoding_header_value, CompressionScheme};
    use std::str::FromStr;

    #[test]
    fn test_from_str() {
        assert_eq!(
            CompressionScheme::from_str("LZ4").unwrap(),
            CompressionScheme::LZ4
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
        assert_eq!(Into::<&str>::into(CompressionScheme::LZ4), "lz4");
        assert_eq!(Into::<&str>::into(CompressionScheme::None), "none");
    }

    #[test]
    fn test_multiple_accepted_encoding_header_value() {
        let multi = vec![CompressionScheme::LZ4, CompressionScheme::None];
        assert_eq!(
            multiple_accepted_encoding_header_value(multi),
            "lz4;none".to_string()
        );

        let singular = vec![CompressionScheme::LZ4];
        assert_eq!(
            multiple_accepted_encoding_header_value(singular),
            "lz4".to_string()
        );
    }
}
