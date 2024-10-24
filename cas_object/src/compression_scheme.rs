use std::fmt::Display;
use std::str::FromStr;

use anyhow::anyhow;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum CompressionScheme {
    #[default]
    None = 0,
    LZ4 = 1,
}

impl Display for CompressionScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionScheme::None => write!(f, "none"),
            CompressionScheme::LZ4 => write!(f, "lz4"),
        }
    }
}

impl TryFrom<u8> for CompressionScheme {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionScheme::None),
            1 => Ok(CompressionScheme::LZ4),
            _ => Err(anyhow!("cannot convert value {value} to CompressionScheme")),
        }
    }
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::CompressionScheme;

    #[test]
    fn test_from_str() {
        assert_eq!(CompressionScheme::from_str("LZ4").unwrap(), CompressionScheme::LZ4);
        assert_eq!(CompressionScheme::from_str("NONE").unwrap(), CompressionScheme::None);
        assert_eq!(CompressionScheme::from_str("NoNE").unwrap(), CompressionScheme::None);
        assert_eq!(CompressionScheme::from_str("none").unwrap(), CompressionScheme::None);
        assert_eq!(CompressionScheme::from_str("").unwrap(), CompressionScheme::None);
        assert!(CompressionScheme::from_str("not-scheme").is_err());
    }

    #[test]
    fn test_to_str() {
        assert_eq!(Into::<&str>::into(CompressionScheme::LZ4), "lz4");
        assert_eq!(Into::<&str>::into(CompressionScheme::None), "none");
    }
}
