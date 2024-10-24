use std::str::FromStr;

use lazy_static::lazy_static;
use regex::{Captures, Regex};
use xet_error::Error;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl Version {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum VersionError {
    #[error("Could not parse {0} as version, expecting standard semantic version e.g. \"0.8.2\"")]
    ParseFailed(String),
}

const MAJOR_KEY: &str = "major";
const MINOR_KEY: &str = "minor";
const PATCH_KEY: &str = "patch";

lazy_static! {
    // matching semantic versions with optional `v` at the start and optional suffix beginning with `-`
    // patch version is expected but optional
    // valid forms:
    //  1.0.0
    //  v0.9.2
    //  0.123.0-anything
    //  v0.0.0-multi-dash
    //  0.1
    //  v0.1-test
    // invalid forms:
    //  x.0.0
    //  0.0.0-
    //  x7.9.0
    static ref VERSION_REGEX: Regex = Regex::new(
        format!(r"^v?(?P<{MAJOR_KEY}>\d+)\.(?P<{MINOR_KEY}>\d+)(\.(?P<{PATCH_KEY}>\d+))?(-.+)?$").as_str()
    )
    .unwrap();
}

/// util to convert regex capture group to parsable
/// captures must have named regex group and specifically have
/// the specific name given
#[inline]
fn parse_from_capture_by_key<'a, T>(captures: &Captures<'a>, name: &'a str) -> Result<T, VersionError>
where
    T: FromStr,
{
    let capture = if let Some(major) = captures.name(name) {
        major
    } else {
        return Err(VersionError::ParseFailed(format!("invalid {name}")));
    };

    capture
        .as_str()
        .parse::<T>()
        .map_err(|_| VersionError::ParseFailed(format!("invalid {name}")))
}

/// enable version string to Version struct conversion
impl TryFrom<&str> for Version {
    type Error = VersionError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let captures = match VERSION_REGEX.captures(value) {
            Some(captures) => captures,
            None => return Err(VersionError::ParseFailed(value.to_string())),
        };

        let major = parse_from_capture_by_key::<u32>(&captures, MAJOR_KEY)?;
        let minor = parse_from_capture_by_key::<u32>(&captures, MINOR_KEY)?;
        // allow patch to not be included
        let patch = parse_from_capture_by_key::<u32>(&captures, PATCH_KEY).unwrap_or(0);

        Ok(Version { major, minor, patch })
    }
}

/// enable version String to Version struct conversion
impl TryFrom<String> for Version {
    type Error = VersionError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Version::try_from(value.as_str())
    }
}

/// enable comparing versions
impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.major.partial_cmp(&other.major) {
            Some(core::cmp::Ordering::Equal) => {},
            ord => return ord,
        }
        match self.minor.partial_cmp(&other.minor) {
            Some(core::cmp::Ordering::Equal) => {},
            ord => return ord,
        }
        self.patch.partial_cmp(&other.patch)
    }
}

/// allow pretty print
impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Version { major, minor, patch } = self;
        write!(f, "{major}.{minor}.{patch}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_to_string() {
        let version = Version::new(1, 2, 3);
        assert_eq!(version.to_string(), "1.2.3".to_string())
    }

    #[test]
    fn test_version_try_from_str() {
        let expected = Version::new(1, 2, 3);

        let standard_version_str = "1.2.3";
        assert_eq!(Version::try_from(standard_version_str).unwrap(), expected);
    }

    #[test]
    fn test_version_try_from_str_special_cases() {
        let expected = Version::new(1, 2, 0);

        let no_patch = "1.2";
        assert_eq!(Version::try_from(no_patch).unwrap(), expected);

        let with_v = "v1.2.0";
        assert_eq!(Version::try_from(with_v).unwrap(), expected);

        let with_suffix = "v1.2.0-abc";
        assert_eq!(Version::try_from(with_suffix).unwrap(), expected);

        let with_v = "v1.2.0-asdasd-adsda";
        assert_eq!(Version::try_from(with_v).unwrap(), expected);
    }

    #[test]
    fn test_version_try_from_str_failure() {
        let bad_all_over = "bad string";
        assert!(Version::try_from(bad_all_over).is_err());

        let bad_major = "a.2.3";
        assert!(Version::try_from(bad_major).is_err());

        let bad_minor = "1.a.3";
        assert!(Version::try_from(bad_minor).is_err());

        let bad_patch = "1.2.a";
        assert!(Version::try_from(bad_patch).is_err());

        let bad_suffix = "1.2.3aaa";
        assert!(Version::try_from(bad_suffix).is_err());

        let bad_prefix = "p1.2.";
        assert!(Version::try_from(bad_prefix).is_err());
    }

    #[test]
    fn test_version_default() {
        assert_eq!(Version::default(), Version::new(0, 0, 0));
    }

    #[test]
    fn test_version_comparison() {
        let zeros = Version::default();
        let ones = Version::new(1, 1, 1);
        let ninties = Version::new(90, 90, 90);

        // 0.0.0 < 1.1.1 && 1.1.1 < 90.90.90 && 0.0.0 < 90.90.90
        assert!(zeros < ones);
        assert!(ones < ninties);
        assert!(zeros < ninties);

        // 0.0.0 < 0.1.1 && 0.1.1 < 1.1.1
        let zero_one_one = Version::new(0, 1, 1);
        assert!(zeros < zero_one_one);
        assert!(zero_one_one < ones);

        // 0.1.0 < 0.1.1
        let zero_one_zero = Version::new(0, 1, 0);
        assert!(zero_one_zero < zero_one_one);

        // 1.0.0 > 0.1.0 && 1.0.0 > 0.1.1
        let one_zero_zero = Version::new(1, 0, 0);
        assert!(one_zero_zero > zero_one_zero);
        assert!(one_zero_zero > zero_one_one);
    }

    #[test]
    fn test_version_range() {
        let zeros = Version::new(0, 0, 0);
        let o_nine_o = Version::new(0, 9, 0);
        let ones = Version::new(1, 1, 1);
        let ninties = Version::new(90, 90, 90);
        let ninety_ninety_ninety_one = Version::new(90, 90, 90);

        let range = o_nine_o..ninties;
        assert!(!range.contains(&zeros));
        assert!(range.contains(&o_nine_o));
        assert!(range.contains(&ones));
        assert!(!range.contains(&ninties));
        assert!(!range.contains(&ninety_ninety_ninety_one));

        let range_inclusive = o_nine_o..=ninties;
        assert!(!range.contains(&zeros));
        assert!(range_inclusive.contains(&o_nine_o));
        assert!(range_inclusive.contains(&ones));
        assert!(range_inclusive.contains(&ninties));
        assert!(!range.contains(&ninety_ninety_ninety_one));
    }
}
