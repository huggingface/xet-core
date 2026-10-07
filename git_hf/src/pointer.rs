use std::fmt::{self, Display};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use thiserror::Error;
use xet_core_structures::merklehash::MerkleHash;

/// Maximum serialized pointer size, including extension lines and the final LF.
pub const MAX_POINTER_SIZE: usize = 4096;
const POINTER_VERSION: &str = "https://hf.co/spec/xet/v1";

/// A Xet pointer. Serialization uses canonical field order and omits unknown extensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XetPointer {
    /// Xet content hash.
    pub hash: MerkleHash,
    /// Logical file size in bytes, not the size of the pointer blob.
    pub size: u64,
    /// Pointer format version URL.
    pub version: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PointerError {
    #[error("native pointer exceeds the {MAX_POINTER_SIZE}-byte limit")]
    TooLarge,
    #[error("native pointer must be UTF-8 with LF endings and a final newline")]
    InvalidEncoding,
    #[error("malformed native pointer line {line}: expected a key, one ASCII space, and a value")]
    MalformedLine { line: usize },
    #[error("native pointer is missing '{0}'")]
    MissingField(&'static str),
    #[error("native pointer contains duplicate '{0}' fields")]
    DuplicateField(&'static str),
    #[error("unsupported native pointer version: {0}")]
    UnsupportedVersion(String),
    #[error("native pointer oid must be 'xet:' followed by 64 lowercase hexadecimal digits")]
    InvalidOid,
    #[error("native pointer size must be an unsigned 64-bit decimal integer with no leading zeroes")]
    InvalidSize,
    #[error("Git LFS pointers are not supported")]
    UnsupportedLfs,
}

impl XetPointer {
    /// Creates a canonical pointer without extensions.
    pub fn new(hash: MerkleHash, size: u64) -> Self {
        Self {
            hash,
            size,
            version: POINTER_VERSION.to_owned(),
        }
    }

    /// Opens a file and parses at most 4097 bytes, using the same recognition rules as [`Self::parse`].
    /// Returns `Ok(None)` for ordinary file data; preserves I/O and pointer validation errors.
    pub fn parse_file(path: impl AsRef<Path>) -> crate::Result<Option<Self>> {
        let mut bytes = Vec::with_capacity(MAX_POINTER_SIZE + 1);
        File::open(path)?.take((MAX_POINTER_SIZE + 1) as u64).read_to_end(&mut bytes)?;
        Ok(Self::parse(&bytes)?)
    }

    /// Distinguishes ordinary file data (`Ok(None)`) from a valid pointer or a recognized invalid pointer.
    ///
    /// Recognition looks for a native version URL or `oid xet:` line within the first 4 KiB, regardless
    /// of field order. LFS version URLs are rejected separately. Marker recognition tolerates case and
    /// whitespace errors so those inputs fail validation instead of being ingested as ordinary data.
    /// The caller should pass the complete small blob or a prefix of at least 4 KiB + 1 bytes when
    /// probing larger input. The probe never scans or copies an entire large file.
    pub fn parse(bytes: &[u8]) -> Result<Option<Self>, PointerError> {
        let prefix = &bytes[..bytes.len().min(MAX_POINTER_SIZE)];
        let mut native = false;
        let mut version = None;
        let mut oid = None;
        let mut size = None;
        let mut parse_error = None;
        for (index, line) in prefix.split(|&b| b == b'\n').enumerate() {
            let mut words = line.split(|b| b.is_ascii_whitespace()).filter(|word| !word.is_empty());
            if let (Some(key), Some(value)) = (words.next(), words.next()) {
                if key.eq_ignore_ascii_case(b"version") {
                    if starts_with_ignore_ascii_case(value, b"https://git-lfs.github.com/spec/") {
                        return Err(PointerError::UnsupportedLfs);
                    }
                    native |= starts_with_ignore_ascii_case(value, b"https://hf.co/spec/xet");
                }
                native |= key.eq_ignore_ascii_case(b"oid") && starts_with_ignore_ascii_case(value, b"xet:");
            }
            if line.is_empty() || parse_error.is_some() {
                continue;
            }
            // Defer validation errors until a marker distinguishes a pointer from ordinary file data.
            let result = (|| {
                let line = std::str::from_utf8(line).map_err(|_| PointerError::InvalidEncoding)?;
                let malformed = || PointerError::MalformedLine { line: index + 1 };
                let (key, value) = line.split_once(' ').ok_or_else(malformed)?;
                if key.is_empty()
                    || key.chars().any(|c| c.is_whitespace() || c.is_control())
                    || value.is_empty()
                    || value.starts_with(char::is_whitespace)
                    || value.chars().any(char::is_control)
                {
                    return Err(malformed());
                }
                let (field, slot) = match key {
                    "version" => ("version", &mut version),
                    "oid" => ("oid", &mut oid),
                    "size" => ("size", &mut size),
                    _ => return Ok(()),
                };
                if slot.replace(value).is_some() {
                    return Err(PointerError::DuplicateField(field));
                }
                Ok(())
            })();
            parse_error = result.err();
        }
        if !native {
            return Ok(None);
        }
        if bytes.len() > MAX_POINTER_SIZE {
            return Err(PointerError::TooLarge);
        }
        let text = std::str::from_utf8(bytes).map_err(|_| PointerError::InvalidEncoding)?;
        if !text.ends_with('\n') || text.contains('\r') {
            return Err(PointerError::InvalidEncoding);
        }
        if let Some(error) = parse_error {
            return Err(error);
        }
        let version = version.ok_or(PointerError::MissingField("version"))?;
        if version != POINTER_VERSION {
            return Err(PointerError::UnsupportedVersion(version.to_owned()));
        }
        let hex = oid
            .ok_or(PointerError::MissingField("oid"))?
            .strip_prefix("xet:")
            .ok_or(PointerError::InvalidOid)?;
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err(PointerError::InvalidOid);
        }
        let hash = MerkleHash::from_hex(hex).map_err(|_| PointerError::InvalidOid)?;
        let size = size.ok_or(PointerError::MissingField("size"))?;
        if !size.bytes().all(|b| b.is_ascii_digit()) || (size.len() > 1 && size.starts_with('0')) {
            return Err(PointerError::InvalidSize);
        }
        let size = size.parse().map_err(|_| PointerError::InvalidSize)?;
        Ok(Some(Self {
            hash,
            size,
            version: version.to_owned(),
        }))
    }

    /// Writes canonical bytes, omitting unknown extensions and blank lines.
    pub fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        write!(writer, "{self}")
    }
}

impl Display for XetPointer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "version {}\noid xet:{}\nsize {}\n", self.version, self.hash, self.size)
    }
}

fn starts_with_ignore_ascii_case(value: &[u8], prefix: &[u8]) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::GitHfError;

    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn canonical() -> String {
        format!("version {POINTER_VERSION}\noid xet:{HASH}\nsize 12345\n")
    }

    #[test]
    fn parse_file_matches_byte_parser() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pointer");
        let mut maximum = canonical();
        maximum.push_str(&"\n".repeat(MAX_POINTER_SIZE - maximum.len()));
        for contents in [
            canonical().into_bytes(),
            maximum.as_bytes().to_vec(),
            format!("{maximum}\n").into_bytes(),
            canonical().replace("12345", "invalid").into_bytes(),
            b"version https://git-lfs.github.com/spec/v1\n".to_vec(),
            Vec::new(),
            vec![0xff; MAX_POINTER_SIZE * 2],
        ] {
            std::fs::write(&path, &contents).unwrap();
            match XetPointer::parse(&contents) {
                Ok(expected) => assert_eq!(XetPointer::parse_file(&path).unwrap(), expected),
                Err(expected) => assert!(matches!(
                    XetPointer::parse_file(&path),
                    Err(GitHfError::Pointer(error)) if error == expected
                )),
            }
        }
    }

    #[test]
    fn parse_file_preserves_io_error() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(
            XetPointer::parse_file(dir.path().join("missing")),
            Err(GitHfError::IO(error)) if error.kind() == io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn canonical_output_and_roundtrip() {
        let pointer = XetPointer::new(MerkleHash::from_hex(HASH).unwrap(), 12345);
        assert_eq!(pointer.to_string(), canonical());
        let parsed = XetPointer::parse(pointer.to_string().as_bytes()).unwrap().unwrap();
        assert_eq!(parsed, pointer);
        assert_eq!(parsed.hash.hex(), HASH);
        assert_eq!(parsed.size, 12345);
        assert_eq!(parsed.version, POINTER_VERSION);
        let mut written = Vec::new();
        parsed.write_to(&mut written).unwrap();
        assert_eq!(written, canonical().as_bytes());
    }

    #[test]
    fn canonicalizes_order_and_discards_blank_lines_and_unknown_extensions() {
        let input = format!(
            "future-key résumé with spaces\nsize 12345\n\noid xet:{HASH}\nversion {POINTER_VERSION}\nfuture-key second value\n未来-key value\n"
        );
        let parsed = XetPointer::parse(input.as_bytes()).unwrap().unwrap();
        assert_eq!(parsed.size, 12345);
        assert_eq!(parsed.version, POINTER_VERSION);
        assert_eq!(parsed.hash.hex(), HASH);
        let mut written = Vec::new();
        parsed.write_to(&mut written).unwrap();
        assert_eq!(written, canonical().as_bytes());
        assert_eq!(parsed, XetPointer::new(MerkleHash::from_hex(HASH).unwrap(), 12345));
    }

    #[test]
    fn byte_limit_includes_extensions_and_final_newline() {
        let mut input = canonical();
        input.push_str("extra ");
        input.push_str(&"x".repeat(MAX_POINTER_SIZE - input.len() - 1));
        input.push('\n');
        assert_eq!(input.len(), MAX_POINTER_SIZE);
        assert!(XetPointer::parse(input.as_bytes()).unwrap().is_some());
        input.push('\n');
        assert_eq!(XetPointer::parse(input.as_bytes()), Err(PointerError::TooLarge));
    }

    #[test]
    fn required_fields_are_unique_and_required() {
        for field in ["version", "oid", "size"] {
            let valid = canonical();
            let line = valid.lines().find(|line| line.starts_with(field)).unwrap();
            let duplicate = format!("{valid}{line}\n");
            assert_eq!(XetPointer::parse(duplicate.as_bytes()), Err(PointerError::DuplicateField(field)));
            let missing = valid.replace(&format!("{line}\n"), "");
            assert_eq!(XetPointer::parse(missing.as_bytes()), Err(PointerError::MissingField(field)));
        }
    }

    #[test]
    fn size_boundaries_and_invalid_sizes() {
        for value in ["0", "1", "18446744073709551615"] {
            let input = canonical().replace("12345", value);
            assert_eq!(XetPointer::parse(input.as_bytes()).unwrap().unwrap().size, value.parse::<u64>().unwrap());
        }
        for value in ["00", "01", "-1", "+1", "1.0", "1 ", "18446744073709551616", "１２"] {
            assert_eq!(
                XetPointer::parse(canonical().replace("12345", value).as_bytes()),
                Err(PointerError::InvalidSize),
                "{value}"
            );
        }
    }

    #[test]
    fn oid_is_exact() {
        for value in [
            HASH.to_uppercase(),
            HASH[..63].to_owned(),
            format!("{HASH}0"),
            "g".repeat(64),
        ] {
            assert_eq!(XetPointer::parse(canonical().replace(HASH, &value).as_bytes()), Err(PointerError::InvalidOid));
        }
        for scheme in ["XET:", "sha256:", ""] {
            assert_eq!(
                XetPointer::parse(canonical().replace("xet:", scheme).as_bytes()),
                Err(PointerError::InvalidOid)
            );
        }
    }

    #[test]
    fn unsupported_versions_and_lfs_are_not_ordinary_data() {
        for version in ["https://hf.co/spec/xet/v2", "https://HF.co/spec/xet/v1", "other"] {
            assert_eq!(
                XetPointer::parse(canonical().replace(POINTER_VERSION, version).as_bytes()),
                Err(PointerError::UnsupportedVersion(version.into()))
            );
        }
        assert_eq!(
            XetPointer::parse(b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 1\n"),
            Err(PointerError::UnsupportedLfs)
        );
        assert!(matches!(
            XetPointer::parse(b"version https://hf.co/spec/xet/v2\nsize 1\n"),
            Err(PointerError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn malformed_native_data_is_rejected() {
        for input in [
            canonical().replace('\n', "\r\n"),
            canonical().trim_end().to_owned(),
            canonical().replace("size ", "size\t"),
            canonical().replace("size ", "size  "),
            canonical().replace("size 12345", "size "),
            canonical().replace("version ", " version "),
            format!("{}bad-extension\n", canonical()),
            format!("bad-extension\n{}", canonical()),
            format!("{}extension bad\0value\n", canonical()),
        ] {
            assert!(XetPointer::parse(input.as_bytes()).is_err(), "{input:?}");
        }
        let mut invalid_utf8 = canonical().into_bytes();
        invalid_utf8.extend_from_slice(b"extension \xff\n");
        assert_eq!(XetPointer::parse(&invalid_utf8), Err(PointerError::InvalidEncoding));
    }

    #[test]
    fn ordinary_data_is_not_a_pointer() {
        for input in [b"".as_slice(), b"hello\n", b"version 1.0\nsize 12\n", b"\0\xffbinary"] {
            assert_eq!(XetPointer::parse(input).unwrap(), None);
        }
        assert_eq!(XetPointer::parse(&vec![b'x'; 8192]).unwrap(), None);
    }

    #[test]
    fn write_errors_propagate() {
        struct BrokenWriter;
        impl Write for BrokenWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        assert_eq!(
            XetPointer::new(MerkleHash::default(), 0)
                .write_to(&mut BrokenWriter)
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}
