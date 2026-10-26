//! The Xet pointer file format (`https://hf.co/spec/xet/v1`): a small text file standing in for
//! content stored in Xet, naming its file hash and logical size.
use std::fmt::{self, Display};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use thiserror::Error;

use crate::merklehash::MerkleHash;

/// Maximum serialized pointer size, including extension lines and the final LF. A canonical pointer
/// is 113 bytes plus the size's decimal digits (at most 133), leaving room for a few extension lines.
pub const MAX_POINTER_SIZE: usize = 200;
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
    #[error("native pointer oid must be 'xet:' followed by 64 hexadecimal digits")]
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

    /// Opens a file and parses it with the same rules as [`Self::parse`]. A regular file whose
    /// metadata reports more than [`MAX_POINTER_SIZE`] bytes is ordinary data and is not read.
    /// Returns `Ok(None)` for ordinary file data; preserves I/O and pointer validation errors.
    pub fn parse_file(path: impl AsRef<Path>) -> crate::error::Result<Option<Self>> {
        let file = File::open(path)?;
        // Only a shortcut: correctness comes from the bounded read and `parse`'s own length check.
        // Non-regular files and handles without metadata (e.g. some Windows pipes) take the read path.
        if file.metadata().is_ok_and(|m| m.is_file() && m.len() > MAX_POINTER_SIZE as u64) {
            return Ok(None);
        }
        // The read stays bounded: the file may have grown since, or (like a FIFO) report no size.
        let mut bytes = Vec::with_capacity(MAX_POINTER_SIZE + 1);
        file.take((MAX_POINTER_SIZE + 1) as u64).read_to_end(&mut bytes)?;
        Ok(Self::parse(&bytes)?)
    }

    /// Distinguishes ordinary file data (`Ok(None)`) from a valid pointer or a recognized invalid pointer.
    ///
    /// Input longer than [`MAX_POINTER_SIZE`] is ordinary data, whatever it contains. Otherwise,
    /// recognition looks for a native version URL or `oid xet:` line, regardless of field order. LFS
    /// version URLs are rejected separately.
    ///
    /// Recognition is deliberately looser than validation: marker keys and prefixes match
    /// case-insensitively and lines are split on any whitespace, so near-misses (e.g. `VERSION …`,
    /// tabs, CRLF) are reported as invalid pointers rather than stored as ordinary data. When probing a
    /// larger input, the caller should pass the complete small blob or a prefix of at least
    /// `MAX_POINTER_SIZE + 1` bytes, so oversized input is recognized as ordinary data.
    pub fn parse(bytes: &[u8]) -> Result<Option<Self>, PointerError> {
        if bytes.len() > MAX_POINTER_SIZE {
            return Ok(None);
        }
        let mut native = false;
        let mut version = None;
        let mut oid = None;
        let mut size = None;
        let mut parse_error = None;
        for (index, line) in bytes.split(|&b| b == b'\n').enumerate() {
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
        // Either hex case is accepted; serialization writes lowercase.
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
    #[cfg(not(target_family = "wasm"))]
    use tempfile::TempDir;

    use super::*;
    use crate::error::CoreError;

    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    // Lines: 1 version, 2 oid, 3 size.
    fn canonical() -> String {
        format!("version {POINTER_VERSION}\noid xet:{HASH}\nsize 12345\n")
    }

    fn expected() -> XetPointer {
        XetPointer::new(MerkleHash::from_hex(HASH).unwrap(), 12345)
    }

    fn with(from: &str, to: &str) -> String {
        canonical().replace(from, to)
    }

    fn parse_err(input: impl AsRef<[u8]>) -> PointerError {
        XetPointer::parse(input.as_ref()).unwrap_err()
    }

    fn malformed(line: usize) -> PointerError {
        PointerError::MalformedLine { line }
    }

    // A canonical pointer of `len` bytes, padded with one extension line.
    fn padded_to(len: usize) -> String {
        let mut input = canonical();
        let padding = len - input.len() - "extension \n".len();
        input.push_str(&format!("extension {}\n", "x".repeat(padding)));
        assert_eq!(input.len(), len);
        input
    }

    // Valid pointers

    #[test]
    fn parses_canonical_pointer() {
        let pointer = XetPointer::parse(canonical().as_bytes()).unwrap().unwrap();
        assert_eq!(pointer.hash.hex(), HASH);
        assert_eq!(pointer.size, 12345);
        assert_eq!(pointer.version, POINTER_VERSION);
    }

    #[test]
    fn accepts_fields_in_any_order() {
        let input = format!("size 12345\noid xet:{HASH}\nversion {POINTER_VERSION}\n");
        assert_eq!(XetPointer::parse(input.as_bytes()).unwrap(), Some(expected()));
    }

    #[test]
    fn ignores_blank_lines() {
        let input = format!("\nversion {POINTER_VERSION}\n\noid xet:{HASH}\nsize 12345\n\n");
        assert_eq!(XetPointer::parse(input.as_bytes()).unwrap(), Some(expected()));
    }

    #[test]
    fn ignores_well_formed_unknown_lines() {
        let input = format!("future-key résumé with spaces\n{}未来-key value\n", canonical());
        assert_eq!(XetPointer::parse(input.as_bytes()).unwrap(), Some(expected()));
    }

    #[test]
    fn unknown_keys_may_repeat() {
        let input = format!("{}future-key one\nfuture-key two\n", canonical());
        assert_eq!(XetPointer::parse(input.as_bytes()).unwrap(), Some(expected()));
    }

    #[test]
    fn accepts_pointer_of_maximum_size() {
        let input = padded_to(MAX_POINTER_SIZE);
        assert_eq!(XetPointer::parse(input.as_bytes()).unwrap(), Some(expected()));
    }

    #[test]
    fn accepts_size_boundaries() {
        for value in ["0", "18446744073709551615"] {
            let pointer = XetPointer::parse(with("12345", value).as_bytes()).unwrap().unwrap();
            assert_eq!(pointer.size, value.parse::<u64>().unwrap());
        }
    }

    // Ordinary data

    #[test]
    fn input_without_marker_is_ordinary_data() {
        for input in [b"".as_slice(), b"hello\n", b"version 1.0\nsize 12\n", b"\0\xffbinary"] {
            assert_eq!(XetPointer::parse(input).unwrap(), None);
        }
    }

    // Line validation is deferred until a marker is seen, so malformed text alone is data.
    #[test]
    fn malformed_lines_without_marker_are_ordinary_data() {
        assert_eq!(XetPointer::parse(b"no-space-here\nsize  12\n\tindented\n").unwrap(), None);
    }

    // Anything larger than the limit is data, so its content is never validated.
    #[test]
    fn input_over_maximum_size_is_ordinary_data() {
        let mut input = with("12345", "invalid");
        input.push_str(&"\n".repeat(MAX_POINTER_SIZE + 1 - input.len()));
        assert_eq!(XetPointer::parse(input.as_bytes()).unwrap(), None);
    }

    // Marker recognition

    // Markers match case-insensitively, so a near-miss is a recognized (invalid) pointer, not data.
    #[test]
    fn marker_is_case_insensitive() {
        assert_eq!(parse_err(with("version ", "VERSION ")), PointerError::MissingField("version"));
    }

    // Encoding failures

    #[test]
    fn rejects_missing_final_newline() {
        assert_eq!(parse_err(canonical().trim_end()), PointerError::InvalidEncoding);
    }

    #[test]
    fn rejects_crlf_line_endings() {
        assert_eq!(parse_err(canonical().replace('\n', "\r\n")), PointerError::InvalidEncoding);
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut input = canonical().into_bytes();
        input.extend_from_slice(b"extension \xff\n");
        assert_eq!(parse_err(input), PointerError::InvalidEncoding);
    }

    // Line syntax failures

    #[test]
    fn rejects_line_without_separator() {
        assert_eq!(parse_err(format!("{}bad-extension\n", canonical())), malformed(4));
    }

    #[test]
    fn rejects_tab_separator() {
        assert_eq!(parse_err(with("size ", "size\t")), malformed(3));
    }

    #[test]
    fn rejects_extra_space_after_key() {
        assert_eq!(parse_err(with("size ", "size  ")), malformed(3));
    }

    #[test]
    fn rejects_empty_value() {
        assert_eq!(parse_err(with("size 12345", "size ")), malformed(3));
    }

    #[test]
    fn rejects_leading_whitespace() {
        assert_eq!(parse_err(with("version ", " version ")), malformed(1));
    }

    #[test]
    fn rejects_control_character_in_value() {
        assert_eq!(parse_err(format!("{}extension bad\0value\n", canonical())), malformed(4));
    }

    // Field failures

    #[test]
    fn rejects_duplicate_required_field() {
        for (field, line) in [
            ("version", format!("version {POINTER_VERSION}")),
            ("oid", format!("oid xet:{HASH}")),
            ("size", "size 12345".to_owned()),
        ] {
            assert_eq!(parse_err(format!("{}{line}\n", canonical())), PointerError::DuplicateField(field));
        }
    }

    // The `oid xet:` line alone is enough to recognize the pointer.
    #[test]
    fn rejects_missing_version() {
        let input = with(&format!("version {POINTER_VERSION}\n"), "");
        assert_eq!(parse_err(input), PointerError::MissingField("version"));
    }

    #[test]
    fn rejects_missing_oid() {
        assert_eq!(parse_err(with(&format!("oid xet:{HASH}\n"), "")), PointerError::MissingField("oid"));
    }

    #[test]
    fn rejects_missing_size() {
        assert_eq!(parse_err(with("size 12345\n", "")), PointerError::MissingField("size"));
    }

    #[test]
    fn rejects_other_xet_version() {
        let version = "https://hf.co/spec/xet/v2";
        assert_eq!(parse_err(with(POINTER_VERSION, version)), PointerError::UnsupportedVersion(version.into()));
    }

    // The marker matches case-insensitively, but the version must match exactly.
    #[test]
    fn version_is_case_sensitive() {
        let version = "https://HF.co/spec/xet/v1";
        assert_eq!(parse_err(with(POINTER_VERSION, version)), PointerError::UnsupportedVersion(version.into()));
    }

    #[test]
    fn rejects_lfs_pointer() {
        let input = b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 1\n";
        assert_eq!(parse_err(input), PointerError::UnsupportedLfs);
    }

    #[test]
    fn oid_requires_xet_scheme() {
        for scheme in ["XET:", "sha256:", ""] {
            assert_eq!(parse_err(with("xet:", scheme)), PointerError::InvalidOid, "{scheme}");
        }
    }

    #[test]
    fn oid_requires_64_hex_digits() {
        for hash in [&HASH[..63], &format!("{HASH}0")] {
            assert_eq!(parse_err(with(HASH, hash)), PointerError::InvalidOid);
        }
    }

    #[test]
    fn oid_accepts_uppercase_hex() {
        let input = with(HASH, &HASH.to_uppercase());
        assert_eq!(XetPointer::parse(input.as_bytes()).unwrap(), Some(expected()));
    }

    #[test]
    fn oid_rejects_non_hex() {
        assert_eq!(parse_err(with(HASH, &"g".repeat(64))), PointerError::InvalidOid);
    }

    #[test]
    fn size_rejects_leading_zero() {
        for value in ["00", "01"] {
            assert_eq!(parse_err(with("12345", value)), PointerError::InvalidSize, "{value}");
        }
    }

    #[test]
    fn size_rejects_sign() {
        for value in ["+1", "-1"] {
            assert_eq!(parse_err(with("12345", value)), PointerError::InvalidSize, "{value}");
        }
    }

    #[test]
    fn size_rejects_non_decimal() {
        for value in ["1.0", "1 ", "１２"] {
            assert_eq!(parse_err(with("12345", value)), PointerError::InvalidSize, "{value}");
        }
    }

    #[test]
    fn size_rejects_overflow() {
        assert_eq!(parse_err(with("12345", "18446744073709551616")), PointerError::InvalidSize);
    }

    // Serialization

    #[test]
    fn display_is_canonical() {
        assert_eq!(expected().to_string(), canonical());
    }

    #[test]
    fn write_to_writes_canonical_bytes() {
        let mut written = Vec::new();
        expected().write_to(&mut written).unwrap();
        assert_eq!(written, canonical().as_bytes());
    }

    #[test]
    fn display_round_trips_through_parse() {
        let pointer = XetPointer::new(MerkleHash::from_hex(HASH).unwrap(), 0);
        assert_eq!(XetPointer::parse(pointer.to_string().as_bytes()).unwrap(), Some(pointer));
    }

    #[test]
    fn write_to_propagates_errors() {
        struct BrokenWriter;
        impl Write for BrokenWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let error = expected().write_to(&mut BrokenWriter).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    // parse_file

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn parse_file_matches_parse() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pointer");
        for contents in [
            canonical(),
            padded_to(MAX_POINTER_SIZE),
            with("12345", "invalid"),
            "hello\n".to_owned(),
        ] {
            std::fs::write(&path, &contents).unwrap();
            match XetPointer::parse(contents.as_bytes()) {
                Ok(parsed) => assert_eq!(XetPointer::parse_file(&path).unwrap(), parsed),
                Err(error) => assert!(matches!(
                    XetPointer::parse_file(&path),
                    Err(CoreError::XetPointer(file_error)) if file_error == error
                )),
            }
        }
    }

    // Exercises the metadata shortcut: a large regular file starting with an invalid pointer is data.
    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn parse_file_treats_large_regular_file_as_ordinary_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("doc");
        std::fs::write(&path, format!("{}{}", with("12345", "invalid"), "x".repeat(MAX_POINTER_SIZE))).unwrap();
        assert_eq!(XetPointer::parse_file(&path).unwrap(), None);
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn parse_file_reports_missing_file() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(
            XetPointer::parse_file(dir.path().join("missing")),
            Err(CoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound
        ));
    }

    // Opening a directory succeeds on Unix; reading it must fail.
    #[cfg(unix)]
    #[test]
    fn parse_file_rejects_directory() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(XetPointer::parse_file(dir.path()), Err(CoreError::Io(_))));
    }

    // A directory's reported size is unrelated to content, so a large one must not take the
    // regular-file shortcut and pass as ordinary data.
    #[cfg(unix)]
    #[test]
    fn parse_file_rejects_large_directory() {
        let dir = TempDir::new().unwrap();
        for i in 0..2000 {
            if std::fs::metadata(dir.path()).unwrap().len() > MAX_POINTER_SIZE as u64 {
                break;
            }
            std::fs::write(dir.path().join(format!("entry-with-a-long-name-{i:04}")), b"").unwrap();
        }
        assert!(std::fs::metadata(dir.path()).unwrap().len() > MAX_POINTER_SIZE as u64);
        assert!(matches!(XetPointer::parse_file(dir.path()), Err(CoreError::Io(_))));
    }
}
