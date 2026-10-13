//! Chooses which files are stored with Xet. The rule must stay identical to the Hub's server-side
//! selection, so a file is stored the same way whether it arrives through git-hf or another client:
//!
//! ```text
//! store_as_xet = size > TEXT_LIMIT || (is_binary(first 512 bytes) && size > BINARY_LIMIT)
//! ```
//!
//! Both comparisons are strict. Change these values or [`is_binary`] only together with the Hub.

/// Files larger than this are stored with Xet regardless of content (10 MiB).
pub(crate) const TEXT_LIMIT: usize = 10_485_760;
/// Binary files larger than this are stored with Xet.
pub(crate) const BINARY_LIMIT: usize = 100_000;
/// The largest file the Hub accepts with Xet storage (decimal 500 GB); equality is allowed.
pub(crate) const FILE_LIMIT: u64 = 500_000_000_000;

/// Whether `data` looks binary, judging only its first 512 bytes.
///
/// This is a rule-for-rule port of the Hub's server-side check, which derives from the MIT-licensed
/// `isBinaryFile` package (<https://github.com/gjtorikian/isBinaryFile>). Keep it identical, including
/// rule order and boundary handling, so git-hf and the Hub agree on every input:
///
/// 1. An empty sample is text.
/// 2. A UTF-8, UTF-32 (BE or LE), or GB18030 byte order mark means text.
/// 3. A `%PDF-` header means binary: PDFs begin with ASCII but are binary.
/// 4. A UTF-16 byte order mark means text. This follows the UTF-32 LE check, whose BOM begins with the UTF-16 LE BOM,
///    and the PDF check.
/// 5. Any NUL byte means binary. Without the BOM checks above, UTF-16 and UTF-32 text would fail here.
/// 6. Otherwise, bytes outside control bytes 7..=14 (bell to shift-out, including tab, newline, and carriage return)
///    and printable ASCII (32..=127) are suspicious unless they start a complete UTF-8 sequence inside the sample. More
///    than 10% suspicious bytes means binary.
pub(crate) fn is_binary(data: &[u8]) -> bool {
    // Rule 1 needs no code: an empty sample has no suspicious bytes and returns false at the end.
    let data = &data[..data.len().min(512)];
    // Rule 2.
    for bom in [
        &b"\xef\xbb\xbf"[..],
        &b"\x00\x00\xfe\xff"[..],
        &b"\xff\xfe\x00\x00"[..],
        &b"\x84\x31\x95\x33"[..],
    ] {
        if data.starts_with(bom) {
            return false;
        }
    }
    // Rule 3.
    if data.starts_with(b"%PDF-") {
        return true;
    }
    // Rule 4.
    if data.starts_with(b"\xfe\xff") || data.starts_with(b"\xff\xfe") {
        return false;
    }
    // Rules 5 and 6.
    let mut suspicious = 0;
    let mut i = 0;
    while i < data.len() {
        let b = data[i];
        if b == 0 {
            return true;
        }
        if !(7..=14).contains(&b) && !(32..=127).contains(&b) {
            // Continuation bytes expected after a UTF-8 lead byte.
            let n = match b {
                194..=223 => 1,
                224..=239 => 2,
                240..=247 => 3,
                _ => 0,
            };
            // The whole sequence must fit in the sample. As on the Hub, the index advances past the lead
            // byte before checking, so a broken sequence also skips the byte that follows it.
            if n > 0 && i + n < data.len() {
                i += 1;
                if data[i..i + n].iter().all(|b| (128..192).contains(b)) {
                    i += n;
                    continue;
                }
            }
            suspicious += 1;
            // Decide early once past byte 32. `x * 100 > len * 10` is the Hub's `x * 100 / len > 10`
            // without division.
            if i > 32 && suspicious * 100 > data.len() * 10 {
                return true;
            }
        }
        i += 1;
    }
    suspicious * 100 > data.len() * 10
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_samples() {
        for text in [
            b"".as_slice(),
            b"hello\n",
            "hello 😀世界".as_bytes(),
            b"\xff\xfe\0\0",
            b"\xef\xbb\xbfhello",
            b"\xfe\xff\0h",
            b"\xff\xfeh\0",
            b"\0\0\xfe\xff",
            b"\x84\x31\x95\x33",
            "Café".as_bytes(),
            "This is a test 👋".as_bytes(),
        ] {
            assert!(!is_binary(text));
        }
        for binary in [b"\0".as_slice(), b"%PDF-1.4", &[255; 512]] {
            assert!(is_binary(binary));
        }
        let mut data = vec![b'a'; 513];
        data[512] = 0;
        assert!(!is_binary(&data));
    }
}
