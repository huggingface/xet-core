//! File selection uses the same byte sample and thresholds as the Hub.
pub(crate) const TEXT_LIMIT: usize = 10_485_760;
pub(crate) const BINARY_LIMIT: usize = 100_000;
pub(crate) const FILE_LIMIT: u64 = 500_000_000_000;

pub(crate) fn is_binary(data: &[u8]) -> bool {
    let data = &data[..data.len().min(512)];
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
    if data.starts_with(b"%PDF-") {
        return true;
    }
    if data.starts_with(b"\xfe\xff") || data.starts_with(b"\xff\xfe") {
        return false;
    }
    let mut suspicious = 0;
    let mut i = 0;
    while i < data.len() {
        let b = data[i];
        if b == 0 {
            return true;
        }
        if !(7..=14).contains(&b) && !(32..=127).contains(&b) {
            let n = match b {
                194..=223 => 1,
                224..=239 => 2,
                240..=247 => 3,
                _ => 0,
            };
            if n > 0 && i + n < data.len() {
                i += 1;
                if data[i..i + n].iter().all(|b| (128..192).contains(b)) {
                    i += n;
                    continue;
                }
            }
            suspicious += 1;
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
