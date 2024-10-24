/// Implements the small file heuristic to determine if this file
/// should be stored in Git or MerkleDB.
/// The current heuristic is simple:
///  - if it is less than the SMALL_FILE_THRESHOLD AND if it decodes as utf-8, it is a small file
pub fn is_file_passthrough(buf: &[u8], small_file_threshold: usize) -> bool {
    buf.len() < small_file_threshold && std::str::from_utf8(buf).is_ok()
}

pub fn is_possible_start_to_text_file(buf: &[u8]) -> bool {
    // In UTF-8 encoding, the maximum length of a character is 4 bytes. This means
    // that a valid UTF-8 character can span up to 4 bytes. Therefore, when you have
    // a sequence of bytes that could be part of a valid UTF-8 string but is truncated,
    // the maximum difference between the end of the buffer and the position indicated
    // by e.valid_up_to() from a Utf8Error is 3 bytes.
    match std::str::from_utf8(buf) {
        Ok(_) => true,
        Err(e) => e.valid_up_to() != 0 && e.valid_up_to() >= buf.len().saturating_sub(3),
    }
}
