use bytes::Bytes;

use crate::cas_client::error::{CasClientError, Result};
use crate::cas_types::HttpRange;

/// A single part from a multipart/byteranges HTTP response.
pub struct MultipartPart {
    pub range: HttpRange,
    pub data: Bytes,
}

/// Parse a `multipart/byteranges` HTTP response body (RFC 7233 §4.1).
///
/// Extracts the boundary from `content_type`, splits the body by boundary markers,
/// parses `Content-Range` headers from each part, and returns parts sorted by byte range start.
pub fn parse_multipart_byteranges(content_type: &str, body: Bytes) -> Result<Vec<MultipartPart>> {
    let boundary = extract_boundary(content_type)?;

    let delimiter = format!("\r\n--{boundary}");
    let body_slice = body.as_ref();

    let mut parts = Vec::new();

    let first_delim = format!("--{boundary}");
    let Some(start) = find_subsequence(body_slice, first_delim.as_bytes()) else {
        return Err(CasClientError::Other("No boundary found in multipart body".to_string()));
    };

    let mut remaining = &body_slice[start + first_delim.len()..];

    loop {
        if remaining.starts_with(b"\r\n") {
            remaining = &remaining[2..];
        } else if remaining.starts_with(b"--") {
            break;
        } else {
            break;
        }

        let next_boundary = find_subsequence(remaining, delimiter.as_bytes());
        let part_data = match next_boundary {
            Some(pos) => &remaining[..pos],
            None => remaining,
        };

        if let Some(header_end) = find_subsequence(part_data, b"\r\n\r\n") {
            let headers = &part_data[..header_end];
            let data_start = header_end + 4;
            let data = &part_data[data_start..];

            let range = parse_content_range(headers)?;
            let offset = body.len() - body_slice.len()
                + (remaining.as_ptr() as usize - body_slice.as_ptr() as usize)
                + data_start;
            parts.push(MultipartPart {
                range,
                data: body.slice(offset..offset + data.len()),
            });
        }

        match next_boundary {
            Some(pos) => {
                remaining = &remaining[pos + delimiter.len()..];
            },
            None => break,
        }
    }

    parts.sort_by_key(|p| p.range.start);

    Ok(parts)
}

fn extract_boundary(content_type: &str) -> Result<String> {
    for part in content_type.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("boundary=") {
            let boundary = value.trim_matches('"');
            return Ok(boundary.to_string());
        }
    }
    Err(CasClientError::Other(format!("No boundary found in Content-Type: {content_type}")))
}

fn parse_content_range(headers: &[u8]) -> Result<HttpRange> {
    let headers_str = std::str::from_utf8(headers)
        .map_err(|e| CasClientError::Other(format!("Invalid UTF-8 in part headers: {e}")))?;

    for line in headers_str.split("\r\n") {
        let line_lower = line.to_ascii_lowercase();
        if let Some(value) = line_lower.strip_prefix("content-range:") {
            let value = value.trim();
            if value.starts_with("bytes ") {
                let original_value = line[line.len() - value.len() + "bytes ".len()..].trim();
                let slash_pos = original_value
                    .find('/')
                    .ok_or_else(|| CasClientError::Other(format!("Invalid Content-Range: {line}")))?;
                let range_part = &original_value[..slash_pos];
                let dash_pos = range_part
                    .find('-')
                    .ok_or_else(|| CasClientError::Other(format!("Invalid Content-Range: {line}")))?;
                let start: u64 = range_part[..dash_pos]
                    .parse()
                    .map_err(|e| CasClientError::Other(format!("Invalid Content-Range start: {e}")))?;
                let end: u64 = range_part[dash_pos + 1..]
                    .parse()
                    .map_err(|e| CasClientError::Other(format!("Invalid Content-Range end: {e}")))?;
                return Ok(HttpRange::new(start, end));
            }
        }
    }

    Err(CasClientError::Other("No Content-Range header found in multipart part".to_string()))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_boundary() {
        assert_eq!(extract_boundary("multipart/byteranges; boundary=something").unwrap(), "something");
        assert_eq!(extract_boundary("multipart/byteranges; boundary=\"quoted\"").unwrap(), "quoted");
    }

    #[test]
    fn test_extract_boundary_missing() {
        assert!(extract_boundary("text/plain").is_err());
    }

    #[test]
    fn test_parse_single_part() {
        let boundary = "abc123";
        let body = format!(
            "--{boundary}\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 0-99/1000\r\n\r\nHello World\r\n--{boundary}--\r\n"
        );
        let content_type = format!("multipart/byteranges; boundary={boundary}");

        let parts = parse_multipart_byteranges(&content_type, Bytes::from(body)).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].range.start, 0);
        assert_eq!(parts[0].range.end, 99);
        assert_eq!(&parts[0].data[..], b"Hello World");
    }

    #[test]
    fn test_parse_multiple_parts() {
        let boundary = "sep";
        let body = format!(
            "--{boundary}\r\nContent-Range: bytes 100-199/1000\r\n\r\nPart2Data\r\n--{boundary}\r\nContent-Range: bytes 0-49/1000\r\n\r\nPart1Data\r\n--{boundary}--\r\n"
        );
        let content_type = format!("multipart/byteranges; boundary={boundary}");

        let parts = parse_multipart_byteranges(&content_type, Bytes::from(body)).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].range.start, 0);
        assert_eq!(parts[0].range.end, 49);
        assert_eq!(&parts[0].data[..], b"Part1Data");
        assert_eq!(parts[1].range.start, 100);
        assert_eq!(parts[1].range.end, 199);
        assert_eq!(&parts[1].data[..], b"Part2Data");
    }

    #[test]
    fn test_parse_empty_body_no_boundary() {
        let content_type = "multipart/byteranges; boundary=xyz";
        let result = parse_multipart_byteranges(content_type, Bytes::new());
        assert!(result.is_err());
    }
}
