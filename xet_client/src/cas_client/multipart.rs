use bytes::{Buf, Bytes, BytesMut};

use crate::cas_types::HttpRange;
use crate::error::{ClientError, Result};

/// A single part from a multipart/byteranges HTTP response.
pub struct MultipartPart {
    pub range: HttpRange,
    pub data: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParserState {
    Preamble,
    AfterBoundary,
    Part,
}

/// Incrementally parses a `multipart/byteranges` HTTP response body (RFC 7233 §4.1).
///
/// Completed parts are returned as soon as their terminating boundary is received, so the parser
/// does not explicitly aggregate the entire response. Its logical payload buffer is limited to
/// the incomplete current part, aside from the incoming transport chunk and allocator capacity.
pub(crate) struct StreamingMultipartParser {
    delimiter: Vec<u8>,
    buffer: BytesMut,
    search_from: usize,
    state: ParserState,
    done: bool,
}

impl StreamingMultipartParser {
    pub(crate) fn new(content_type: &str) -> Result<Self> {
        let boundary = extract_boundary(content_type)?;

        // Include the preceding CRLF so it is not left attached to the part body. Seed the
        // buffer with a virtual CRLF so a body may also start directly with `--boundary`.
        let delimiter = format!("\r\n--{boundary}").into_bytes();
        let mut buffer = BytesMut::with_capacity(delimiter.len());
        buffer.extend_from_slice(b"\r\n");

        Ok(Self {
            delimiter,
            buffer,
            search_from: 0,
            state: ParserState::Preamble,
            done: false,
        })
    }

    /// Pushes another response-body chunk and returns every part completed by that chunk.
    pub(crate) fn push(&mut self, chunk: Bytes) -> Result<Vec<MultipartPart>> {
        if self.done {
            return Ok(Vec::new());
        }

        self.buffer.extend_from_slice(&chunk);
        self.parse_available()
    }

    /// Finishes parsing after the response stream reaches EOF.
    ///
    /// For compatibility with the previous whole-body parser, a final part without a closing
    /// boundary is accepted if it otherwise has valid headers.
    pub(crate) fn finish(&mut self) -> Result<Vec<MultipartPart>> {
        let mut parts = self.parse_available()?;
        if self.done {
            return Ok(parts);
        }

        match self.state {
            ParserState::Preamble => {
                return Err(ClientError::Other("No boundary found in multipart body".to_string()));
            },
            ParserState::AfterBoundary => {
                // The previous whole-body parser stopped successfully after an exact boundary,
                // even when neither CRLF nor the closing `--` followed it. Preserve that EOF
                // behavior while moving to incremental parsing.
                self.buffer.clear();
                self.done = true;
            },
            ParserState::Part => {
                let part = self.buffer.split().freeze();
                parts.push(parse_part(part)?);
                self.done = true;
            },
        }

        Ok(parts)
    }

    fn parse_available(&mut self) -> Result<Vec<MultipartPart>> {
        let mut parts = Vec::new();

        loop {
            match self.state {
                ParserState::Preamble | ParserState::Part => {
                    let delimiter_position = find_subsequence(&self.buffer[self.search_from..], &self.delimiter)
                        .map(|position| self.search_from + position);

                    let Some(delimiter_position) = delimiter_position else {
                        if self.state == ParserState::Preamble {
                            // Discard confirmed preamble while preserving a possible partial delimiter.
                            let keep = self.delimiter.len().saturating_sub(1);
                            let discard = self.buffer.len().saturating_sub(keep);
                            self.buffer.advance(discard);
                            self.search_from = 0;
                        } else {
                            // Only the new suffix can begin a delimiter on the next push.
                            self.search_from = self.buffer.len().saturating_sub(self.delimiter.len() - 1);
                        }
                        break;
                    };

                    if self.state == ParserState::Part {
                        let part = self.buffer.split_to(delimiter_position).freeze();
                        parts.push(parse_part(part)?);
                    } else {
                        self.buffer.advance(delimiter_position);
                    }

                    self.buffer.advance(self.delimiter.len());
                    self.search_from = 0;
                    self.state = ParserState::AfterBoundary;
                },
                ParserState::AfterBoundary => {
                    if self.buffer.len() < 2 {
                        break;
                    }

                    if self.buffer.starts_with(b"--") {
                        self.buffer.advance(2);
                        self.buffer.clear();
                        self.done = true;
                        break;
                    }

                    if self.buffer.starts_with(b"\r\n") {
                        self.buffer.advance(2);
                        self.search_from = 0;
                        self.state = ParserState::Part;
                        continue;
                    }

                    return Err(ClientError::Other(
                        "Malformed multipart body: expected CRLF or closing delimiter after boundary".to_string(),
                    ));
                },
            }
        }

        Ok(parts)
    }
}

/// Parse a complete `multipart/byteranges` HTTP response body (RFC 7233 §4.1).
///
/// Extracts the boundary from `content_type`, splits the body by boundary markers,
/// parses `Content-Range` headers from each part, and returns zero-copy parts sorted by byte
/// range start. The HTTP client uses its crate-private streaming parser instead.
pub fn parse_multipart_byteranges(content_type: &str, body: Bytes) -> Result<Vec<MultipartPart>> {
    let boundary = extract_boundary(content_type)?;

    let delimiter = format!("\r\n--{boundary}");
    let body_slice = body.as_ref();

    let mut parts = Vec::new();

    let first_delimiter = format!("--{boundary}");
    let Some(start) = find_subsequence(body_slice, first_delimiter.as_bytes()) else {
        return Err(ClientError::Other("No boundary found in multipart body".to_string()));
    };

    let mut remaining = &body_slice[start + first_delimiter.len()..];

    loop {
        if remaining.starts_with(b"\r\n") {
            remaining = &remaining[2..];
        } else {
            break;
        }

        let next_boundary = find_subsequence(remaining, delimiter.as_bytes());
        let part_data = match next_boundary {
            Some(position) => &remaining[..position],
            None => remaining,
        };

        let Some(header_end) = find_subsequence(part_data, b"\r\n\r\n") else {
            return Err(ClientError::Other("Malformed multipart part: missing header/data separator".to_string()));
        };

        let headers = &part_data[..header_end];
        let data_start = header_end + 4;
        let data = &part_data[data_start..];

        let range = parse_content_range(headers)?;
        // Compute the absolute byte offset into the original `body` so we can use Bytes::slice
        // for zero-copy extraction of this part's data.
        let offset =
            body.len() - body_slice.len() + (remaining.as_ptr() as usize - body_slice.as_ptr() as usize) + data_start;
        parts.push(MultipartPart {
            range,
            data: body.slice(offset..offset + data.len()),
        });

        match next_boundary {
            Some(position) => {
                remaining = &remaining[position + delimiter.len()..];
            },
            None => break,
        }
    }

    parts.sort_by_key(|part| part.range.start);
    Ok(parts)
}

fn parse_part(part: Bytes) -> Result<MultipartPart> {
    let Some(header_end) = find_subsequence(&part, b"\r\n\r\n") else {
        return Err(ClientError::Other("Malformed multipart part: missing header/data separator".to_string()));
    };

    let range = parse_content_range(&part[..header_end])?;
    let data_start = header_end + 4;

    Ok(MultipartPart {
        range,
        data: part.slice(data_start..),
    })
}

fn extract_boundary(content_type: &str) -> Result<String> {
    for part in content_type.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("boundary=") {
            let boundary = value.trim_matches('"');
            return Ok(boundary.to_string());
        }
    }
    Err(ClientError::Other(format!("No boundary found in Content-Type: {content_type}")))
}

fn parse_content_range(headers: &[u8]) -> Result<HttpRange> {
    let headers_str =
        std::str::from_utf8(headers).map_err(|e| ClientError::Other(format!("Invalid UTF-8 in part headers: {e}")))?;

    for line in headers_str.split("\r\n") {
        let line_lower = line.to_ascii_lowercase();
        if let Some(value) = line_lower.strip_prefix("content-range:") {
            // Digits, dashes, and slashes are case-invariant, so we can parse
            // directly from the lowercased value.
            if let Some(range_spec) = value.trim().strip_prefix("bytes ") {
                let original_value = range_spec.trim();
                let slash_pos = original_value
                    .find('/')
                    .ok_or_else(|| ClientError::Other(format!("Invalid Content-Range: {line}")))?;
                let range_part = &original_value[..slash_pos];
                let dash_pos = range_part
                    .find('-')
                    .ok_or_else(|| ClientError::Other(format!("Invalid Content-Range: {line}")))?;
                let start: u64 = range_part[..dash_pos]
                    .parse()
                    .map_err(|e| ClientError::Other(format!("Invalid Content-Range start: {e}")))?;
                let end: u64 = range_part[dash_pos + 1..]
                    .parse()
                    .map_err(|e| ClientError::Other(format!("Invalid Content-Range end: {e}")))?;
                // RFC 7233 Content-Range uses an inclusive end, which matches HttpRange.
                return Ok(HttpRange::new(start, end));
            }
        }
    }

    Err(ClientError::Other("No Content-Range header found in multipart part".to_string()))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn multipart_body(boundary: &str, parts: &[(HttpRange, &[u8])]) -> Bytes {
        let mut body = Vec::new();
        for (range, data) in parts {
            body.extend_from_slice(
                format!(
                    "--{boundary}\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes {}-{}/1000\r\n\r\n",
                    range.start, range.end
                )
                .as_bytes(),
            );
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        Bytes::from(body)
    }

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
        let body = multipart_body(boundary, &[(HttpRange::new(0, 99), b"Hello World")]);
        let body_start = body.as_ptr() as usize;
        let body_end = body_start + body.len();
        let content_type = format!("multipart/byteranges; boundary={boundary}");

        let parts = parse_multipart_byteranges(&content_type, body).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].range.start, 0);
        assert_eq!(parts[0].range.end, 99);
        assert_eq!(&parts[0].data[..], b"Hello World");
        let data_start = parts[0].data.as_ptr() as usize;
        assert!((body_start..body_end).contains(&data_start), "part data must slice the input Bytes");
    }

    #[test]
    fn test_parse_multiple_parts_sorts_by_range() {
        let boundary = "sep";
        let body = multipart_body(
            boundary,
            &[
                (HttpRange::new(100, 199), b"Part2Data"),
                (HttpRange::new(0, 49), b"Part1Data"),
            ],
        );
        let content_type = format!("multipart/byteranges; boundary={boundary}");

        let parts = parse_multipart_byteranges(&content_type, body).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].range.start, 0);
        assert_eq!(&parts[0].data[..], b"Part1Data");
        assert_eq!(parts[1].range.start, 100);
        assert_eq!(&parts[1].data[..], b"Part2Data");
    }

    #[test]
    fn test_streaming_emits_part_before_end_of_body() {
        let boundary = "streaming";
        let body = multipart_body(
            boundary,
            &[
                (HttpRange::new(0, 9), b"first-part"),
                (HttpRange::new(100, 110), b"second-part"),
            ],
        );
        let split = find_subsequence(&body, b"second-part").unwrap();
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let mut parser = StreamingMultipartParser::new(&content_type).unwrap();

        let first = parser.push(body.slice(..split)).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(&first[0].data[..], b"first-part");
        assert!(!parser.done);

        let mut rest = parser.push(body.slice(split..)).unwrap();
        rest.extend(parser.finish().unwrap());
        assert_eq!(rest.len(), 1);
        assert_eq!(&rest[0].data[..], b"second-part");
    }

    #[test]
    fn test_streaming_handles_every_two_chunk_split() {
        let boundary = "split-boundary";
        let body = multipart_body(boundary, &[(HttpRange::new(0, 4), b"hello"), (HttpRange::new(10, 15), b"world!")]);
        let content_type = format!("multipart/byteranges; boundary={boundary}");

        for split in 0..=body.len() {
            let mut parser = StreamingMultipartParser::new(&content_type).unwrap();
            let mut parts = parser.push(body.slice(..split)).unwrap();
            parts.extend(parser.push(body.slice(split..)).unwrap());
            parts.extend(parser.finish().unwrap());

            assert_eq!(parts.len(), 2, "split at {split}");
            assert_eq!(&parts[0].data[..], b"hello", "split at {split}");
            assert_eq!(&parts[1].data[..], b"world!", "split at {split}");
        }
    }

    #[test]
    fn test_streaming_one_byte_chunks_release_completed_parts() {
        let boundary = "tiny-chunks";
        let large_part = vec![0x5a; 4096];
        let parts = (0..32)
            .map(|index| {
                let start = index * 5000;
                (HttpRange::new(start, start + 4095), large_part.as_slice())
            })
            .collect::<Vec<_>>();
        let body = multipart_body(boundary, &parts);
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let mut parser = StreamingMultipartParser::new(&content_type).unwrap();
        let mut emitted = 0;
        let mut max_retained = 0;
        let mut max_retained_capacity = 0;

        for byte in body.iter().copied() {
            emitted += parser.push(Bytes::from(vec![byte])).unwrap().len();
            max_retained = max_retained.max(parser.buffer.len());
            max_retained_capacity = max_retained_capacity.max(parser.buffer.capacity());
        }
        emitted += parser.finish().unwrap().len();

        assert_eq!(emitted, parts.len());
        assert!(
            max_retained <= large_part.len() + 256,
            "retained {max_retained} bytes while the largest part is {} bytes",
            large_part.len()
        );
        assert!(
            max_retained_capacity < body.len() / 4,
            "retained capacity {max_retained_capacity} for a {}-byte multipart body",
            body.len()
        );
    }

    #[test]
    fn test_streaming_handles_binary_delimiter_prefixes() {
        let boundary = "binary";
        let data = b"\x00\r\n--binar\xff\r\n---\x00\xff";
        let body = multipart_body(boundary, &[(HttpRange::new(0, data.len() as u64 - 1), data)]);
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let mut parser = StreamingMultipartParser::new(&content_type).unwrap();
        let mut parts = Vec::new();

        for byte in body.iter().copied() {
            parts.extend(parser.push(Bytes::from(vec![byte])).unwrap());
        }
        parts.extend(parser.finish().unwrap());

        assert_eq!(parts.len(), 1);
        assert_eq!(&parts[0].data[..], data);
    }

    #[test]
    fn test_parse_empty_body_no_boundary() {
        let content_type = "multipart/byteranges; boundary=xyz";
        let result = parse_multipart_byteranges(content_type, Bytes::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_part_missing_header_separator() {
        let boundary = "xyz";
        let body = format!("--{boundary}\r\nContent-Range: bytes 0-9/100\r\nMISSING_SEPARATOR\r\n--{boundary}--\r\n");
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let result = parse_multipart_byteranges(&content_type, Bytes::from(body));
        assert!(result.is_err());
    }

    #[test]
    fn test_streaming_finish_accepts_eof_immediately_after_boundary() {
        let boundary = "legacy-eof";
        let body = Bytes::from(format!("--{boundary}\r\nContent-Range: bytes 0-4/5\r\n\r\nhello\r\n--{boundary}"));
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let mut parser = StreamingMultipartParser::new(&content_type).unwrap();

        let mut parts = parser.push(body).unwrap();
        parts.extend(parser.finish().unwrap());
        assert_eq!(parts.len(), 1);
        assert_eq!(&parts[0].data[..], b"hello");
    }
}
