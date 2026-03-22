//! Shared utilities for reconstruction range computation and V2 URL encoding.
//!
//! This module consolidates logic used by both `MemoryClient` and `LocalClient`
//! for computing reconstruction ranges from file segment info, merging adjacent
//! ranges, and encoding/decoding V2 fetch URLs.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use more_asserts::{assert_ge, assert_gt, debug_assert_lt};
use tokio::time::{Duration, Instant};
use xet_core_structures::MerkleHashMap;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_core_structures::xorb_object::XorbObject;

use crate::cas_types::{ChunkRange, FileRange, HttpRange, XorbRangeDescriptor, XorbReconstructionTerm};
use crate::error::{ClientError, Result};

/// Reference instant for URL timestamps. Initialized far in the past to allow
/// testing timestamps that are earlier in the current process lifetime.
pub(crate) static REFERENCE_INSTANT: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(|| {
    let now = Instant::now();
    now.checked_sub(Duration::from_secs(365 * 24 * 60 * 60))
        .unwrap_or(now)
});

/// A merged byte/chunk range for a single xorb.
#[derive(Clone, Debug)]
pub(crate) struct MergedRange {
    pub chunk_range: ChunkRange,
    pub byte_range: FileRange,
}

/// Result of `compute_reconstruction_ranges`: the offset into the first range,
/// the list of reconstruction terms, and the merged ranges per xorb hash.
pub(crate) type ReconstructionRangesResult =
    Option<(u64, Vec<XorbReconstructionTerm>, MerkleHashMap<Vec<MergedRange>>)>;

/// Computes reconstruction ranges from file segment info.
///
/// Iterates the segments in `file_info`, prunes chunk boundaries to the
/// requested `bytes_range`, and merges adjacent/overlapping ranges per xorb.
///
/// `get_xorb_footer` is called for each unique xorb hash encountered to obtain
/// the `XorbObject` metadata needed for chunk-level byte offset calculations.
///
/// Returns `Ok(None)` when the range is out of bounds, or
/// `Ok(Some((offset_into_first_range, terms, merged_ranges_per_xorb)))`.
pub(crate) fn compute_reconstruction_ranges(
    file_info: &MDBFileInfo,
    bytes_range: Option<FileRange>,
    get_xorb_footer: &mut dyn FnMut(&MerkleHash) -> Result<XorbObject>,
) -> Result<ReconstructionRangesResult> {
    let total_file_size: u64 = file_info.file_size();

    let file_range = if let Some(range) = bytes_range {
        if range.start >= total_file_size {
            if total_file_size == 0 && range.start == 0 {
                return Ok(Some((0, vec![], MerkleHashMap::new())));
            }
            return Ok(None);
        }
        FileRange::new(range.start, range.end.min(total_file_size))
    } else {
        if total_file_size == 0 {
            return Ok(Some((0, vec![], MerkleHashMap::new())));
        }
        FileRange::full()
    };

    let mut s_idx = 0;
    let mut cumulative_bytes = 0u64;
    let mut first_chunk_byte_start;

    loop {
        if s_idx >= file_info.segments.len() {
            return Err(ClientError::InvalidRange);
        }

        let n = file_info.segments[s_idx].unpacked_segment_bytes as u64;
        if cumulative_bytes + n > file_range.start {
            assert_ge!(file_range.start, cumulative_bytes);
            first_chunk_byte_start = cumulative_bytes;
            break;
        } else {
            cumulative_bytes += n;
            s_idx += 1;
        }
    }

    let mut terms = Vec::new();

    #[derive(Clone)]
    struct FetchInfoIntermediate {
        chunk_range: ChunkRange,
        byte_range: FileRange,
    }

    let mut fetch_info_map: MerkleHashMap<Vec<FetchInfoIntermediate>> = MerkleHashMap::new();

    while s_idx < file_info.segments.len() && cumulative_bytes < file_range.end {
        let mut segment = file_info.segments[s_idx].clone();
        let mut chunk_range = ChunkRange::new(segment.chunk_index_start, segment.chunk_index_end);

        let xorb_footer = get_xorb_footer(&segment.xorb_hash)?;

        if cumulative_bytes < file_range.start {
            while chunk_range.start < chunk_range.end {
                let next_chunk_size = xorb_footer.uncompressed_chunk_length(chunk_range.start)? as u64;
                if cumulative_bytes + next_chunk_size <= file_range.start {
                    cumulative_bytes += next_chunk_size;
                    first_chunk_byte_start += next_chunk_size;
                    segment.unpacked_segment_bytes -= next_chunk_size as u32;
                    chunk_range.start += 1;
                    debug_assert_lt!(chunk_range.start, chunk_range.end);
                } else {
                    break;
                }
            }
        }

        if cumulative_bytes + segment.unpacked_segment_bytes as u64 > file_range.end {
            while chunk_range.end > chunk_range.start {
                let last_chunk_size = xorb_footer.uncompressed_chunk_length(chunk_range.end - 1)?;
                if cumulative_bytes + (segment.unpacked_segment_bytes - last_chunk_size) as u64 >= file_range.end {
                    chunk_range.end -= 1;
                    segment.unpacked_segment_bytes -= last_chunk_size;
                    debug_assert_lt!(chunk_range.start, chunk_range.end);
                    assert_gt!(segment.unpacked_segment_bytes, 0);
                } else {
                    break;
                }
            }
        }

        let (byte_start, byte_end) = xorb_footer.get_byte_offset(chunk_range.start, chunk_range.end)?;
        let byte_range = FileRange::new(byte_start as u64, byte_end as u64);

        terms.push(XorbReconstructionTerm {
            hash: segment.xorb_hash.into(),
            unpacked_length: segment.unpacked_segment_bytes,
            range: chunk_range,
        });

        fetch_info_map
            .entry(segment.xorb_hash)
            .or_default()
            .push(FetchInfoIntermediate {
                chunk_range,
                byte_range,
            });

        cumulative_bytes += segment.unpacked_segment_bytes as u64;
        s_idx += 1;
    }

    debug_assert!(!terms.is_empty());

    let mut merged: MerkleHashMap<Vec<MergedRange>> = MerkleHashMap::new();
    for (hash, mut fi_vec) in fetch_info_map {
        fi_vec.sort_by_key(|fi| fi.chunk_range.start);

        let mut result: Vec<MergedRange> = Vec::new();
        let mut idx = 0;

        while idx < fi_vec.len() {
            let mut cur = fi_vec[idx].clone();

            while idx + 1 < fi_vec.len() {
                let next = &fi_vec[idx + 1];
                if next.chunk_range.start <= cur.chunk_range.end {
                    cur.chunk_range.end = cur.chunk_range.end.max(next.chunk_range.end);
                    cur.byte_range.end = cur.byte_range.end.max(next.byte_range.end);
                    idx += 1;
                } else {
                    break;
                }
            }

            result.push(MergedRange {
                chunk_range: cur.chunk_range,
                byte_range: cur.byte_range,
            });
            idx += 1;
        }

        merged.insert(hash, result);
    }

    Ok(Some((file_range.start - first_chunk_byte_start, terms, merged)))
}

/// Generates a V2 fetch URL: base64("{hash_hex}:{timestamp_ms}:{r1_start}-{r1_end},...")
pub(crate) fn generate_v2_fetch_url(hash: &MerkleHash, ranges: &[XorbRangeDescriptor], timestamp: Instant) -> String {
    let timestamp_ms = timestamp.saturating_duration_since(*REFERENCE_INSTANT).as_millis() as u64;
    let ranges_str: Vec<String> = ranges.iter().map(|r| format!("{}-{}", r.bytes.start, r.bytes.end)).collect();
    let payload = format!("{}:{}:{}", hash.hex(), timestamp_ms, ranges_str.join(","));
    URL_SAFE_NO_PAD.encode(payload.as_bytes())
}

/// Parses a V2 fetch URL back into (hash, timestamp, byte ranges).
pub(crate) fn parse_v2_fetch_url(url: &str) -> Result<(MerkleHash, Instant, Vec<HttpRange>)> {
    let bytes = URL_SAFE_NO_PAD.decode(url).map_err(|_| ClientError::InvalidArguments)?;
    let payload = String::from_utf8(bytes).map_err(|_| ClientError::InvalidArguments)?;

    let mut parts = payload.splitn(3, ':');
    let hash_hex = parts.next().ok_or(ClientError::InvalidArguments)?;
    let ts_str = parts.next().ok_or(ClientError::InvalidArguments)?;
    let ranges_str = parts.next().ok_or(ClientError::InvalidArguments)?;

    let hash = MerkleHash::from_hex(hash_hex).map_err(|_| ClientError::InvalidArguments)?;
    let timestamp_ms: u64 = ts_str.parse().map_err(|_| ClientError::InvalidArguments)?;
    let timestamp = *REFERENCE_INSTANT + Duration::from_millis(timestamp_ms);

    let mut ranges = Vec::new();
    for r in ranges_str.split(',').filter(|s| !s.is_empty()) {
        let mut parts = r.splitn(2, '-');
        let start: u64 = parts
            .next()
            .ok_or(ClientError::InvalidArguments)?
            .parse()
            .map_err(|_| ClientError::InvalidArguments)?;
        let end: u64 = parts
            .next()
            .ok_or(ClientError::InvalidArguments)?
            .parse()
            .map_err(|_| ClientError::InvalidArguments)?;
        ranges.push(HttpRange::new(start, end));
    }

    Ok((hash, timestamp, ranges))
}

#[cfg(test)]
mod tests {
    use xet_core_structures::metadata_shard::file_structs::{
        FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo,
    };

    use super::super::random_xorb::RandomXorb;
    use super::*;

    fn make_range_descriptor(chunk_start: u32, chunk_end: u32, byte_start: u64, byte_end: u64) -> XorbRangeDescriptor {
        XorbRangeDescriptor {
            chunks: ChunkRange::new(chunk_start, chunk_end),
            bytes: HttpRange::new(byte_start, byte_end),
        }
    }

    fn build_xorb(chunk_sizes: &[usize]) -> (MerkleHash, XorbObject) {
        let seed_and_sizes: Vec<(u64, u32)> =
            chunk_sizes.iter().enumerate().map(|(i, &s)| (i as u64, s as u32)).collect();
        let xorb = RandomXorb::new(&seed_and_sizes);
        let xorb_object = xorb.get_xorb_object();
        let hash = xorb.xorb_hash();
        (hash, xorb_object)
    }

    fn make_segment(
        xorb_hash: MerkleHash,
        chunk_start: u32,
        chunk_end: u32,
        unpacked_bytes: u32,
    ) -> FileDataSequenceEntry {
        FileDataSequenceEntry {
            xorb_hash,
            xorb_flags: 0,
            chunk_index_start: chunk_start,
            chunk_index_end: chunk_end,
            unpacked_segment_bytes: unpacked_bytes,
        }
    }

    fn make_file_info(segments: Vec<FileDataSequenceEntry>) -> MDBFileInfo {
        MDBFileInfo {
            metadata: FileDataSequenceHeader {
                file_hash: MerkleHash::default(),
                ..Default::default()
            },
            segments,
            verification: vec![],
            metadata_ext: None,
        }
    }

    #[test]
    fn test_v2_url_roundtrip() {
        let hash = MerkleHash::from_hex("a32d3a2a2e83e4d41b04899f13a8e891f4dd3f2ed940f96f91da7bf55b7ee299").unwrap();
        let ranges = vec![
            make_range_descriptor(0, 3, 0, 1024),
            make_range_descriptor(5, 8, 2048, 4096),
        ];
        let timestamp = Instant::now();

        let url = generate_v2_fetch_url(&hash, &ranges, timestamp);
        let (parsed_hash, parsed_ts, parsed_ranges) = parse_v2_fetch_url(&url).unwrap();

        assert_eq!(hash, parsed_hash);
        assert_eq!(parsed_ranges.len(), 2);
        assert_eq!(parsed_ranges[0].start, 0);
        assert_eq!(parsed_ranges[0].end, 1024);
        assert_eq!(parsed_ranges[1].start, 2048);
        assert_eq!(parsed_ranges[1].end, 4096);

        let diff = if parsed_ts > timestamp {
            parsed_ts - timestamp
        } else {
            timestamp - parsed_ts
        };
        assert!(diff < Duration::from_millis(2));
    }

    #[test]
    fn test_v2_url_single_range() {
        let hash = MerkleHash::default();
        let ranges = vec![make_range_descriptor(0, 1, 100, 200)];
        let timestamp = Instant::now();

        let url = generate_v2_fetch_url(&hash, &ranges, timestamp);
        let (_, _, parsed_ranges) = parse_v2_fetch_url(&url).unwrap();

        assert_eq!(parsed_ranges.len(), 1);
        assert_eq!(parsed_ranges[0].start, 100);
        assert_eq!(parsed_ranges[0].end, 200);
    }

    #[test]
    fn test_v2_url_invalid_base64() {
        assert!(parse_v2_fetch_url("not-valid!!!").is_err());
    }

    #[test]
    fn test_v2_url_invalid_payload() {
        let url = URL_SAFE_NO_PAD.encode(b"bad");
        assert!(parse_v2_fetch_url(&url).is_err());
    }

    #[test]
    fn test_compute_ranges_single_segment() {
        let (xorb_hash, xorb_object) = build_xorb(&[100, 200, 300]);
        let file_info = make_file_info(vec![make_segment(xorb_hash, 0, 3, 600)]);

        let result = compute_reconstruction_ranges(&file_info, None, &mut |_| Ok(xorb_object.clone())).unwrap();
        let (offset, terms, merged) = result.unwrap();

        assert_eq!(offset, 0);
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].unpacked_length, 600);
        assert_eq!(terms[0].range.start, 0);
        assert_eq!(terms[0].range.end, 3);

        let xorb_ranges = merged.get(&xorb_hash).unwrap();
        assert_eq!(xorb_ranges.len(), 1);
        assert_eq!(xorb_ranges[0].chunk_range.start, 0);
        assert_eq!(xorb_ranges[0].chunk_range.end, 3);
    }

    #[test]
    fn test_compute_ranges_partial_range() {
        let (xorb_hash, xorb_object) = build_xorb(&[100, 200, 300]);
        let file_info = make_file_info(vec![make_segment(xorb_hash, 0, 3, 600)]);

        let range = FileRange::new(100, 300);
        let result = compute_reconstruction_ranges(&file_info, Some(range), &mut |_| Ok(xorb_object.clone())).unwrap();
        let (offset, terms, merged) = result.unwrap();

        assert_eq!(offset, 0, "range starts exactly at chunk boundary");
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].range.start, 1);
        assert_eq!(terms[0].range.end, 2);
        assert_eq!(terms[0].unpacked_length, 200);

        let xorb_ranges = merged.get(&xorb_hash).unwrap();
        assert_eq!(xorb_ranges.len(), 1);
        assert_eq!(xorb_ranges[0].chunk_range.start, 1);
        assert_eq!(xorb_ranges[0].chunk_range.end, 2);
    }

    #[test]
    fn test_compute_ranges_out_of_bounds() {
        let file_info = make_file_info(vec![make_segment(MerkleHash::default(), 0, 1, 100)]);

        let range = FileRange::new(200, 300);
        let result = compute_reconstruction_ranges(&file_info, Some(range), &mut |_| {
            panic!("should not be called for out-of-range")
        })
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_compute_ranges_empty_file() {
        let file_info = make_file_info(vec![]);

        let result =
            compute_reconstruction_ranges(&file_info, None, &mut |_| panic!("should not be called for empty file"))
                .unwrap();
        let (offset, terms, merged) = result.unwrap();
        assert_eq!(offset, 0);
        assert!(terms.is_empty());
        assert!(merged.is_empty());

        let result = compute_reconstruction_ranges(&file_info, Some(FileRange::new(0, 100)), &mut |_| {
            panic!("should not be called for empty file")
        })
        .unwrap();
        let (offset, terms, _) = result.unwrap();
        assert_eq!(offset, 0);
        assert!(terms.is_empty());

        let result = compute_reconstruction_ranges(&file_info, Some(FileRange::new(1, 100)), &mut |_| {
            panic!("should not be called for empty file")
        })
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_compute_ranges_merges_adjacent() {
        let (xorb_hash, xorb_object) = build_xorb(&[100, 100, 100, 100]);
        let file_info = make_file_info(vec![make_segment(xorb_hash, 0, 2, 200), make_segment(xorb_hash, 2, 4, 200)]);

        let result = compute_reconstruction_ranges(&file_info, None, &mut |_| Ok(xorb_object.clone())).unwrap();
        let (offset, terms, merged) = result.unwrap();

        assert_eq!(offset, 0);
        assert_eq!(terms.len(), 2);

        let xorb_ranges = merged.get(&xorb_hash).unwrap();
        assert_eq!(xorb_ranges.len(), 1);
        assert_eq!(xorb_ranges[0].chunk_range.start, 0);
        assert_eq!(xorb_ranges[0].chunk_range.end, 4);
    }

    #[test]
    fn test_compute_ranges_multi_xorb_non_contiguous() {
        let (hash_a, obj_a) = build_xorb(&[100, 100, 100, 100]);
        let (hash_b, obj_b) = build_xorb(&[150, 150]);

        let file_info = make_file_info(vec![
            make_segment(hash_a, 0, 2, 200),
            make_segment(hash_b, 0, 2, 300),
            make_segment(hash_a, 2, 4, 200),
        ]);

        let result = compute_reconstruction_ranges(&file_info, None, &mut |hash| {
            if *hash == hash_a {
                Ok(obj_a.clone())
            } else if *hash == hash_b {
                Ok(obj_b.clone())
            } else {
                Err(ClientError::XORBNotFound(*hash))
            }
        })
        .unwrap();

        let (offset, terms, merged) = result.unwrap();
        assert_eq!(offset, 0);
        assert_eq!(terms.len(), 3);

        let a_ranges = merged.get(&hash_a).unwrap();
        assert_eq!(a_ranges.len(), 1);
        assert_eq!(a_ranges[0].chunk_range.start, 0);
        assert_eq!(a_ranges[0].chunk_range.end, 4);

        let b_ranges = merged.get(&hash_b).unwrap();
        assert_eq!(b_ranges.len(), 1);
        assert_eq!(b_ranges[0].chunk_range.start, 0);
        assert_eq!(b_ranges[0].chunk_range.end, 2);
    }

    #[test]
    fn test_compute_ranges_truncates_to_file_size() {
        let (xorb_hash, xorb_object) = build_xorb(&[500]);
        let file_info = make_file_info(vec![make_segment(xorb_hash, 0, 1, 500)]);

        let range = FileRange::new(0, 10000);
        let result = compute_reconstruction_ranges(&file_info, Some(range), &mut |_| Ok(xorb_object.clone())).unwrap();
        let (offset, terms, _) = result.unwrap();
        assert_eq!(offset, 0);
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].unpacked_length, 500);
    }

    #[test]
    fn test_compute_ranges_offset_into_first_range() {
        let (xorb_hash, xorb_object) = build_xorb(&[100, 200, 300]);
        let file_info = make_file_info(vec![make_segment(xorb_hash, 0, 3, 600)]);

        let range = FileRange::new(150, 600);
        let result = compute_reconstruction_ranges(&file_info, Some(range), &mut |_| Ok(xorb_object.clone())).unwrap();
        let (offset, terms, _) = result.unwrap();

        assert_eq!(offset, 50);
        assert_eq!(terms[0].range.start, 1);
    }
}
