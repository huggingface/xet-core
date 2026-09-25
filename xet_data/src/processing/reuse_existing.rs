//! Reuse of an existing (partial) local file when downloading a file to a path.
//!
//! When a download is started with `reuse_existing`, the bytes already present at the
//! destination path are checked segment by segment against the file's verification entries
//! (one keyed hash over the chunk hashes of each file segment, computed at upload time).
//! Segments that match are kept as is; only the remaining byte ranges are downloaded.
//!
//! Nothing about the existing file is trusted: it may be an interrupted download, a
//! corrupted one, or an unrelated file. The worst case is that every segment is downloaded.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use tracing::{info, warn};
use xet_client::cas_types::FileRange;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks;
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_runtime::core::XetContext;
use xet_runtime::core::par_utils::run_constrained;

use crate::deduplication::Chunker;
use crate::error::Result;
use crate::progress_tracking::ItemProgressUpdater;

/// Read size used when re-chunking existing data.
const READ_BLOCK_SIZE: usize = 8 * 1024 * 1024;

/// Outcome of checking an existing file against the expected content.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReusePlan {
    /// Byte ranges that still need to be downloaded, sorted and non-overlapping.
    pub ranges_to_download: Vec<FileRange>,
    /// Number of existing bytes that already hold the expected content.
    pub reused_bytes: u64,
}

/// Checks the file at `path` against `file_info` and returns which byte ranges must be downloaded.
///
/// Returns `None` when there is nothing to reuse (missing or empty file) or when the file has
/// no verification entries, in which case the caller should do a regular full download.
///
/// Only segments that lie entirely within the existing file are checked. Each one is re-chunked
/// independently, which is valid because segment boundaries are chunk boundaries and the chunker
/// state only depends on the data since the previous boundary. Segments are checked in parallel.
pub(crate) async fn plan_reuse(
    ctx: &XetContext,
    path: &Path,
    file_info: &MDBFileInfo,
    progress_updater: &Arc<ItemProgressUpdater>,
) -> Result<Option<ReusePlan>> {
    let existing_len = match std::fs::metadata(path) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e.into()),
    };
    if existing_len == 0 {
        return Ok(None);
    }
    if file_info.verification.len() != file_info.segments.len() {
        warn!(
            file_hash = %file_info.metadata.file_hash,
            "No verification entries available for this file; existing data cannot be reused"
        );
        return Ok(None);
    }

    // Segments with their byte range in the file, and whether they can be checked.
    let mut segments = Vec::with_capacity(file_info.segments.len());
    let mut offset = 0u64;
    for (segment, verification) in file_info.segments.iter().zip(&file_info.verification) {
        let range = FileRange::new(offset, offset + segment.unpacked_segment_bytes as u64);
        offset = range.end;
        segments.push((range, verification.range_hash));
    }

    let to_check: Vec<(FileRange, MerkleHash)> =
        segments.iter().copied().filter(|(r, _)| r.end <= existing_len).collect();
    let check_bytes: u64 = to_check.iter().map(|(r, _)| r.end - r.start).sum();
    progress_updater.update_resume_check_size(check_bytes);

    let max_concurrent = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let runtime = ctx.runtime.clone();
    let checks = to_check.into_iter().map(|(range, expected)| {
        let runtime = runtime.clone();
        let path = path.to_path_buf();
        let progress_updater = progress_updater.clone();
        async move {
            runtime
                .spawn_blocking(move || segment_matches(&path, range, expected, &progress_updater))
                .await?
        }
    });
    let matches = run_constrained(checks, max_concurrent).await?;

    // `matches` is in the same order as the checked segments, which are a prefix of `segments`.
    let mut ranges_to_download: Vec<FileRange> = Vec::new();
    let mut reused_bytes = 0u64;
    for (i, (range, _)) in segments.iter().enumerate() {
        if matches.get(i).copied().unwrap_or(false) {
            reused_bytes += range.end - range.start;
        } else if let Some(last) = ranges_to_download.last_mut()
            && last.end == range.start
        {
            last.end = range.end;
        } else {
            ranges_to_download.push(*range);
        }
    }

    info!(
        file_hash = %file_info.metadata.file_hash,
        existing_len,
        reused_bytes,
        n_ranges_to_download = ranges_to_download.len(),
        "Checked existing file for reuse"
    );

    Ok(Some(ReusePlan {
        ranges_to_download,
        reused_bytes,
    }))
}

/// Re-chunks `range` of the file at `path` and compares its verification hash with `expected`.
fn segment_matches(
    path: &Path,
    range: FileRange,
    expected: MerkleHash,
    progress_updater: &ItemProgressUpdater,
) -> Result<bool> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(range.start))?;

    let mut chunker = Chunker::default();
    let mut chunk_hashes = Vec::new();
    let mut buf = vec![0u8; READ_BLOCK_SIZE.min((range.end - range.start) as usize)];
    let mut remaining = range.end - range.start;
    while remaining > 0 {
        let n = buf.len().min(remaining as usize);
        match file.read_exact(&mut buf[..n]) {
            Ok(()) => {},
            // The file shrank since it was inspected: this segment just can't be reused.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        remaining -= n as u64;
        chunk_hashes.extend(chunker.next_block(&buf[..n], remaining == 0).into_iter().map(|c| c.hash));
        progress_updater.report_resume_check_bytes_completed(n as u64);
    }

    Ok(range_hash_from_chunks(&chunk_hashes) == expected)
}
