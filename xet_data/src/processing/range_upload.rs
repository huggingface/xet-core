use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, info};
use xet_client::cas_client::Client;
use xet_client::cas_types::{FileChunkHashesResponse, FileRange};
use xet_core_structures::merklehash::{ChunkHashList, MerkleHash, MerkleHashSubtree};
use xet_core_structures::metadata_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo,
};
use xet_runtime::core::XetContext;

use super::XetFileInfo;
use super::configurations::TranslatorConfig;
use super::file_cleaner::Sha256Policy;
use super::file_upload_session::FileUploadSession;
use crate::error::{DataError, Result};
use crate::file_reconstruction::FileReconstructor;

/// A single edit applied to the original file: replace `original_range` with `new_length`
/// bytes from `reader`.
///
/// All three combinations are supported:
/// - `original_range.len() == new_length` → in-place edit (no file size change).
/// - `original_range.len() != new_length` → resize edit (file grows or shrinks).
/// - `original_range.start == original_range.end` → pure insert at that position.
/// - `new_length == 0` → pure delete of `original_range`.
///
/// Pure append at end of file is `{ original_range: original_size..original_size, new_length: N, reader: ... }`.
/// Pure truncate-to-N is `{ original_range: N..original_size, new_length: 0, reader: empty }`.
pub struct DirtyInput {
    pub original_range: Range<u64>,
    pub reader: Pin<Box<dyn AsyncRead + Send>>,
    pub new_length: u64,
}

/// Size of blocks read from the dirty source and fed to the cleaner.
const STREAM_BLOCK_SIZE: usize = 4 * 1024 * 1024; // 4 MB

/// A dirty window the server told us to re-upload, augmented with the bytes the cleaner
/// produced and the resulting per-window file info. `start`/`end` are file byte offsets
/// extended for append/truncation past `original_size` if needed.
struct UploadedWindow {
    start: u64,
    end: u64,
    info: XetFileInfo,
    chunks: ChunkHashList,
}

/// Upload an edited version of an existing file, reusing the unchanged regions from the
/// original file's CAS segments and only re-uploading the parts the caller actually
/// rewrites.
///
/// `dirty_inputs` is a list of edits expressed in the **original file's coordinates**.
/// Each edit replaces `original_range` with `new_length` bytes from its reader; resize
/// edits (including pure inserts and pure deletes) are supported. The output file size is
/// derived from the inputs.
///
/// # When to use
///
/// - **In-place edit**: `original_range.len() == new_length`, file size unchanged.
/// - **Resize edit**: any `new_length`. Replaces `original_range.len()` original bytes with `new_length` new bytes.
/// - **Pure insert**: `original_range.start == original_range.end`, `new_length > 0`.
/// - **Pure delete**: `original_range.start < original_range.end`, `new_length == 0`.
/// - **Append**: `original_range == original_size..original_size`, `new_length > 0`.
/// - **Truncate to N**: `original_range == N..original_size`, `new_length == 0`.
/// - **No change**: empty `dirty_inputs`. Returns the original hash without any CAS call.
///
/// # Arguments
///
/// * `config` - Translator configuration for creating upload sessions.
/// * `cas_client` - CAS client for fetching original file metadata and downloading boundary bytes.
/// * `original_hash` - Merkle hash of the original file in CAS.
/// * `original_size` - Size of the original file in bytes.
/// * `dirty_inputs` - Edits to apply. Must be sorted by `original_range.start` and non-overlapping
///   (`prev.original_range.end <= next.original_range.start`). Each reader must yield exactly `new_length` bytes. Each
///   reader is consumed exactly once.
///
/// # Limitations
///
/// The composed file has no SHA-256 metadata (`metadata_ext = None`), since recomputing
/// it would require reading the full file. This means `upload_ranges` is only suitable
/// for contexts that don't require SHA-256 verification (e.g. HF buckets, xet-native
/// repos), not for Git LFS-backed repos that verify SHA-256 on download.
pub async fn upload_ranges(
    config: Arc<TranslatorConfig>,
    cas_client: Arc<dyn Client>,
    original_hash: MerkleHash,
    original_size: u64,
    mut dirty_inputs: Vec<DirtyInput>,
) -> Result<XetFileInfo> {
    validate_dirty_ranges(&dirty_inputs, original_size)?;
    let total_size = compute_total_size(original_size, &dirty_inputs);

    if dirty_inputs.is_empty() {
        debug_assert_eq!(total_size, original_size);
        return Ok(XetFileInfo::new(original_hash.hex(), original_size));
    }

    // Empty original: nothing to compose against — upload as a fresh file (concatenation of
    // the edits' new bytes, since every `original_range` must be `0..0`).
    if original_size == 0 {
        return upload_fresh_file(config, dirty_inputs, total_size).await;
    }

    let recon_result = cas_client.get_file_reconstruction_info(&original_hash).await?;
    let original_mdb = recon_result
        .map(|(mdb, _)| mdb)
        .ok_or_else(|| DataError::ParameterError("file not found".into()))?;
    if original_mdb.file_size() != original_size {
        return Err(DataError::ParameterError(format!(
            "caller said original_size={original_size} but reconstruction info reports {}",
            original_mdb.file_size()
        )));
    }

    // `seg_byte_starts[i]` is the first byte of segment `i`; the trailing entry equals `original_size`.
    let mut seg_byte_starts: Vec<u64> = Vec::with_capacity(original_mdb.segments.len() + 1);
    seg_byte_starts.push(0);
    let mut acc = 0u64;
    for s in &original_mdb.segments {
        acc += s.unpacked_segment_bytes as u64;
        seg_byte_starts.push(acc);
    }

    // Snapping to *segment* boundaries (rather than chunk boundaries) lets us swap whole
    // segments during composition, so the client never has to truncate a segment mid-chunk.
    // Safe to send to the server because segment edges are chunk edges, so the server's
    // chunk-aligned windows come back identical to our snapped ranges.
    //
    // Pure inserts (`original_range.start == original_range.end`) snap to the segment that
    // owns the insert position so the cleaner has enough surrounding bytes to re-chunk
    // around the insertion. An insert at `original_size` snaps to the last segment.
    let n_segs = original_mdb.segments.len();
    let mut snapped: Vec<(u64, u64)> = Vec::with_capacity(dirty_inputs.len());
    for input in &dirty_inputs {
        let r = &input.original_range;
        let (s, e) = if r.start == r.end {
            // Pure insert: pick the segment containing `r.start`. At end-of-file, fall back to
            // the last segment.
            if r.start == original_size {
                (seg_byte_starts[n_segs - 1], seg_byte_starts[n_segs])
            } else {
                (snap_to_segment_start(&seg_byte_starts, r.start), snap_to_segment_end(&seg_byte_starts, r.start + 1))
            }
        } else {
            (snap_to_segment_start(&seg_byte_starts, r.start), snap_to_segment_end(&seg_byte_starts, r.end))
        };
        snapped.push((s, e));
    }

    snapped.sort_by_key(|&(s, _)| s);
    let mut coalesced: Vec<(u64, u64)> = Vec::with_capacity(snapped.len());
    for r in snapped {
        if let Some(last) = coalesced.last_mut()
            && r.0 <= last.1
        {
            last.1 = last.1.max(r.1);
            continue;
        }
        coalesced.push(r);
    }

    let server_query: Vec<FileRange> = coalesced.iter().map(|&(s, e)| FileRange::new(s, e)).collect();
    if server_query.is_empty() {
        return Err(DataError::InternalError("internal: non-empty dirty_inputs produced no server query".into()));
    }

    let n_windows = server_query.len();
    let response: FileChunkHashesResponse = cas_client.get_file_chunk_hashes(&original_hash, server_query).await?;
    // These invariants are part of the server contract; violating them silently truncates
    // the merge sequence (`zip` would drop windows) and produces a wrong file hash, so we
    // bail loudly instead of trusting the response.
    if response.windows.len() != n_windows {
        return Err(DataError::InternalError(format!(
            "server returned {} windows, expected {n_windows} (one per dirty range)",
            response.windows.len()
        )));
    }
    if response.hash_ranges.len() != n_windows + 1 {
        return Err(DataError::InternalError(format!(
            "server returned {} hash_ranges, expected {} (n_windows + 1)",
            response.hash_ranges.len(),
            n_windows + 1
        )));
    }
    let gap_verification = response.gap_verification;

    let ctx = config.ctx.clone();
    let session = FileUploadSession::new(config.clone()).await?;
    let mut input_idx = 0usize;
    let mut uploaded: Vec<UploadedWindow> = Vec::with_capacity(n_windows);

    let mut buf = vec![0u8; STREAM_BLOCK_SIZE];
    for window in response.windows.iter() {
        let w_start = window.dirty_byte_range[0];
        let w_end = window.dirty_byte_range[1];

        // Find the slice of edits that land in this window. A pure insert at exactly
        // `w_end` belongs here only when `w_end == original_size` (no later window can
        // take it); anywhere else it belongs to the next window starting at that byte.
        let edits_end = dirty_inputs[input_idx..]
            .iter()
            .take_while(|d| {
                let r = &d.original_range;
                if r.start == r.end {
                    r.start < w_end || (r.start == w_end && w_end == original_size)
                } else {
                    r.end <= w_end
                }
            })
            .count()
            + input_idx;
        let window_edits = &mut dirty_inputs[input_idx..edits_end];

        let (removed, added): (u64, u64) = window_edits
            .iter()
            .map(|d| (d.original_range.end - d.original_range.start, d.new_length))
            .fold((0, 0), |(rm, ad), (r, a)| (rm + r, ad + a));
        let middle_size = (w_end - w_start) + added - removed;

        let (_id, mut cleaner) = session.start_clean(None, Some(middle_size), Sha256Policy::Skip)?;

        let mut cursor = w_start;
        for input in window_edits.iter_mut() {
            let edit_start = input.original_range.start;
            let edit_end = input.original_range.end;
            debug_assert!(edit_start >= w_start && edit_end <= w_end, "edit straddles window (validation bug)");

            if cursor < edit_start {
                stream_cas_range(&ctx, &cas_client, original_hash, cursor, edit_start, &mut cleaner).await?;
            }

            let mut remaining = input.new_length as usize;
            while remaining > 0 {
                let to_read = buf.len().min(remaining);
                input.reader.read_exact(&mut buf[..to_read]).await.map_err(|err| {
                    DataError::InternalError(format!(
                        "failed to read dirty input [{}, {}): {err}",
                        input.original_range.start, input.original_range.end
                    ))
                })?;
                cleaner.add_data(&buf[..to_read]).await?;
                remaining -= to_read;
            }

            cursor = edit_end;
        }
        input_idx = edits_end;

        if cursor < w_end {
            stream_cas_range(&ctx, &cas_client, original_hash, cursor, w_end, &mut cleaner).await?;
        }

        let (info, chunks, _metrics) = cleaner.finish_with_chunks().await?;
        uploaded.push(UploadedWindow {
            start: w_start,
            end: w_end,
            info,
            chunks,
        });
    }

    // Every edit must have been assigned to exactly one window. If the server
    // returned narrower `dirty_byte_range`s than requested, leftover edits would
    // silently drop and produce a corrupt file with no error.
    if input_idx != dirty_inputs.len() {
        return Err(DataError::InternalError(format!(
            "{} dirty edits not assigned to any window (input_idx={input_idx}, total={})",
            dirty_inputs.len() - input_idx,
            dirty_inputs.len()
        )));
    }

    session.checkpoint().await?;
    let mdb_list = session.file_info_list().await?;
    let mdb_by_hash: HashMap<MerkleHash, MDBFileInfo> =
        mdb_list.into_iter().map(|m| (m.metadata.file_hash, m)).collect();

    // Merge sequence: [gap0, w0, gap1, w1, ..., gapN]. Empty gaps (`None`) are skipped.
    let mut hash_ranges = response.hash_ranges;
    let trailing_gap = hash_ranges.pop().flatten();
    let first_window_at_start = hash_ranges.first().is_some_and(Option::is_none);
    let last_window_at_end = trailing_gap.is_none();
    let last_idx = uploaded.len() - 1;

    let mut merge_seq: Vec<MerkleHashSubtree> = Vec::with_capacity(2 * n_windows + 1);
    for (i, (w, gap)) in uploaded.iter().zip(hash_ranges).enumerate() {
        if let Some(g) = gap {
            merge_seq.push(g);
        }
        let at_start = i == 0 && first_window_at_start;
        let at_end = i == last_idx && last_window_at_end;
        merge_seq.push(MerkleHashSubtree::from_chunks(at_start, &w.chunks, at_end));
    }
    if let Some(g) = trailing_gap {
        merge_seq.push(g);
    }

    let merged = MerkleHashSubtree::merge(&merge_seq)
        .map_err(|err| DataError::InternalError(format!("MerkleHashSubtree::merge failed: {err}")))?;
    // `final_hash()` is the aggregated chunk hash; the file hash is its HMAC with the zero
    // salt (matching `file_hash` / the cleaner's output for files without SHA-256 metadata,
    // the only flavor `upload_ranges` produces). Empty content is the one exception:
    // `file_hash([])` short-circuits to `MerkleHash::default()` *without* HMAC, so we mirror
    // that here when the result is zero-length (e.g. truncating to empty).
    let aggregated_hash = merged.final_hash().ok_or_else(|| {
        DataError::InternalError("merged subtree is not fully closed; cannot derive final hash".into())
    })?;
    let combined_hash = if total_size == 0 {
        MerkleHash::default()
    } else {
        aggregated_hash.hmac(MerkleHash::default())
    };

    // The composed shard must carry a verification section if either side has one: the
    // server populates `gap_verification` whenever the original file's segments had verif
    // entries, and the locally-fetched `original_mdb` (sim path) populates
    // `verification` directly. Real prod always takes the first half; sim tests the second.
    let original_has_verification =
        !gap_verification.is_empty() || original_mdb.verification.len() == original_mdb.segments.len();
    let mut all_segments: Vec<FileDataSequenceEntry> = Vec::new();
    let mut all_verification: Vec<FileVerificationEntry> = Vec::new();
    let mut seg_idx = 0usize;
    let n_segs = original_mdb.segments.len();
    let mut gap_idx = 0usize;
    let push_stable = |idx: usize,
                       gap_idx: &mut usize,
                       segs: &mut Vec<FileDataSequenceEntry>,
                       vers: &mut Vec<FileVerificationEntry>|
     -> Result<()> {
        segs.push(original_mdb.segments[idx].clone());
        if original_has_verification {
            let entry = gap_verification.get(*gap_idx).ok_or_else(|| {
                DataError::InternalError(format!(
                    "ran out of gap_verification entries at stable segment {idx}; \
                     server response is inconsistent with the segment layout"
                ))
            })?;
            vers.push(FileVerificationEntry::new(entry.into()));
            *gap_idx += 1;
        }
        Ok(())
    };
    for w in &uploaded {
        while seg_idx < n_segs && seg_byte_starts[seg_idx] < w.start {
            if seg_byte_starts[seg_idx + 1] > w.start {
                return Err(DataError::InternalError(format!(
                    "server returned a window starting at {} that straddles segment {} \
                     ({}..{}); composition requires segment-aligned windows",
                    w.start,
                    seg_idx,
                    seg_byte_starts[seg_idx],
                    seg_byte_starts[seg_idx + 1]
                )));
            }
            push_stable(seg_idx, &mut gap_idx, &mut all_segments, &mut all_verification)?;
            seg_idx += 1;
        }
        let original_window_end = w.end.min(original_size);
        while seg_idx < n_segs && seg_byte_starts[seg_idx] < original_window_end {
            seg_idx += 1;
        }
        let middle_hash = MerkleHash::from_hex(w.info.hash())?;
        let middle_mdb = mdb_by_hash
            .get(&middle_hash)
            .ok_or_else(|| DataError::InternalError(format!("no MDBFileInfo for window hash {}", middle_hash.hex())))?;
        all_segments.extend_from_slice(&middle_mdb.segments);
        if original_has_verification {
            if middle_mdb.verification.len() != middle_mdb.segments.len() {
                return Err(DataError::InternalError(format!(
                    "window MDB for {} has {} segments but {} verification entries",
                    middle_hash.hex(),
                    middle_mdb.segments.len(),
                    middle_mdb.verification.len()
                )));
            }
            all_verification.extend_from_slice(&middle_mdb.verification);
        }
    }
    while seg_idx < n_segs {
        push_stable(seg_idx, &mut gap_idx, &mut all_segments, &mut all_verification)?;
        seg_idx += 1;
    }
    if gap_idx < gap_verification.len() {
        return Err(DataError::InternalError(format!(
            "server returned {} gap_verification entries but only {} stable segments were emitted",
            gap_verification.len(),
            gap_idx
        )));
    }

    debug!(
        "upload_ranges: composed hash={}, {} segments, {} windows",
        combined_hash.hex(),
        all_segments.len(),
        uploaded.len()
    );

    debug_assert!(!original_has_verification || all_segments.len() == all_verification.len());

    let composed_mdb = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(combined_hash, all_segments.len(), original_has_verification, false),
        segments: all_segments,
        verification: all_verification,
        // SHA-256 is intentionally omitted: the file content changed, and recomputing it
        // would require reading the full file.
        metadata_ext: None,
    };

    session.register_composed_file(composed_mdb).await?;
    session.finalize().await?;

    let total_dirty: u64 = dirty_inputs.iter().map(|d| d.new_length).sum();
    info!(
        "upload_ranges: hash={} size={} (original={}, {} windows, {} dirty bytes)",
        combined_hash.hex(),
        total_size,
        original_size,
        uploaded.len(),
        total_dirty
    );

    Ok(XetFileInfo::new(combined_hash.hex(), total_size))
}

/// Validate the caller-provided dirty ranges.
///
/// `dirty_inputs` must be sorted by `original_range.start`, non-overlapping
/// (`prev.original_range.end <= next.original_range.start`), and every
/// `original_range.end <= original_size`. Empty ranges (pure inserts) are allowed at any
/// position including `original_size` (append).
fn validate_dirty_ranges(dirty_inputs: &[DirtyInput], original_size: u64) -> Result<()> {
    let mut prev_end = 0u64;
    for (i, input) in dirty_inputs.iter().enumerate() {
        let r = &input.original_range;
        if r.start > r.end {
            return Err(DataError::ParameterError(format!(
                "dirty_inputs[{i}].original_range is reversed: {}..{}",
                r.start, r.end
            )));
        }
        if r.end > original_size {
            return Err(DataError::ParameterError(format!(
                "dirty_inputs[{i}].original_range end ({}) exceeds original_size ({original_size})",
                r.end
            )));
        }
        if i > 0 && r.start < prev_end {
            return Err(DataError::ParameterError(format!(
                "dirty_inputs[{i}].original_range overlaps the previous edit (starts at {} < {prev_end})",
                r.start
            )));
        }
        prev_end = r.end;
    }
    Ok(())
}

/// Compute the resulting file size: `original_size` minus bytes removed plus bytes added.
fn compute_total_size(original_size: u64, dirty_inputs: &[DirtyInput]) -> u64 {
    let (removed, added) = dirty_inputs
        .iter()
        .fold((0u64, 0u64), |(r, a), d| (r + (d.original_range.end - d.original_range.start), a + d.new_length));
    original_size + added - removed
}

/// Upload a brand-new file from `dirty_inputs` (no original to compose against).
/// Used when the original file is empty: every edit's `original_range` is `0..0`, so we
/// just stream their new bytes through the cleaner.
async fn upload_fresh_file(
    config: Arc<TranslatorConfig>,
    mut dirty_inputs: Vec<DirtyInput>,
    total_size: u64,
) -> Result<XetFileInfo> {
    let session = FileUploadSession::new(config).await?;
    let (_id, mut cleaner) = session.start_clean(None, Some(total_size), Sha256Policy::Skip)?;
    for input in &mut dirty_inputs {
        let mut remaining = input.new_length as usize;
        let mut buf = vec![0u8; STREAM_BLOCK_SIZE.min(remaining.max(1))];
        while remaining > 0 {
            let to_read = buf.len().min(remaining);
            input.reader.read_exact(&mut buf[..to_read]).await.map_err(|err| {
                DataError::InternalError(format!("failed to read dirty input at {}: {err}", input.original_range.start))
            })?;
            cleaner.add_data(&buf[..to_read]).await?;
            remaining -= to_read;
        }
    }
    let (info, _metrics) = cleaner.finish().await?;
    session.finalize().await?;
    Ok(info)
}

/// Stream a byte range from CAS into the cleaner.
async fn stream_cas_range(
    ctx: &XetContext,
    cas_client: &Arc<dyn Client>,
    file_hash: MerkleHash,
    start: u64,
    end: u64,
    cleaner: &mut super::SingleFileCleaner,
) -> Result<()> {
    let reconstructor = FileReconstructor::new(ctx, cas_client, file_hash).with_byte_range(FileRange::new(start, end));
    let mut stream = reconstructor.reconstruct_to_stream();
    while let Some(chunk) = stream.next().await? {
        cleaner.add_data(&chunk).await?;
    }
    Ok(())
}

/// Largest segment-start byte that is `<= byte`. Used to snap a dirty-range start back
/// to its enclosing segment boundary.
fn snap_to_segment_start(seg_byte_starts: &[u64], byte: u64) -> u64 {
    let idx = seg_byte_starts.partition_point(|&s| s <= byte);
    seg_byte_starts[idx.saturating_sub(1)]
}

/// Smallest segment-start byte that is `>= byte`. Used to snap a dirty-range end forward
/// to the segment boundary that fully contains it.
fn snap_to_segment_end(seg_byte_starts: &[u64], byte: u64) -> u64 {
    let idx = seg_byte_starts.partition_point(|&s| s < byte);
    seg_byte_starts[idx]
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::TempDir;
    use xet_client::cas_client::{Client, LocalTestServerBuilder};
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;
    use crate::processing::configurations::TranslatorConfig;
    use crate::processing::file_cleaner::Sha256Policy;
    use crate::processing::file_download_session::FileDownloadSession;
    use crate::processing::file_upload_session::FileUploadSession;

    fn test_config(endpoint: impl AsRef<str>, base_dir: impl AsRef<Path>) -> Arc<TranslatorConfig> {
        let ctx = XetContext::default().unwrap();
        Arc::new(TranslatorConfig::test_server_config(&ctx, endpoint, base_dir).unwrap())
    }

    /// Test helper: fetch the original file's per-segment byte sizes. Tests use these to
    /// build dirty ranges that align with segment (== chunk-group) boundaries — handy for
    /// scenarios that want to overwrite or truncate exactly on a boundary.
    async fn fetch_segment_sizes(cas_client: &Arc<dyn Client>, hash: &MerkleHash) -> Vec<u64> {
        let (mdb, _) = cas_client.get_file_reconstruction_info(hash).await.unwrap().unwrap();
        mdb.segments.iter().map(|s| s.unpacked_segment_bytes as u64).collect()
    }

    /// Build in-place `DirtyInput`s (each edit's `new_length` matches its `original_range`
    /// length, so the file size doesn't change) from a source buffer and range list.
    fn make_dirty_inputs(ranges: &[(u64, u64)], data: &[u8]) -> Vec<DirtyInput> {
        ranges
            .iter()
            .map(|&(start, end)| {
                let slice = data[start as usize..end as usize].to_vec();
                DirtyInput {
                    original_range: start..end,
                    new_length: end - start,
                    reader: Box::pin(Cursor::new(slice)),
                }
            })
            .collect()
    }

    /// Build `DirtyInput`s with dummy readers (for validation error tests where
    /// the reader is never consumed).
    fn make_dummy_inputs(ranges: &[(u64, u64)]) -> Vec<DirtyInput> {
        ranges
            .iter()
            .map(|&(start, end)| DirtyInput {
                original_range: start..end,
                new_length: end - start,
                reader: Box::pin(Cursor::new(Vec::new())),
            })
            .collect()
    }

    /// Bridge the old `(start, end)` + `total_size`-shaped test cases into the new edit
    /// model. `(start, end)` indexes into `data` AND describes the output byte range.
    /// Handles in-place edits, pure appends, mid-edit-plus-append spans (split into two
    /// edits), and trailing truncations.
    fn make_legacy_inputs(specs: &[(u64, u64)], data: &[u8], original_size: u64, total_size: u64) -> Vec<DirtyInput> {
        let mut out: Vec<DirtyInput> = Vec::new();
        for &(start, end) in specs {
            let bytes = data[start as usize..end as usize].to_vec();
            if end <= original_size {
                out.push(DirtyInput {
                    original_range: start..end,
                    new_length: bytes.len() as u64,
                    reader: Box::pin(Cursor::new(bytes)),
                });
            } else if start >= original_size {
                out.push(DirtyInput {
                    original_range: original_size..original_size,
                    new_length: bytes.len() as u64,
                    reader: Box::pin(Cursor::new(bytes)),
                });
            } else {
                let split = (original_size - start) as usize;
                let (head, tail) = bytes.split_at(split);
                out.push(DirtyInput {
                    original_range: start..original_size,
                    new_length: head.len() as u64,
                    reader: Box::pin(Cursor::new(head.to_vec())),
                });
                out.push(DirtyInput {
                    original_range: original_size..original_size,
                    new_length: tail.len() as u64,
                    reader: Box::pin(Cursor::new(tail.to_vec())),
                });
            }
        }
        if total_size < original_size {
            out.push(DirtyInput {
                original_range: total_size..original_size,
                new_length: 0,
                reader: Box::pin(Cursor::new(Vec::new())),
            });
        }
        out
    }

    // original: [=========================== 256 KB ===========================]
    // dirty:                  [== 1 KB ===]
    // result:   [===stable===][re-uploaded][============stable=================]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_mid_file_edit() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let endpoint = server.http_endpoint().to_string();
        let config = test_config(&endpoint, base_dir.path());

        // Use the server directly as the CAS client (bypasses HTTP for get_file_chunk_hashes).
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // 1. Upload an original file: 256 KB of pseudo-random bytes.
        let original_data = random_data(42, 256 * 1024);
        let original_hash = {
            let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
            let (_id, mut cleaner) = upload_session
                .start_clean(Some("original".into()), Some(original_data.len() as u64), Sha256Policy::Skip)
                .unwrap();
            cleaner.add_data(&original_data).await.unwrap();
            let (xfi, _metrics) = cleaner.finish().await.unwrap();
            upload_session.finalize().await.unwrap();
            MerkleHash::from_hex(xfi.hash()).unwrap()
        };
        let original_size = original_data.len() as u64;

        // 2. Build modified content: overwrite [100_000, 101_000) with 0xBB.
        let mut modified_data = original_data.clone();
        let dirty_start = 100_000usize;
        let dirty_end = 101_000usize;
        modified_data[dirty_start..dirty_end].fill(0xBB);
        let total_size = modified_data.len() as u64;
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(dirty_start as u64, dirty_end as u64)], &modified_data, original_size, total_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size, Some(total_size));

        // 3. Download and verify the composed file.
        let composed_hash = MerkleHash::from_hex(result.hash()).unwrap();
        let session = FileDownloadSession::new(config.clone(), None).await.unwrap();
        let file_info = crate::processing::XetFileInfo::new(composed_hash.hex(), total_size);
        let out_path = base_dir.path().join("output");
        session.download_file(&file_info, &out_path).await.unwrap();
        let downloaded = std::fs::read(&out_path).unwrap();

        assert_eq!(downloaded.len(), modified_data.len());
        assert_eq!(downloaded, modified_data);

        // Hash must match a clean upload of the same content.
        let clean_hash = upload_file(&config, &modified_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [=========================== 256 KB ===========================]
    // result:   [========= 100 KB =========]
    //                                       ^ cut here (mid-chunk)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_truncation() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = random_data(43, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Truncate to 100 KB (no dirty ranges, pure truncation).
        let truncated_size = 100_000u64;
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[], &[], original_size, truncated_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), Some(truncated_size));

        // Download and verify: first truncated_size bytes should match original.
        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), truncated_size).await;
        assert_eq!(downloaded.len(), truncated_size as usize);
        assert_eq!(downloaded, &original_data[..truncated_size as usize]);

        let clean_hash = upload_file(&config, &original_data[..truncated_size as usize]).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [======== 100 KB ========]
    // result:   [======== 100 KB ========][== 50 KB appended ==]
    //                                     ^ last chunk re-chunked with append
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_append() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 100 KB file.
        let original_data = random_data(44, 100 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Append 50 KB of pseudo-random data.
        let mut full_data = original_data.clone();
        full_data.extend(random_data(99, 50 * 1024));
        let total_size = full_data.len() as u64;
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(original_size, total_size)], &full_data, original_size, total_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), Some(total_size));

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded, full_data);

        let clean_hash = upload_file(&config, &full_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [=========================== 256 KB ==============================]
    // dirty:    [4K]
    // result:   [re-uploaded][=================stable=============================]
    //           ^ no stable prefix
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_at_file_start() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = random_data(45, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Overwrite [0, 4096) with 0xBB (dirty range at offset 0, no stable prefix).
        let mut modified_data = original_data.clone();
        modified_data[..4096].fill(0xBB);
        let total_size = modified_data.len() as u64;
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(0, 4096)], &modified_data, original_size, total_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), Some(total_size));

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded.len(), modified_data.len());
        assert_eq!(downloaded, modified_data);

        let clean_hash = upload_file(&config, &modified_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [=========================== 256 KB ===========================]
    // dirty:       [2K]                               [2K]
    // result:   [s][re-up][=======stable========][re-up][========stable========]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_multiple_regions() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = random_data(46, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Two non-adjacent dirty ranges with a stable gap between them.
        let mut modified_data = original_data.clone();
        modified_data[10_000..12_000].fill(0xBB); // first dirty range
        modified_data[200_000..202_000].fill(0xCC); // second dirty range
        let total_size = modified_data.len() as u64;
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(10_000, 12_000), (200_000, 202_000)], &modified_data, original_size, total_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), Some(total_size));

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded.len(), modified_data.len());
        assert_eq!(downloaded, modified_data);

        let clean_hash = upload_file(&config, &modified_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [======== 100 KB ========]
    // inputs:   [======== 100 KB ========][000][=4K written=]
    //                                     ^ gap (zeros from seek past EOF)
    //
    // With the new API, the caller provides the full append region [original_size, total_size)
    // as a single DirtyInput, including the sparse gap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_append_with_gap_before_dirty_range() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(50, 100 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let gap = 500u64;
        let write_data = random_data(101, 4096);
        let total_size = original_size + gap + write_data.len() as u64;

        let mut full_data = original_data.clone();
        full_data.extend(vec![0x00u8; gap as usize]);
        full_data.extend(&write_data);

        // The caller provides the entire append region (including the sparse gap).
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(original_size, total_size)], &full_data, original_size, total_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), Some(total_size));

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded.len(), full_data.len(), "size mismatch");
        assert_eq!(&downloaded[..], &full_data[..], "content mismatch: gap bytes were lost");

        let clean_hash = upload_file(&config, &full_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // staging:  [000000 (zeros) 000000][=== appended ===]
    // CAS:      [=== original data ==]
    // result:   [=== original data ==][=== appended ===]
    //           ^ boundary prefix must come from CAS, not from staging zeros
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_append_sparse_staging_file() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = vec![0xDDu8; 100 * 1024];
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let append_data = vec![0xEEu8; 50 * 1024];
        let total_size = original_size + append_data.len() as u64;

        // Build a sparse staging file: zeros for [0, original_size), real data after.
        // This is what hf-mount produces (sparse hole + appended bytes).
        let mut sparse_staging = vec![0u8; total_size as usize];
        sparse_staging[original_size as usize..].copy_from_slice(&append_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(original_size, total_size)], &sparse_staging, original_size, total_size),
        )
        .await
        .unwrap();

        // Expected: original data + appended data (not zeros + appended data).
        let mut expected = original_data.clone();
        expected.extend(&append_data);

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded.len(), expected.len(), "size mismatch");
        assert_eq!(&downloaded[..], &expected[..], "content mismatch: CAS data replaced by zeros from sparse file");

        let clean_hash = upload_file(&config, &expected).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_data_integrity_scenarios() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // original: [========================= 256 KB =========================]
        // dirty:                         [10K]
        // result:   [====== 90 KB ======][10K]
        //           0           90K   100K   ^ truncate here, dirty touches cut
        {
            let original = vec![0xAAu8; 256 * 1024];
            let mut expected = original[..100_000].to_vec();
            expected[90_000..100_000].fill(0xBB);
            assert_range_edit(&config, &cas_client, &original, &expected, &[(90_000, 100_000)], 100_000).await;
        }

        // original: [========= 128 KB =========]
        // dirty:    [========= 128 KB =========]
        // result:   [======= re-uploaded ======]  (no stable regions)
        {
            let original = vec![0xAAu8; 128 * 1024];
            let expected = vec![0xBBu8; 128 * 1024];
            let size = original.len() as u64;
            assert_range_edit(&config, &cas_client, &original, &expected, &[(0, size)], size).await;
        }

        // original: [===================== 256 KB =====================]
        // dirty:                [1K][1K][1K]
        //                       ^-- coalesced into one region
        {
            let original = vec![0xAAu8; 256 * 1024];
            let mut expected = original.clone();
            expected[50_000..51_000].fill(0xBB);
            expected[51_000..52_000].fill(0xCC);
            expected[52_000..53_000].fill(0xDD);
            let size = original.len() as u64;
            assert_range_edit(
                &config,
                &cas_client,
                &original,
                &expected,
                &[(50_000, 51_000), (51_000, 52_000), (52_000, 53_000)],
                size,
            )
            .await;
        }

        // original: [======== 100 KB ========]
        // result:   [======== 100 KB ========][== 50 KB ==]
        //           no dirty_ranges, only total_size > original_size
        {
            let original = vec![0xAAu8; 100 * 1024];
            let mut expected = original.clone();
            expected.extend(vec![0xEEu8; 50 * 1024]);
            let total = expected.len() as u64;
            assert_range_edit(&config, &cas_client, &original, &expected, &[], total).await;
        }

        // original: [chunk0][chunk1][chunk2][...]
        // dirty:                    [chunk2]
        //                           ^      ^-- starts/ends on chunk boundary
        {
            let original: Vec<u8> = (0..256 * 1024)
                .map(|i: usize| {
                    let x = i.wrapping_mul(2654435761);
                    (x >> 16) as u8
                })
                .collect();
            let original_hash = upload_file(&config, &original).await;
            let seg_sizes = fetch_segment_sizes(&cas_client, &original_hash).await;
            if seg_sizes.len() >= 3 {
                let boundary: u64 = seg_sizes[0] + seg_sizes[1];
                let dirty_end = boundary + seg_sizes[2];
                let mut expected = original.clone();
                expected[boundary as usize..dirty_end as usize].fill(0xFF);
                let size = original.len() as u64;
                let result = upload_ranges(
                    config.clone(),
                    cas_client.clone(),
                    original_hash,
                    size,
                    make_legacy_inputs(&[(boundary, dirty_end)], &expected, size, size),
                )
                .await
                .unwrap();
                let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), size).await;
                assert_eq!(downloaded, expected, "chunk-boundary edit mismatch");

                let clean_hash = upload_file(&config, &expected).await;
                assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
            }
        }
    }

    // No changes: dirty_ranges=[], total_size == original_size -> early return.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_noop_returns_original_hash() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(70, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;
        let result = upload_ranges(config, cas_client, hash, size, make_legacy_inputs(&[], &[], size, size))
            .await
            .unwrap();

        assert_eq!(result.hash(), hash.hex());
        assert_eq!(result.file_size(), Some(size));
    }

    // dirty_range end > total_size -> rejected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_dirty_range_past_total_size() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(71, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;
        let err = upload_ranges(config, cas_client, hash, size, make_dummy_inputs(&[(100, size + 1)])).await;
        assert!(err.is_err(), "dirty range past total_size should be rejected");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_overlapping_dirty_ranges() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(60, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;
        let err = upload_ranges(config, cas_client, hash, size, make_dummy_inputs(&[(100, 300), (200, 400)])).await;
        assert!(err.is_err(), "overlapping ranges should be rejected");
    }

    // Regression: validation must run *before* the `original_size == 0` short-circuit, or
    // the empty-original path silently accepts ranges that exceed `original_size`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_empty_original_validates_ranges() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_hash = upload_file(&config, &[]).await;

        // `0..10` and `5..15` both have `end > original_size == 0`; either would corrupt
        // the upload if validation didn't fire.
        let inputs = vec![
            DirtyInput {
                original_range: 0..10,
                new_length: 10,
                reader: Box::pin(Cursor::new(vec![0xAA; 10])),
            },
            DirtyInput {
                original_range: 5..15,
                new_length: 10,
                reader: Box::pin(Cursor::new(vec![0xBB; 10])),
            },
        ];
        let err = upload_ranges(config, cas_client, original_hash, 0, inputs).await;
        assert!(err.is_err(), "ranges with end > original_size must be rejected for empty originals too");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_unsorted_dirty_ranges() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(62, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;
        let err = upload_ranges(config, cas_client, hash, size, make_dummy_inputs(&[(300, 400), (100, 200)])).await;
        assert!(err.is_err(), "unsorted ranges should be rejected");
    }

    // original: [chunk0][chunk1][chunk2][chunk3][...more chunks...]
    // input:              [========= single large write ==========]
    //
    // A single DirtyInput that spans many segments. Verifies the reader is consumed
    // correctly across the multi-segment window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_input_spanning_many_chunks() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(99, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Overwrite a large middle section (likely spans many CDC chunks).
        let mut modified = original_data.clone();
        let dirty_start = 10_000u64;
        let dirty_end = 200_000u64;
        modified[dirty_start as usize..dirty_end as usize].fill(0xFF);

        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(dirty_start, dirty_end)], &modified, original_size, original_size),
        )
        .await
        .unwrap();

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), original_size).await;
        assert_eq!(downloaded, modified, "large spanning input produced wrong content");

        let clean_hash = upload_file(&config, &modified).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: b"AAAA_HEADER_AAAA|" (17 bytes, single CAS chunk)
    // dirty:          [SPARSE]        (bytes [5, 11))
    // expected: b"AAAA_SPARSE_AAAA|"  (17 bytes)
    //
    // Tests mid-file edit on a very small file (single chunk, smaller than
    // typical CDC minimum). See test_truncate_then_mid_edit for the regression
    // test that reproduces the real production bug.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_small_file_mid_edit() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = b"AAAA_HEADER_AAAA|";
        let original_hash = upload_file(&config, original_data).await;
        let original_size = original_data.len() as u64;

        let dirty_data = b"SPARSE";
        let dirty_inputs = vec![DirtyInput {
            original_range: 5..11,
            new_length: dirty_data.len() as u64,
            reader: Box::pin(Cursor::new(dirty_data.to_vec())),
        }];

        let result = upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, dirty_inputs)
            .await
            .unwrap();

        assert_eq!(result.file_size(), Some(original_size));

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), original_size).await;
        assert_eq!(downloaded.len(), original_size as usize, "reconstructed size mismatch");
        assert_eq!(&downloaded[..5], b"AAAA_", "prefix from CAS");
        assert_eq!(&downloaded[5..11], b"SPARSE", "dirty range");
        assert_eq!(&downloaded[11..], b"_AAAA|", "suffix from CAS");

        let expected = b"AAAA_SPARSE_AAAA|";
        let clean_hash = upload_file(&config, expected).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [=========================== 256 KB ===========================]
    // staging:  [0000000000000000000000000000] (all zeros, file never opened for write)
    // result:   [====== 100 KB from CAS =====]
    //                                         ^ cut here (mid-chunk)
    //
    // The boundary chunk bytes must come from CAS, not from the zero-filled staging.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_truncation_empty_staging() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(77, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let truncated_size = 100_000u64;

        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[], &[], original_size, truncated_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), Some(truncated_size));

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), truncated_size).await;
        assert_eq!(downloaded.len(), truncated_size as usize);
        assert_eq!(
            &downloaded[..],
            &original_data[..truncated_size as usize],
            "truncated content should match original CAS data, not staging zeros"
        );

        let clean_hash = upload_file(&config, &original_data[..truncated_size as usize]).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [=========================== 256 KB ===========================]
    // staging:  [000000000000][0xBB][0000000] (zeros except the dirty range)
    //                          ^  ^
    //                       90K  95K (dirty from caller)
    // result:   [==CAS==][stg][===CAS===]
    //                                    ^ cut at 100K (mid-chunk)
    //
    // Dirty bytes [90K,95K) come from staging; boundary bytes from CAS.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_truncation_with_overlapping_dirty() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(88, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let truncated_size = 100_000u64;

        let dirty_start = 90_000u64;
        let dirty_end = 95_000u64;

        let mut expected = original_data[..truncated_size as usize].to_vec();
        expected[dirty_start as usize..dirty_end as usize].fill(0xBB);

        let mut staging = vec![0u8; truncated_size as usize];
        staging[dirty_start as usize..dirty_end as usize].fill(0xBB);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[(dirty_start, dirty_end)], &staging, original_size, truncated_size),
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), Some(truncated_size));

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), truncated_size).await;
        assert_eq!(downloaded.len(), expected.len());
        assert_eq!(&downloaded[..], &expected[..], "dirty bytes should come from staging, boundary bytes from CAS");

        let clean_hash = upload_file(&config, &expected).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn random_data(seed: u64, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| {
                let x = (i as u64).wrapping_add(seed).wrapping_mul(2654435761);
                (x >> 16) as u8
            })
            .collect()
    }

    async fn upload_file(config: &Arc<TranslatorConfig>, data: &[u8]) -> MerkleHash {
        let session = FileUploadSession::new(config.clone()).await.unwrap();
        let (_id, mut cleaner) = session
            .start_clean(Some("test".into()), Some(data.len() as u64), Sha256Policy::Skip)
            .unwrap();
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        session.finalize().await.unwrap();
        MerkleHash::from_hex(xfi.hash()).unwrap()
    }

    async fn download_file(config: &Arc<TranslatorConfig>, hash: MerkleHash, size: u64) -> Vec<u8> {
        let session = FileDownloadSession::new(config.clone(), None).await.unwrap();
        let xfi = crate::processing::XetFileInfo::new(hash.hex(), size);
        let dir = TempDir::new().unwrap();
        let out = dir.path().join("out");
        session.download_file(&xfi, &out).await.unwrap();
        std::fs::read(&out).unwrap()
    }

    /// Empty `Box::pin(Cursor::new(Vec::new()))` for delete / dummy edits.
    fn empty_reader() -> Pin<Box<dyn AsyncRead + Send>> {
        Box::pin(Cursor::new(Vec::<u8>::new()))
    }

    /// End-to-end check: upload `original`, apply `inputs` via `upload_ranges`, download,
    /// compare against `expected`, and verify the hash matches a clean upload of `expected`.
    async fn assert_edits(
        config: &Arc<TranslatorConfig>,
        cas_client: &Arc<dyn Client>,
        original: &[u8],
        inputs: Vec<DirtyInput>,
        expected: &[u8],
    ) {
        let original_hash = upload_file(config, original).await;
        let original_size = original.len() as u64;
        let result = upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, inputs)
            .await
            .unwrap();
        assert_eq!(result.file_size(), Some(expected.len() as u64), "file size mismatch");
        let downloaded =
            download_file(config, MerkleHash::from_hex(result.hash()).unwrap(), expected.len() as u64).await;
        assert_eq!(downloaded, expected, "content mismatch");
        let clean = upload_file(config, expected).await;
        assert_eq!(result.hash(), clean.hex(), "hash diverges from clean upload");
    }

    async fn assert_range_edit(
        config: &Arc<TranslatorConfig>,
        cas_client: &Arc<dyn Client>,
        original_data: &[u8],
        expected: &[u8],
        dirty_ranges: &[(u64, u64)],
        total_size: u64,
    ) {
        let original_hash = upload_file(config, original_data).await;
        let original_size = original_data.len() as u64;

        // Build dirty inputs from the caller's ranges.
        let mut inputs = make_dirty_inputs(dirty_ranges, expected);

        // For appends, ensure the appended region is included as a pure-insert edit at the
        // end of the original.
        if total_size > original_size {
            let append_start = original_size;
            let already_covered = dirty_ranges.iter().any(|&(s, e)| s <= append_start && e >= total_size);
            if !already_covered {
                inputs.push(DirtyInput {
                    original_range: original_size..original_size,
                    new_length: total_size - original_size,
                    reader: Box::pin(Cursor::new(expected[append_start as usize..total_size as usize].to_vec())),
                });
                inputs.sort_by_key(|d| d.original_range.start);
            }
        }

        // Truncation: drop bytes past `total_size` from the original via a pure-delete edit.
        if total_size < original_size {
            inputs.push(DirtyInput {
                original_range: total_size..original_size,
                new_length: 0,
                reader: empty_reader(),
            });
            inputs.sort_by_key(|d| d.original_range.start);
        }

        let result = upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, inputs)
            .await
            .unwrap();

        assert_eq!(result.file_size(), Some(total_size), "file size mismatch");
        let downloaded = download_file(config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded.len(), expected.len(), "downloaded length mismatch");
        assert_eq!(&downloaded[..], expected, "content mismatch");

        let clean_hash = upload_file(config, expected).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // Regression: mid-file edit + tail append in a single call. Codex caught that the last
    // existing window (covering the mid-file edit) was being stretched to `total_size`,
    // dropping the stable bytes between the edit and the appended region.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mid_edit_plus_append() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(7, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let dirty_start = 50_000usize;
        let dirty_end = 51_000usize;
        let append_extra: Vec<u8> = (0..16 * 1024).map(|i| (i % 251) as u8).collect();
        let mut expected = original_data.clone();
        expected[dirty_start..dirty_end].fill(0xAA);
        expected.extend_from_slice(&append_extra);
        let total_size = expected.len() as u64;

        let inputs = vec![
            DirtyInput {
                original_range: dirty_start as u64..dirty_end as u64,
                new_length: (dirty_end - dirty_start) as u64,
                reader: Box::pin(Cursor::new(expected[dirty_start..dirty_end].to_vec())),
            },
            DirtyInput {
                original_range: original_size..original_size,
                new_length: append_extra.len() as u64,
                reader: Box::pin(Cursor::new(append_extra)),
            },
        ];
        let result = upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, inputs)
            .await
            .unwrap();

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded, expected, "content mismatch (mid-edit + append regression)");
        let clean_hash = upload_file(&config, &expected).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // Regression: empty original + append. Codex caught that we'd return an internal error
    // instead of treating it as a fresh upload.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_empty_original_append() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data: &[u8] = &[];
        let original_hash = upload_file(&config, original_data).await;
        let new_data: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
        let total_size = new_data.len() as u64;

        let inputs = vec![DirtyInput {
            original_range: 0..0,
            new_length: total_size,
            reader: Box::pin(Cursor::new(new_data.clone())),
        }];
        let result = upload_ranges(config.clone(), cas_client.clone(), original_hash, 0, inputs)
            .await
            .unwrap();

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded, new_data);
        let clean_hash = upload_file(&config, &new_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // Regression: truncating to empty must produce the canonical empty-file hash
    // (`MerkleHash::default()` without HMAC), not `default.hmac(default)`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_truncate_to_empty_matches_clean_empty() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(11, 64 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            make_legacy_inputs(&[], &[], original_size, 0),
        )
        .await
        .unwrap();

        let clean_empty = upload_file(&config, &[]).await;
        assert_eq!(result.hash(), clean_empty.hex(), "truncate-to-empty must match clean empty upload hash");
    }

    // The three small examples that motivated the resize-edit API: replace, insert, delete
    // on a tiny 3-byte file. Each produces an output of a different length than the input.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resize_edits_abc() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // abc + replace [0, 1) with "foo" => "foobc"
        assert_edits(
            &config,
            &cas_client,
            b"abc",
            vec![DirtyInput {
                original_range: 0..1,
                new_length: 3,
                reader: Box::pin(Cursor::new(b"foo".to_vec())),
            }],
            b"foobc",
        )
        .await;

        // abc + insert "foo" at 0 => "fooabc"
        assert_edits(
            &config,
            &cas_client,
            b"abc",
            vec![DirtyInput {
                original_range: 0..0,
                new_length: 3,
                reader: Box::pin(Cursor::new(b"foo".to_vec())),
            }],
            b"fooabc",
        )
        .await;

        // abc + delete [0, 1) => "bc"
        assert_edits(
            &config,
            &cas_client,
            b"abc",
            vec![DirtyInput {
                original_range: 0..1,
                new_length: 0,
                reader: empty_reader(),
            }],
            b"bc",
        )
        .await;
    }

    // original: [============= 256 KB =============]
    // edit:                [== 4K stale ==]
    //                      ^ replaced with 32 K (resize +28 K)
    // result:   [== prefix ==][== 32K new ==][== suffix ==]
    //
    // Big in-place replace where new_length >> original_range.len(); exercises the
    // resize branch with a multi-segment-touching window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resize_large_replace_grows_file() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original = random_data(101, 256 * 1024);
        let drop_start = 100_000usize;
        let drop_end = 104_000usize;
        let new_bytes: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
        let mut expected = Vec::with_capacity(original.len() - (drop_end - drop_start) + new_bytes.len());
        expected.extend_from_slice(&original[..drop_start]);
        expected.extend_from_slice(&new_bytes);
        expected.extend_from_slice(&original[drop_end..]);

        assert_edits(
            &config,
            &cas_client,
            &original,
            vec![DirtyInput {
                original_range: drop_start as u64..drop_end as u64,
                new_length: new_bytes.len() as u64,
                reader: Box::pin(Cursor::new(new_bytes)),
            }],
            &expected,
        )
        .await;
    }

    // original: [============= 256 KB =============]
    // edit:                [============= 80 KB stale =============]
    //                      ^ replaced with 4 K (resize -76 K)
    // result:   [== prefix ==][4 K new][== suffix ==]
    //
    // Big in-place replace where new_length << original_range.len(); shrink case.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resize_large_replace_shrinks_file() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original = random_data(102, 256 * 1024);
        let drop_start = 80_000usize;
        let drop_end = 160_000usize;
        let new_bytes: Vec<u8> = (0..4 * 1024).map(|i| (0xCC ^ i) as u8).collect();
        let mut expected = Vec::with_capacity(original.len() - (drop_end - drop_start) + new_bytes.len());
        expected.extend_from_slice(&original[..drop_start]);
        expected.extend_from_slice(&new_bytes);
        expected.extend_from_slice(&original[drop_end..]);

        assert_edits(
            &config,
            &cas_client,
            &original,
            vec![DirtyInput {
                original_range: drop_start as u64..drop_end as u64,
                new_length: new_bytes.len() as u64,
                reader: Box::pin(Cursor::new(new_bytes)),
            }],
            &expected,
        )
        .await;
    }

    // original: [============= 100 KB =============]
    // edit:                  [insert 8 KB here]
    // result:   [== prefix ==][== 8 KB new ==][======= suffix =======]
    //
    // Pure mid-file insert (range start == end), large payload that forces re-chunking
    // around the insertion point.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resize_mid_file_insert() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original = random_data(103, 100 * 1024);
        let at = 40_000usize;
        let new_bytes: Vec<u8> = (0..8u32 * 1024).map(|i| (i.wrapping_mul(13) % 251) as u8).collect();
        let mut expected = Vec::with_capacity(original.len() + new_bytes.len());
        expected.extend_from_slice(&original[..at]);
        expected.extend_from_slice(&new_bytes);
        expected.extend_from_slice(&original[at..]);

        assert_edits(
            &config,
            &cas_client,
            &original,
            vec![DirtyInput {
                original_range: at as u64..at as u64,
                new_length: new_bytes.len() as u64,
                reader: Box::pin(Cursor::new(new_bytes)),
            }],
            &expected,
        )
        .await;
    }

    // original: [============= 256 KB =============]
    // edit:                [== 64 KB hole ==]
    //                      ^ delete this slice (no replacement)
    // result:   [== prefix ==][== suffix ==]
    //
    // Pure mid-file delete: shrinks the file without touching the tail boundary.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resize_mid_file_delete() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original = random_data(104, 256 * 1024);
        let drop_start = 80_000usize;
        let drop_end = 144_000usize;
        let mut expected = Vec::with_capacity(original.len() - (drop_end - drop_start));
        expected.extend_from_slice(&original[..drop_start]);
        expected.extend_from_slice(&original[drop_end..]);

        assert_edits(
            &config,
            &cas_client,
            &original,
            vec![DirtyInput {
                original_range: drop_start as u64..drop_end as u64,
                new_length: 0,
                reader: empty_reader(),
            }],
            &expected,
        )
        .await;
    }

    // original: [== seg0 ==][== seg1 ==][== seg2 ==]
    // edits:        [shrink][grow][   delete   ]
    //
    // Three independent edits in one call: an in-place shrink, a pure insert in seg1, and
    // a pure delete in seg2. Mix of resize directions, far enough apart that they don't
    // coalesce into one window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resize_multi_edit_mix() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original = random_data(105, 384 * 1024);
        let (a_start, a_end) = (10 * 1024usize, 20 * 1024usize);
        let a_new: Vec<u8> = vec![0xAA; 2 * 1024];
        let b_at = 150 * 1024usize;
        let b_new: Vec<u8> = vec![0xBB; 4 * 1024];
        let (c_start, c_end) = (300 * 1024usize, 320 * 1024usize);

        let mut expected = Vec::with_capacity(original.len() + b_new.len());
        expected.extend_from_slice(&original[..a_start]);
        expected.extend_from_slice(&a_new);
        expected.extend_from_slice(&original[a_end..b_at]);
        expected.extend_from_slice(&b_new);
        expected.extend_from_slice(&original[b_at..c_start]);
        expected.extend_from_slice(&original[c_end..]);

        assert_edits(
            &config,
            &cas_client,
            &original,
            vec![
                DirtyInput {
                    original_range: a_start as u64..a_end as u64,
                    new_length: a_new.len() as u64,
                    reader: Box::pin(Cursor::new(a_new)),
                },
                DirtyInput {
                    original_range: b_at as u64..b_at as u64,
                    new_length: b_new.len() as u64,
                    reader: Box::pin(Cursor::new(b_new)),
                },
                DirtyInput {
                    original_range: c_start as u64..c_end as u64,
                    new_length: 0,
                    reader: empty_reader(),
                },
            ],
            &expected,
        )
        .await;
    }

    // original: [== seg0 ==][== seg1 ==]
    //                       ^ insert here, exactly on segment boundary
    // result:   [== seg0 ==][== inserted ==][== seg1 ==]
    //
    // Pure insert on a segment boundary mid-file. Snap picks the segment starting at the
    // boundary; insert lands at `w_start` of that window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resize_insert_at_segment_boundary() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original = random_data(106, 200 * 1024);
        let original_hash = upload_file(&config, &original).await;
        let original_size = original.len() as u64;

        // Pick the first interior segment boundary; bail if the file lands in one segment.
        let seg_sizes = fetch_segment_sizes(&cas_client, &original_hash).await;
        let Some(boundary) = seg_sizes
            .iter()
            .scan(0u64, |acc, s| {
                *acc += s;
                Some(*acc)
            })
            .find(|&b| b > 0 && b < original_size)
        else {
            return;
        };

        let new_bytes: Vec<u8> = vec![0x42; 4 * 1024];
        let mut expected = Vec::with_capacity(original.len() + new_bytes.len());
        expected.extend_from_slice(&original[..boundary as usize]);
        expected.extend_from_slice(&new_bytes);
        expected.extend_from_slice(&original[boundary as usize..]);

        assert_edits(
            &config,
            &cas_client,
            &original,
            vec![DirtyInput {
                original_range: boundary..boundary,
                new_length: new_bytes.len() as u64,
                reader: Box::pin(Cursor::new(new_bytes)),
            }],
            &expected,
        )
        .await;
    }
}
