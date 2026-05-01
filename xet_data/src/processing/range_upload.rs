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

/// A dirty byte range paired with an async reader that provides the modified bytes.
///
/// Each `DirtyInput` represents a contiguous region of the file that was modified by the
/// caller. The `reader` must yield exactly `range.end - range.start` bytes.
pub struct DirtyInput {
    pub range: Range<u64>,
    pub reader: Pin<Box<dyn AsyncRead + Send>>,
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

/// Upload modified ranges of an existing file, composing the result with
/// the original file's CAS segments. Only the dirty regions (plus CDC boundary
/// chunks) are re-uploaded; stable regions between and around dirty ranges are
/// reused from the original file's reconstruction plan.
///
/// # When to use
///
/// - **Mid-file edit**: pass modified byte ranges in `dirty_inputs`, same `total_size`.
/// - **Append**: include `[original_size, total_size)` in `dirty_inputs` with a reader for the new bytes (including
///   sparse gaps). The last original chunk is automatically re-chunked.
/// - **Truncation**: pass `dirty_inputs = vec![]`, `total_size < original_size`. The boundary chunk at the cut point is
///   re-uploaded from CAS automatically.
/// - **No change**: pass `dirty_inputs = vec![]`, `total_size == original_size`. Returns the original hash immediately
///   (no CAS calls).
///
/// # Arguments
///
/// * `config` - Translator configuration for creating upload sessions.
/// * `cas_client` - CAS client for fetching original file metadata and downloading boundary chunks.
/// * `original_hash` - Merkle hash of the original file in CAS.
/// * `original_size` - Size of the original file in bytes.
/// * `dirty_inputs` - Sorted, non-overlapping dirty ranges, each paired with an async reader that yields exactly the
///   bytes for that range. Bytes outside these ranges within `[0, original_size)` are reconstructed from CAS. Each
///   reader is consumed exactly once.
/// * `total_size` - Total size of the modified file.
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
    total_size: u64,
) -> Result<XetFileInfo> {
    if dirty_inputs.is_empty() && total_size == original_size {
        return Ok(XetFileInfo::new(original_hash.hex(), original_size));
    }

    // Empty original: nothing to compose against — upload as a fresh file. Validation below
    // still runs (via the early return path inside `upload_fresh_file`) to enforce that the
    // caller provided coverage of `[0, total_size)`.
    if original_size == 0 {
        return upload_fresh_file(config, dirty_inputs, total_size).await;
    }

    let dirty_ranges_pairs: Vec<(u64, u64)> = dirty_inputs.iter().map(|d| (d.range.start, d.range.end)).collect();
    validate_dirty_ranges(&dirty_ranges_pairs, original_size, total_size)?;

    let recon_result = cas_client.get_file_reconstruction_info(&original_hash).await?;
    let original_mdb = recon_result
        .map(|(mdb, _)| mdb)
        .ok_or_else(|| DataError::ParameterError("file not found".into()))?;
    debug_assert_eq!(original_mdb.file_size(), original_size, "reconstruction info disagrees with original_size");

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
    let mut snapped: Vec<(u64, u64)> = Vec::with_capacity(dirty_ranges_pairs.len() + 1);
    for &(start, end) in &dirty_ranges_pairs {
        let in_start = start.min(original_size);
        let in_end = end.min(original_size);
        if in_start >= in_end {
            continue;
        }
        snapped
            .push((snap_to_segment_start(&seg_byte_starts, in_start), snap_to_segment_end(&seg_byte_starts, in_end)));
    }
    if total_size < original_size {
        let snap_start = snap_to_segment_start(&seg_byte_starts, total_size);
        if snap_start < original_size {
            // total_size+1 forces the snap forward to the first boundary strictly past the cut.
            let snap_end = snap_to_segment_end(&seg_byte_starts, total_size + 1);
            snapped.push((snap_start, snap_end));
        }
    }
    if total_size > original_size && !original_mdb.segments.is_empty() {
        // Append: always include the last original segment in the upload set so its boundary
        // extends past `original_size` into the appended bytes. Needed both for pure append
        // and for mid-edit-plus-append (otherwise the last existing window would be the one
        // we extend to `total_size`, which corrupts files with a stable tail before the
        // appended region). Coalescing below merges this with any overlapping range.
        let n = original_mdb.segments.len();
        snapped.push((seg_byte_starts[n - 1], seg_byte_starts[n]));
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
        return Err(DataError::InternalError(
            "internal: no server query computed for non-trivial upload_ranges call".into(),
        ));
    }

    let n_windows = server_query.len();
    let response: FileChunkHashesResponse = cas_client.get_file_chunk_hashes(&original_hash, server_query).await?;
    debug_assert_eq!(response.windows.len(), n_windows, "server windows must match segment-aligned query");
    debug_assert_eq!(response.hash_ranges.len(), n_windows + 1, "expected N+1 hash ranges for N windows");

    let ctx = config.ctx.clone();
    let session = FileUploadSession::new(config.clone()).await?;
    let mut input_idx = 0usize;
    let mut uploaded: Vec<UploadedWindow> = Vec::with_capacity(n_windows);

    let last_idx = n_windows - 1;
    for (idx, window) in response.windows.iter().enumerate() {
        let w_start = window.dirty_byte_range[0];
        let w_end_in_original = window.dirty_byte_range[1];

        let effective_end = if idx == last_idx && total_size != original_size {
            total_size
        } else {
            w_end_in_original
        };

        // Cleaner was sized to exactly `middle_size`; never stream past it (matters for truncation
        // where the segment runs further than `effective_end`).
        let original_window_end = w_end_in_original.min(original_size).min(effective_end);
        let middle_size = effective_end - w_start;

        let (_id, mut cleaner) = session.start_clean(None, Some(middle_size), Sha256Policy::Skip)?;

        let mut cursor = w_start;
        while input_idx < dirty_inputs.len() {
            let input_range_start = dirty_inputs[input_idx].range.start;
            if input_range_start >= effective_end {
                break;
            }
            let input = &mut dirty_inputs[input_idx];
            let input_start = input.range.start.max(w_start);
            let input_end = input.range.end.min(effective_end);

            // CAS gap before this input (within the original file).
            if cursor < input_start {
                let gap_end = input_start.min(original_window_end);
                if cursor < gap_end {
                    stream_cas_range(&ctx, &cas_client, original_hash, cursor, gap_end, &mut cleaner).await?;
                }
                if input_start > original_size && cursor < input_start {
                    return Err(DataError::InternalError(format!(
                        "gap in dirty_inputs: no data for bytes [{cursor}, {input_start}) \
                         (beyond original_size {original_size})"
                    )));
                }
            }

            // Stream the dirty bytes from the async reader.
            let bytes_to_read = (input_end - input_start) as usize;
            let mut remaining = bytes_to_read;
            let mut buf = vec![0u8; STREAM_BLOCK_SIZE.min(remaining.max(1))];
            while remaining > 0 {
                let to_read = buf.len().min(remaining);
                input.reader.read_exact(&mut buf[..to_read]).await.map_err(|err| {
                    DataError::InternalError(format!(
                        "failed to read dirty input [{}, {}): {err}",
                        input.range.start, input.range.end
                    ))
                })?;
                cleaner.add_data(&buf[..to_read]).await?;
                remaining -= to_read;
            }

            cursor = input_end;
            if input.range.end <= effective_end {
                input_idx += 1;
            } else {
                break;
            }
        }

        // CAS suffix: stable bytes after the last input within the original portion.
        if cursor < original_window_end {
            stream_cas_range(&ctx, &cas_client, original_hash, cursor, original_window_end, &mut cleaner).await?;
        }

        let (info, chunks, _metrics) = cleaner.finish().await?;
        uploaded.push(UploadedWindow {
            start: w_start,
            end: effective_end,
            info,
            chunks,
        });
    }

    session.checkpoint().await?;
    let mdb_list = session.file_info_list().await?;
    let mdb_by_hash: HashMap<MerkleHash, MDBFileInfo> =
        mdb_list.into_iter().map(|m| (m.metadata.file_hash, m)).collect();

    // Merge sequence: [gap0, w0, gap1, w1, ..., gapN]. Empty gaps (`None`) are skipped.
    // For truncation, the trailing gap covers bytes that no longer exist and is dropped.
    let mut hash_ranges = response.hash_ranges;
    let trailing_gap = if total_size >= original_size {
        hash_ranges.pop().flatten()
    } else {
        hash_ranges.pop().and(None)
    };
    let first_window_at_start = hash_ranges.first().is_some_and(Option::is_none);
    let last_window_at_end = trailing_gap.is_none();

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

    // Walk original segments; replace those a window covers with the window's segments.
    // Segment-aligned windows guarantee every original segment is wholly inside or outside a
    // window. Verification entries are 1:1 with segments when the original file had them on;
    // legacy / test files registered without verification yield an empty vec, in which case
    // we emit a verification-less composed MDB.
    let with_verification = original_mdb.verification.len() == original_mdb.segments.len();
    let mut all_segments: Vec<FileDataSequenceEntry> = Vec::new();
    let mut all_verification: Vec<FileVerificationEntry> = Vec::new();
    let mut seg_idx = 0usize;
    let n_segs = original_mdb.segments.len();
    let emit_seg = |idx: usize, segs: &mut Vec<FileDataSequenceEntry>, vers: &mut Vec<FileVerificationEntry>| {
        segs.push(original_mdb.segments[idx].clone());
        if with_verification {
            vers.push(original_mdb.verification[idx].clone());
        }
    };
    for w in &uploaded {
        while seg_idx < n_segs && seg_byte_starts[seg_idx] < w.start {
            debug_assert!(
                seg_byte_starts[seg_idx + 1] <= w.start,
                "segment straddles window start (not segment-aligned)"
            );
            emit_seg(seg_idx, &mut all_segments, &mut all_verification);
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
        if with_verification {
            all_verification.extend_from_slice(&middle_mdb.verification);
        }
    }
    if total_size >= original_size {
        while seg_idx < n_segs {
            emit_seg(seg_idx, &mut all_segments, &mut all_verification);
            seg_idx += 1;
        }
    }

    debug!(
        "upload_ranges: composed hash={}, {} segments, {} windows",
        combined_hash.hex(),
        all_segments.len(),
        uploaded.len()
    );

    let composed_mdb = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(combined_hash, all_segments.len(), with_verification, false),
        segments: all_segments,
        verification: all_verification,
        // SHA-256 is intentionally omitted: the file content changed, and recomputing it
        // would require reading the full file.
        metadata_ext: None,
    };

    session.register_composed_file(composed_mdb).await?;
    session.finalize().await?;

    let total_dirty: u64 = dirty_ranges_pairs.iter().map(|(s, e)| e - s).sum();
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
/// `dirty_ranges` must be sorted, non-overlapping, and contain only non-empty intervals
/// whose end is `<= total_size`. When `total_size > original_size` (append), the inputs
/// must reach `total_size` and cover the entire `[original_size, total_size)` tail with
/// no gap.
fn validate_dirty_ranges(dirty_ranges: &[(u64, u64)], original_size: u64, total_size: u64) -> Result<()> {
    if !dirty_ranges.windows(2).all(|w| w[0].1 <= w[1].0) {
        return Err(DataError::ParameterError(format!(
            "dirty_ranges must be sorted and non-overlapping, got: {dirty_ranges:?}"
        )));
    }
    if !dirty_ranges.iter().all(|&(s, e)| s < e) {
        return Err(DataError::ParameterError(format!(
            "dirty_ranges must be non-empty intervals, got: {dirty_ranges:?}"
        )));
    }
    if let Some(&(_, last_end)) = dirty_ranges.last()
        && last_end > total_size
    {
        return Err(DataError::ParameterError(format!(
            "dirty_range end ({last_end}) exceeds total_size ({total_size})"
        )));
    }
    if total_size > original_size {
        let last_input_end = dirty_ranges.last().map_or(0, |&(_, e)| e);
        if last_input_end < total_size {
            return Err(DataError::ParameterError(format!(
                "total_size ({total_size}) > original_size ({original_size}) but dirty_inputs \
                 only cover up to byte {last_input_end} (must reach total_size)"
            )));
        }
        // Walk the append tail. `covered_up_to` starts at `original_size` and only ever
        // grows, so any range whose `start` runs ahead of it leaves an uncovered gap.
        let mut covered_up_to = original_size;
        for &(start, end) in dirty_ranges {
            if start > covered_up_to {
                return Err(DataError::ParameterError(format!(
                    "gap in append region: bytes [{covered_up_to}, {start}) are beyond \
                     original_size ({original_size}) and not covered by any dirty input"
                )));
            }
            covered_up_to = covered_up_to.max(end);
        }
    }
    Ok(())
}

/// Upload a brand-new file from `dirty_inputs` (no original to compose against).
/// Used when the original file is empty: the caller-provided inputs already cover
/// `[0, total_size)` (verified here), so we just stream them through the cleaner and
/// finalize the session.
async fn upload_fresh_file(
    config: Arc<TranslatorConfig>,
    mut dirty_inputs: Vec<DirtyInput>,
    total_size: u64,
) -> Result<XetFileInfo> {
    let mut cursor = 0u64;
    for input in &dirty_inputs {
        if input.range.start > cursor {
            return Err(DataError::ParameterError(format!(
                "empty original: gap in dirty_inputs at [{cursor}, {})",
                input.range.start
            )));
        }
        cursor = input.range.end;
    }
    if cursor < total_size {
        return Err(DataError::ParameterError(format!(
            "empty original: dirty_inputs only cover up to byte {cursor} (must reach total_size {total_size})"
        )));
    }

    let session = FileUploadSession::new(config).await?;
    let (_id, mut cleaner) = session.start_clean(None, Some(total_size), Sha256Policy::Skip)?;
    for input in &mut dirty_inputs {
        let mut remaining = (input.range.end - input.range.start) as usize;
        let mut buf = vec![0u8; STREAM_BLOCK_SIZE.min(remaining.max(1))];
        while remaining > 0 {
            let to_read = buf.len().min(remaining);
            input.reader.read_exact(&mut buf[..to_read]).await.map_err(|err| {
                DataError::InternalError(format!(
                    "failed to read dirty input [{}, {}): {err}",
                    input.range.start, input.range.end
                ))
            })?;
            cleaner.add_data(&buf[..to_read]).await?;
            remaining -= to_read;
        }
    }
    let (info, _chunks, _metrics) = cleaner.finish().await?;
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

    /// Build `DirtyInput`s from a source buffer and range list. Each input gets
    /// a `Cursor` over the corresponding slice of `data`.
    fn make_dirty_inputs(ranges: &[(u64, u64)], data: &[u8]) -> Vec<DirtyInput> {
        ranges
            .iter()
            .map(|&(start, end)| {
                let slice = data[start as usize..end as usize].to_vec();
                DirtyInput {
                    range: start..end,
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
                range: start..end,
                reader: Box::pin(Cursor::new(Vec::new())),
            })
            .collect()
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
            let (xfi, _chunks, _metrics) = cleaner.finish().await.unwrap();
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
            make_dirty_inputs(&[(dirty_start as u64, dirty_end as u64)], &modified_data),
            total_size,
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
        let result =
            upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, vec![], truncated_size)
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
            make_dirty_inputs(&[(original_size, total_size)], &full_data),
            total_size,
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
            make_dirty_inputs(&[(0, 4096)], &modified_data),
            total_size,
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
            make_dirty_inputs(&[(10_000, 12_000), (200_000, 202_000)], &modified_data),
            total_size,
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
            make_dirty_inputs(&[(original_size, total_size)], &full_data),
            total_size,
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
            make_dirty_inputs(&[(original_size, total_size)], &sparse_staging),
            total_size,
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
                    make_dirty_inputs(&[(boundary, dirty_end)], &expected),
                    size,
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
        let result = upload_ranges(config, cas_client, hash, size, vec![], size).await.unwrap();

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
        let err = upload_ranges(config, cas_client, hash, size, make_dummy_inputs(&[(100, size + 1)]), size).await;
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
        let err =
            upload_ranges(config, cas_client, hash, size, make_dummy_inputs(&[(100, 300), (200, 400)]), size).await;
        assert!(err.is_err(), "overlapping ranges should be rejected");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_empty_dirty_range() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(61, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;
        let err = upload_ranges(config, cas_client, hash, size, make_dummy_inputs(&[(100, 100)]), size).await;
        assert!(err.is_err(), "empty range (start == end) should be rejected");
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
        let err =
            upload_ranges(config, cas_client, hash, size, make_dummy_inputs(&[(300, 400), (100, 200)]), size).await;
        assert!(err.is_err(), "unsorted ranges should be rejected");
    }

    // total_size > original_size but dirty_inputs don't cover appended region -> rejected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_append_without_dirty_inputs() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(63, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;
        let bigger = size + 1000;

        // No dirty inputs but total_size > original_size.
        let err = upload_ranges(config.clone(), cas_client.clone(), hash, size, vec![], bigger).await;
        assert!(err.is_err(), "append without dirty_inputs covering appended bytes should be rejected");

        // Dirty input stops before total_size.
        let partial = make_dirty_inputs(&[(size, size + 500)], &vec![0xEEu8; bigger as usize]);
        let err = upload_ranges(config.clone(), cas_client.clone(), hash, size, partial, bigger).await;
        assert!(err.is_err(), "append with partial coverage should be rejected");

        // Dirty input covers end but leaves gap after original_size.
        let gap_start = make_dirty_inputs(&[(size + 100, bigger)], &vec![0xEEu8; bigger as usize]);
        let err = upload_ranges(config, cas_client, hash, size, gap_start, bigger).await;
        assert!(err.is_err(), "append with gap at start of append region should be rejected");
    }

    // Regression: dirty_inputs = [(original_size + 100, total_size)] passes the old
    // "last input reaches total_size" check, but leaves a gap [original_size, original_size + 100)
    // that is beyond original_size (CAS can't fill it) and not covered by any input.
    // Without validation, the cleaner silently skips those bytes → corrupted file.
    //
    // Original: [################]  (256 KB)
    // Append:                    [--gap--][=====dirty=====]
    //                            ^        ^                ^
    //                       original   +100          total_size
    //                       = 256KB   = 256KB+100    = 256KB+50KB
    //
    // The gap [256KB, 256KB+100) has no source: CAS stops at 256KB, no input covers it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_append_with_gap_after_original_size() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = test_config(server.http_endpoint(), base_dir.path());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(42, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;
        let total_size = original_size + 50_000;
        let gap = 100u64;

        // Input starts at original_size + gap, leaving [original_size, original_size + gap) uncovered.
        let append_data = vec![0xBBu8; (total_size - original_size - gap) as usize];
        let inputs = vec![DirtyInput {
            range: (original_size + gap)..total_size,
            reader: Box::pin(std::io::Cursor::new(append_data)),
        }];

        let err = upload_ranges(config, cas_client, original_hash, original_size, inputs, total_size).await;
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("gap in append region"), "expected gap-in-append-region error, got: {msg}");
    }

    // original: [chunk0][chunk1][chunk2][chunk3][...more chunks...]
    // input:              [========= single large write ==========]
    //
    // A single DirtyInput that spans many chunks. Verifies that the reader is
    // consumed correctly even when build_dirty_regions merges multiple chunk
    // ranges into one DirtyRegion.
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
            make_dirty_inputs(&[(dirty_start, dirty_end)], &modified),
            original_size,
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
            range: 5..11,
            reader: Box::pin(Cursor::new(dirty_data.to_vec())),
        }];

        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            dirty_inputs,
            original_size,
        )
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

        let result =
            upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, vec![], truncated_size)
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
            make_dirty_inputs(&[(dirty_start, dirty_end)], &staging),
            truncated_size,
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
        let (xfi, _chunks, _metrics) = cleaner.finish().await.unwrap();
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

        // For appends, ensure the appended region is included as a dirty input.
        if total_size > original_size {
            let append_start = original_size;
            let already_covered = dirty_ranges.iter().any(|&(s, e)| s <= append_start && e >= total_size);
            if !already_covered {
                inputs.push(DirtyInput {
                    range: append_start..total_size,
                    reader: Box::pin(Cursor::new(expected[append_start as usize..total_size as usize].to_vec())),
                });
                inputs.sort_by_key(|d| d.range.start);
            }
        }

        let result =
            upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, inputs, total_size)
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
                range: dirty_start as u64..dirty_end as u64,
                reader: Box::pin(Cursor::new(expected[dirty_start..dirty_end].to_vec())),
            },
            DirtyInput {
                range: original_size..total_size,
                reader: Box::pin(Cursor::new(append_extra)),
            },
        ];
        let result =
            upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, inputs, total_size)
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
            range: 0..total_size,
            reader: Box::pin(Cursor::new(new_data.clone())),
        }];
        let result = upload_ranges(config.clone(), cas_client.clone(), original_hash, 0, inputs, total_size)
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

        let result = upload_ranges(config.clone(), cas_client.clone(), original_hash, original_size, vec![], 0)
            .await
            .unwrap();

        let clean_empty = upload_file(&config, &[]).await;
        assert_eq!(result.hash(), clean_empty.hex(), "truncate-to-empty must match clean empty upload hash");
    }
}
