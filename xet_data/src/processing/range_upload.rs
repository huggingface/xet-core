use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use tracing::{debug, info};
use ulid::Ulid;
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_core_structures::merklehash::{ChunkHashList, MerkleHash, file_hash};
use xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks;
use xet_core_structures::metadata_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo,
};

use super::XetFileInfo;
use super::configurations::TranslatorConfig;
use super::errors::{DataProcessingError, Result};
use super::file_cleaner::Sha256Policy;
use super::file_upload_session::FileUploadSession;
use crate::file_reconstruction::FileReconstructor;

/// Trait alias for a seekable byte source (e.g. `std::fs::File`).
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

/// Size of blocks read from the dirty source and fed to the cleaner.
const STREAM_BLOCK_SIZE: usize = 4 * 1024 * 1024; // 4 MB

/// A dirty byte range expanded to chunk-aligned boundaries.
struct DirtyRegion {
    dirty_start: u64,
    dirty_end: u64,
    first_chunk: usize, // inclusive
    last_chunk: usize,  // exclusive
}

/// Result of uploading a single dirty region through the cleaner.
struct UploadedRegion {
    region: DirtyRegion,
    info: XetFileInfo,
    chunks: ChunkHashList,
}

/// A dirty region paired with its MDBFileInfo and chunk hashes.
struct ComposedRegion {
    region: DirtyRegion,
    mdb: MDBFileInfo,
    chunks: ChunkHashList,
}

/// Upload modified ranges of an existing file, composing the result with
/// the original file's CAS segments. Only the dirty regions (plus CDC boundary
/// chunks) are re-uploaded; stable regions between and around dirty ranges are
/// reused from the original file's reconstruction plan.
///
/// # When to use
///
/// - **Mid-file edit**: pass the modified byte ranges in `dirty_ranges`, same `total_size`.
/// - **Append**: pass `dirty_ranges` covering the written bytes (or empty), `total_size > original_size`. The last
///   original chunk is automatically re-chunked with the appended data.
/// - **Truncation**: pass `dirty_ranges = &[]`, `total_size < original_size`. The boundary chunk at the cut point is
///   automatically re-uploaded.
/// - **No change**: pass `dirty_ranges = &[]`, `total_size == original_size`. Returns the original hash immediately (no
///   CAS calls).
///
/// `dirty_ranges` can be empty when only the file size changed (truncation or
/// append via ftruncate). The function adds implicit dirty ranges as needed.
///
/// # Arguments
///
/// * `config` - Translator configuration for creating upload sessions.
/// * `cas_client` - CAS client for fetching original file metadata and downloading boundary chunks.
/// * `original_hash` - Merkle hash of the original file in CAS.
/// * `original_size` - Size of the original file in bytes.
/// * `dirty_ranges` - Sorted, non-overlapping `(start, end)` byte ranges that were modified. Must not overlap and must
///   be in ascending order. Can be empty for pure size changes.
/// * `dirty_source` - Seekable reader positioned over the full modified file (e.g. a staging file). Only bytes within
///   dirty ranges (and the append region) are read.
/// * `total_size` - Total size of the modified file. Compared to `original_size` to detect append (`total_size >
///   original_size`) or truncation (`total_size < original_size`).
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
    dirty_ranges: &[(u64, u64)],
    dirty_source: &mut dyn ReadSeek,
    total_size: u64,
) -> Result<XetFileInfo> {
    // No changes: return original file as-is.
    if dirty_ranges.is_empty() && total_size == original_size {
        return Ok(XetFileInfo::new(original_hash.hex(), original_size));
    }

    // Ranges must be in ascending order with no overlaps.
    if !dirty_ranges.windows(2).all(|w| w[0].1 <= w[1].0) {
        return Err(DataProcessingError::InternalError(format!(
            "dirty_ranges must be sorted and non-overlapping, got: {dirty_ranges:?}"
        )));
    }
    // Each range must cover at least one byte.
    if !dirty_ranges.iter().all(|&(s, e)| s < e) {
        return Err(DataProcessingError::InternalError(format!(
            "dirty_ranges must be non-empty intervals, got: {dirty_ranges:?}"
        )));
    }
    // No range may extend past the end of the file.
    if let Some(&(_, last_end)) = dirty_ranges.last()
        && last_end > total_size
    {
        return Err(DataProcessingError::InternalError(format!(
            "dirty_range end ({last_end}) exceeds total_size ({total_size})"
        )));
    }

    // 1. Fetch chunk hashes and reconstruction info in parallel.
    let (original_chunks, recon_result) = tokio::try_join!(
        cas_client.get_file_chunk_hashes(&original_hash),
        cas_client.get_file_reconstruction_info(&original_hash),
    )?;
    let original_mdb = recon_result
        .map(|(mdb, _)| mdb)
        .ok_or_else(|| DataProcessingError::InternalError("no reconstruction info for original file".into()))?;

    // 2. Map chunks to cumulative byte offsets.
    // This builds a sorted array of byte boundaries, where chunk_offsets[i] is the
    // start byte of chunk[i]. chunk_offsets has len = original_chunks.len() + 1,
    // and chunk_offsets[i+1] is the end byte of chunk[i].
    //
    // Example with 3 chunks of sizes [100, 200, 150]:
    //   chunk_offsets = [0, 100, 300, 450]
    //                     ^   ^    ^    ^
    //                     |   |    |    +-- end of chunk[2]
    //                     |   |    +------ end of chunk[1] = start of chunk[2]
    //                     |   +---------- end of chunk[0] = start of chunk[1]
    //                     +-------------- start of chunk[0]
    let mut chunk_offsets: Vec<u64> = Vec::with_capacity(original_chunks.len() + 1);
    let mut offset = 0u64;
    chunk_offsets.push(0);
    for (_, size) in &original_chunks {
        offset += size;
        chunk_offsets.push(offset);
    }

    // 3. Build effective dirty ranges: start from caller's ranges, then handle truncation/append.
    let mut effective_ranges: Vec<(u64, u64)> = dirty_ranges.to_vec();

    let num_chunks = original_chunks.len();
    // Number of original chunks to keep in the final composition.
    let mut compose_num_chunks = num_chunks;

    if total_size < original_size {
        // Truncation: when the cut point falls mid-chunk, we can't reuse that chunk
        // (CAS chunks are immutable). We re-upload bytes from the last full chunk
        // boundary up to total_size, and only keep chunks entirely before the cut.
        //
        // Example: truncate from 450 to 250 bytes.
        //
        //   chunk[0]=[0,100)  chunk[1]=[100,300)  chunk[2]=[300,450)
        //                                  ^--- cut at 250 falls here
        //
        //   chunk[0]: fully before cut  -> reuse (stable)
        //   chunk[1]: partially before  -> re-upload bytes [100, 250)
        //   chunk[2]: fully after cut   -> discard
        let last_full = chunk_offsets.iter().rposition(|&o| o <= total_size).unwrap_or(0);
        compose_num_chunks = last_full;
        let boundary = chunk_offsets[last_full];
        if boundary < total_size {
            // Cut falls mid-chunk: re-upload [boundary, total_size) from the dirty source.
            // If boundary == total_size, the cut is exactly on a chunk boundary and
            // all kept chunks are complete, so no re-upload is needed.
            merge_or_push(&mut effective_ranges, (boundary, total_size));
        }
    }
    if total_size > original_size {
        // Append: add an implicit dirty range for the new bytes. The last original chunk
        // (EOF-terminated) will be included via a first_chunk adjustment below, and its
        // bytes will come from CAS via the boundary prefix mechanism.
        //
        // Example: file grew from 450 to 550 bytes.
        //   The last chunk [300,450) gets re-chunked together with appended bytes [450,550).
        //   Boundary prefix downloads [300,450) from CAS, dirty reads [450,550) from staging.
        merge_or_push(&mut effective_ranges, (original_size, total_size));
    }

    // Note: if effective_ranges is empty here, it means pure truncation (no dirty ranges,
    // file shrunk). We still proceed to compose a new file from the truncated chunk set.

    // 4. Expand dirty byte ranges to chunk-aligned boundaries.
    //
    // A dirty range rarely starts/ends on a chunk boundary. Since CAS chunks are
    // atomic (can't reuse half a chunk), we expand each range to cover every chunk
    // it touches. Adjacent/overlapping regions are then coalesced.
    //
    //   chunk[0]=[0,100)  chunk[1]=[100,300)  chunk[2]=[300,450)
    //
    //   dirty bytes [150, 350)
    //                 ^    ^
    //                 |    +-- inside chunk[2]
    //                 +------- inside chunk[1]
    //
    //   -> expand to chunks [1, 3)  (chunks 1 and 2 must be re-uploaded)
    let dirty_regions = build_dirty_regions(&effective_ranges, &chunk_offsets, num_chunks, original_size, total_size)?;

    // 5. Process each dirty region: download boundary, stream dirty bytes, upload. Collect the resulting middle file
    //    infos and chunk hashes. A single upload session is shared across all dirty regions.
    let session = FileUploadSession::new(config.clone(), None).await?;

    let mut uploaded_regions: Vec<UploadedRegion> = Vec::with_capacity(dirty_regions.len());

    for region in dirty_regions {
        let boundary_start = *chunk_offsets.get(region.first_chunk).ok_or_else(|| {
            DataProcessingError::InternalError(format!(
                "first_chunk {} out of bounds ({})",
                region.first_chunk,
                chunk_offsets.len()
            ))
        })?;
        let boundary_end = *chunk_offsets.get(region.last_chunk).ok_or_else(|| {
            DataProcessingError::InternalError(format!(
                "last_chunk {} out of bounds ({})",
                region.last_chunk,
                chunk_offsets.len()
            ))
        })?;
        debug_assert!(region.dirty_start >= boundary_start, "dirty_start before boundary_start");
        debug_assert!(region.dirty_end <= total_size, "dirty_end exceeds total_size");

        // The cleaner processes a "middle" file that spans [boundary_start, middle_end).
        // We stream it in three parts directly, without buffering boundary data:
        //
        //   a) Prefix:  CAS stream [boundary_start, dirty_start)  ← stable bytes before edit
        //   b) Dirty:   staging file [dirty_start, dirty_end)     ← modified bytes
        //   c) Suffix:  CAS stream [dirty_end, boundary_end)      ← stable bytes after edit
        //
        // Example: dirty region [200, 400), boundary [100, 500)
        //   a) CAS stream [100..200)   → cleaner
        //   b) staging    [200..400)   → cleaner (in 4MB blocks)
        //   c) CAS stream [400..500)   → cleaner
        let effective_boundary_end = boundary_end.min(total_size);
        let middle_end = effective_boundary_end.max(region.dirty_end).min(total_size);
        let middle_size = middle_end.saturating_sub(boundary_start);

        let mut cleaner = session.start_clean(None, Some(middle_size), Sha256Policy::Skip, Ulid::new()).await;

        // a) Boundary prefix: stable bytes before the dirty range.
        if region.dirty_start > boundary_start && boundary_end <= original_size {
            stream_cas_range(&cas_client, original_hash, boundary_start, region.dirty_start, &mut cleaner).await?;
        }

        // b) Dirty bytes from source, streamed in blocks.
        // TODO: seek/read_exact are blocking I/O in an async context. Acceptable for local
        // files (<1ms per 4MB block) but consider block_in_place for network-backed sources.
        if region.dirty_end > region.dirty_start {
            dirty_source.seek(SeekFrom::Start(region.dirty_start))?;
            let mut remaining = (region.dirty_end - region.dirty_start) as usize;
            let mut buf = vec![0u8; STREAM_BLOCK_SIZE.min(remaining)];
            while remaining > 0 {
                let to_read = buf.len().min(remaining);
                dirty_source.read_exact(&mut buf[..to_read])?;
                cleaner.add_data(&buf[..to_read]).await?;
                remaining -= to_read;
            }
        }

        // c) Boundary suffix: stable bytes after the dirty range.
        let suffix_start = region.dirty_end.min(effective_boundary_end);
        if suffix_start < effective_boundary_end && boundary_end <= original_size {
            stream_cas_range(&cas_client, original_hash, suffix_start, effective_boundary_end, &mut cleaner).await?;
        }

        let (info, chunks, _metrics) = cleaner.finish().await?;
        uploaded_regions.push(UploadedRegion { region, info, chunks });
    }

    // Checkpoint: flush xorbs without consuming the session, then retrieve MDBFileInfos.
    // TODO: the middle files are registered in the shard as real files, but nobody will
    // ever reference them. Check if GC cleans up unreferenced file entries, or find a way
    // to retrieve segments from the session without persisting them to the shard.
    session.checkpoint().await?;
    let middle_file_infos = session.file_info_list().await?;

    // Pair each uploaded region with its MDBFileInfo from the session.
    // Match by content hash. The shard manager deduplicates by file_hash (BTreeMap),
    // so two regions with identical content produce only ONE MDBFileInfo entry.
    // This is correct: same hash = same bytes = same chunks = same segments,
    // so we clone the same MDBFileInfo for all regions sharing that hash.
    let mdb_by_hash: HashMap<MerkleHash, MDBFileInfo> =
        middle_file_infos.into_iter().map(|mdb| (mdb.metadata.file_hash, mdb)).collect();

    let mut composed_regions: Vec<ComposedRegion> = Vec::with_capacity(uploaded_regions.len());
    for uploaded in uploaded_regions {
        let middle_hash = MerkleHash::from_hex(uploaded.info.hash())?;
        let mdb = mdb_by_hash.get(&middle_hash).cloned().ok_or_else(|| {
            DataProcessingError::InternalError(format!("no MDBFileInfo for middle hash {}", middle_hash.hex()))
        })?;
        composed_regions.push(ComposedRegion {
            region: uploaded.region,
            mdb,
            chunks: uploaded.chunks,
        });
    }

    // 6. Compose the final file: interleave stable regions with middle results.
    //
    // The final file is built by alternating:
    //   [Stable chunks] [Re-uploaded chunks] [Stable chunks] [Re-uploaded chunks] ...
    //
    // Example:
    //   Original: [chunk[0], chunk[1], chunk[2], chunk[3]]  (4 chunks)
    //   Dirty region affects chunks [1..3]
    //   Composition:
    //     [chunk[0]]  <-- stable, reuse from original
    //     [middle chunks for region]  <-- re-uploaded, from cleaner
    //     [chunk[3]]  <-- stable suffix, reuse from original
    //
    let mut all_chunks: Vec<(MerkleHash, u64)> = Vec::new();
    let mut all_segments: Vec<FileDataSequenceEntry> = Vec::new();
    let mut all_verification = Vec::new();
    let mut chunk_cursor = 0usize;
    let mut seg_cursor = 0usize;

    for composed in &composed_regions {
        // Stable region before this dirty region.
        if composed.region.first_chunk > chunk_cursor {
            let (segments, verification_hashes) = extract_segments(
                &original_mdb,
                &original_chunks,
                chunk_cursor,
                composed.region.first_chunk,
                &mut seg_cursor,
            );
            all_chunks.extend_from_slice(&original_chunks[chunk_cursor..composed.region.first_chunk]);
            all_segments.extend(segments);
            all_verification.extend(verification_hashes);
        }

        // Middle (dirty) region.
        all_chunks.extend_from_slice(&composed.chunks);
        all_segments.extend_from_slice(&composed.mdb.segments);
        all_verification.extend_from_slice(&composed.mdb.verification);

        chunk_cursor = composed.region.last_chunk;
    }

    // Stable suffix after the last dirty region.
    if chunk_cursor < compose_num_chunks {
        let (segments, verification_hashes) =
            extract_segments(&original_mdb, &original_chunks, chunk_cursor, compose_num_chunks, &mut seg_cursor);
        all_chunks.extend_from_slice(&original_chunks[chunk_cursor..compose_num_chunks]);
        all_segments.extend(segments);
        all_verification.extend(verification_hashes);
    }

    let combined_hash = file_hash(&all_chunks);

    debug!(
        "upload_ranges: composed hash={}, {} segments, {} dirty regions",
        combined_hash.hex(),
        all_segments.len(),
        composed_regions.len()
    );

    let composed_mdb = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(combined_hash, all_segments.len(), true, false),
        segments: all_segments,
        verification: all_verification,
        // SHA-256 metadata_ext is intentionally omitted: the file content changed
        // so the original SHA-256 is no longer valid, and recomputing it would require
        // reading the full file.
        metadata_ext: None,
    };

    // 7. Register composed file and finalize on the same session.
    session.register_composed_file(composed_mdb).await?;
    session.finalize().await?;

    let total_dirty: u64 = effective_ranges.iter().map(|(s, e)| e - s).sum();
    info!(
        "upload_ranges: hash={} size={} (original={}, {} dirty regions, {} dirty bytes)",
        combined_hash.hex(),
        total_size,
        original_size,
        composed_regions.len(),
        total_dirty
    );

    Ok(XetFileInfo::new(combined_hash.hex(), total_size))
}

/// Stream a byte range from CAS into the cleaner.
async fn stream_cas_range(
    cas_client: &Arc<dyn Client>,
    file_hash: MerkleHash,
    start: u64,
    end: u64,
    cleaner: &mut super::SingleFileCleaner,
) -> Result<()> {
    let reconstructor = FileReconstructor::new(cas_client, file_hash).with_byte_range(FileRange::new(start, end));
    let mut stream = reconstructor.reconstruct_to_stream();
    while let Some(chunk) = stream.next().await? {
        cleaner.add_data(&chunk).await?;
    }
    Ok(())
}

/// Expand dirty byte ranges to chunk-aligned boundaries and coalesce overlapping regions.
///
/// Each dirty range is mapped to the chunks it touches (since CAS chunks are atomic),
/// then adjacent/overlapping chunk ranges are merged to avoid uploading the same
/// boundary chunks twice.
fn build_dirty_regions(
    effective_ranges: &[(u64, u64)],
    chunk_offsets: &[u64],
    num_chunks: usize,
    original_size: u64,
    total_size: u64,
) -> Result<Vec<DirtyRegion>> {
    let mut raw = Vec::with_capacity(effective_ranges.len());
    for &(dirty_start, dirty_end) in effective_ranges {
        // Find the first chunk whose end offset exceeds dirty_start.
        let mut first_chunk = chunk_offsets[1..].partition_point(|&o| o <= dirty_start);
        debug_assert!(first_chunk <= num_chunks, "first_chunk {first_chunk} out of bounds ({num_chunks} chunks)");

        // For append regions (dirty_start >= original_size), include the last original
        // chunk so it gets re-chunked with the appended data. The last chunk was
        // terminated by EOF (not by the rolling hash), so its boundary is artificial.
        // The boundary prefix mechanism will download its bytes from CAS.
        if total_size > original_size && dirty_start >= original_size && first_chunk > 0 {
            first_chunk -= 1;
        }

        // Find the last chunk (exclusive) that starts before dirty_end.
        debug_assert!(dirty_end <= total_size, "dirty_end ({dirty_end}) exceeds total_size ({total_size})");
        let clamped_end = dirty_end.min(original_size);
        let last_chunk = (0..num_chunks)
            .rev()
            .find(|&i| chunk_offsets[i] < clamped_end)
            .map(|i| i + 1)
            .ok_or_else(|| {
                DataProcessingError::InternalError(format!(
                    "no chunk starts before clamped_end ({clamped_end}), chunks may be inconsistent"
                ))
            })?;
        raw.push(DirtyRegion {
            dirty_start,
            dirty_end,
            first_chunk,
            last_chunk,
        });
    }

    // Coalesce dirty regions whose chunk ranges overlap or are adjacent.
    // This prevents uploading the same boundary chunks twice.
    let mut merged: Vec<DirtyRegion> = Vec::with_capacity(raw.len());
    for region in raw {
        if let Some(last) = merged.last_mut()
            && region.first_chunk <= last.last_chunk
        {
            last.dirty_end = last.dirty_end.max(region.dirty_end);
            last.last_chunk = last.last_chunk.max(region.last_chunk);
            continue;
        }
        merged.push(region);
    }
    Ok(merged)
}

/// Merge `range` with the last element of `ranges` if they overlap or touch,
/// otherwise append it.
///
/// Three cases:
/// - **Overlap** (last.1 >= range.0): last and range share bytes
/// - **Touch** (last.1 == range.0): last ends exactly where range starts
/// - **Gap**: last ends before range starts, keep both separate
///
/// ```text
/// overlap:  last [=====]        touch:  last [=====]
///          range    [=====]            range       [=====]
///         result [=========]          result [===========]
///
/// gap:      last [=====]
///          range          [=====]
///         result [=====]  [=====]   (two separate entries)
/// ```
fn merge_or_push(ranges: &mut Vec<(u64, u64)>, range: (u64, u64)) {
    if let Some(last) = ranges.last_mut() {
        // Overlap or touch: merge by expanding last's bounds
        if last.1 >= range.0 {
            last.0 = last.0.min(range.0);
            last.1 = last.1.max(range.1);
            return;
        }
    }
    // Gap: append as separate entry
    ranges.push(range);
}

/// Extract segments and verification entries for chunks `[chunk_start, chunk_end)`
/// from the original reconstruction plan, truncating segments at boundaries.
///
/// `seg_cursor` tracks the current position in the segment list across calls. Pass
/// `&mut 0` on the first call; subsequent calls resume from where the last left off.
/// This avoids re-scanning segments from the beginning on each call (O(S) total
/// instead of O(K*S) for K calls).
fn extract_segments(
    original_mdb: &MDBFileInfo,
    original_chunks: &[(MerkleHash, u64)],
    chunk_start: usize,
    chunk_end: usize,
    seg_cursor: &mut usize,
) -> (Vec<FileDataSequenceEntry>, Vec<FileVerificationEntry>) {
    let mut segments = Vec::new();
    let mut verification = Vec::new();

    // Compute the chunk-level cursor from the segment cursor.
    let mut chunk_cursor: usize = original_mdb.segments[..*seg_cursor]
        .iter()
        .map(|s| (s.chunk_index_end - s.chunk_index_start) as usize)
        .sum();

    // Walk segments starting from seg_cursor, extracting the overlap with [chunk_start, chunk_end).
    //
    // Example: segments cover chunks [0,3), [3,7), [7,10). We want chunks [2, 8).
    //   seg[0]: covers [0,3), overlap with [2,8) = [2,3) -> truncate to 1 chunk
    //   seg[1]: covers [3,7), overlap with [2,8) = [3,7) -> keep whole segment
    //   seg[2]: covers [7,10), overlap with [2,8) = [7,8) -> truncate to 1 chunk
    for seg in &original_mdb.segments[*seg_cursor..] {
        let seg_count = (seg.chunk_index_end - seg.chunk_index_start) as usize;
        let seg_end = chunk_cursor + seg_count;

        if chunk_cursor >= chunk_end {
            break;
        }

        // Compute the overlap between this segment and the requested range.
        let overlap_start = chunk_cursor.max(chunk_start);
        let overlap_end = seg_end.min(chunk_end);
        if overlap_start < overlap_end {
            // Truncate the segment to only cover the overlapping chunks.
            let count = overlap_end - overlap_start;
            let mut truncated = seg.clone();
            truncated.chunk_index_start += (overlap_start - chunk_cursor) as u32;
            truncated.chunk_index_end = truncated.chunk_index_start + count as u32;
            let overlap = &original_chunks[overlap_start..overlap_end];
            let mut bytes = 0u64;
            let mut hashes = Vec::with_capacity(overlap.len());
            for &(hash, size) in overlap {
                bytes += size;
                hashes.push(hash);
            }
            // u32 cast: unpacked_segment_bytes is u32 in the shard format.
            // Safe because CDC parameters prevent segments from exceeding u32::MAX.
            truncated.unpacked_segment_bytes = bytes as u32;
            segments.push(truncated);

            // Recompute the verification hash for the truncated chunk range.
            verification.push(FileVerificationEntry::new(range_hash_from_chunks(&hashes)));
        }

        chunk_cursor = seg_end;
        // Only advance seg_cursor if this segment is fully consumed.
        // If it extends beyond chunk_end, a later call may need its suffix.
        if seg_end <= chunk_end {
            *seg_cursor += 1;
        }
    }

    (segments, verification)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use tempfile::TempDir;
    use ulid::Ulid;
    use xet_client::cas_client::{Client, LocalTestServerBuilder};
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;
    use crate::processing::configurations::TranslatorConfig;
    use crate::processing::file_cleaner::Sha256Policy;
    use crate::processing::file_download_session::FileDownloadSession;
    use crate::processing::file_upload_session::FileUploadSession;

    // original: [=========================== 256 KB ===========================]
    // dirty:                  [== 1 KB ===]
    // result:   [===stable===][re-uploaded][============stable=================]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_mid_file_edit() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let endpoint = server.http_endpoint().to_string();
        let config = Arc::new(TranslatorConfig::test_server_config(&endpoint, base_dir.path()).unwrap());

        // Use the server directly as the CAS client (bypasses HTTP for get_file_chunk_hashes).
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // 1. Upload an original file: 256 KB of pseudo-random bytes.
        let original_data = random_data(42, 256 * 1024);
        let original_hash = {
            let upload_session = FileUploadSession::new(config.clone(), None).await.unwrap();
            let mut cleaner = upload_session
                .start_clean(Some("original".into()), original_data.len() as u64, Sha256Policy::Skip, Ulid::new())
                .await;
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

        let mut dirty_source = Cursor::new(&modified_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(dirty_start as u64, dirty_end as u64)],
            &mut dirty_source,
            total_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size, total_size);

        // 3. Download and verify the composed file.
        let composed_hash = MerkleHash::from_hex(result.hash()).unwrap();
        let session = FileDownloadSession::new(config.clone(), None).await.unwrap();
        let file_info = crate::processing::XetFileInfo::new(composed_hash.hex(), total_size);
        let out_path = base_dir.path().join("output");
        session.download_file(&file_info, &out_path, Ulid::new()).await.unwrap();
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
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = random_data(43, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Truncate to 100 KB (no dirty ranges, pure truncation).
        let truncated_size = 100_000u64;
        let mut source = Cursor::new(&original_data[..truncated_size as usize]);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[], // no dirty ranges, just truncation
            &mut source,
            truncated_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), truncated_size);

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
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 100 KB file.
        let original_data = random_data(44, 100 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Append 50 KB of pseudo-random data.
        let mut full_data = original_data.clone();
        full_data.extend(random_data(99, 50 * 1024));
        let total_size = full_data.len() as u64;

        let mut source = Cursor::new(&full_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(original_size, total_size)], // appended region is dirty
            &mut source,
            total_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), total_size);

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
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = random_data(45, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Overwrite [0, 4096) with 0xBB (dirty range at offset 0, no stable prefix).
        let mut modified_data = original_data.clone();
        modified_data[..4096].fill(0xBB);
        let total_size = modified_data.len() as u64;

        let mut source = Cursor::new(&modified_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(0, 4096)],
            &mut source,
            total_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), total_size);

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
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
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

        let mut source = Cursor::new(&modified_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(10_000, 12_000), (200_000, 202_000)],
            &mut source,
            total_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), total_size);

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded.len(), modified_data.len());
        assert_eq!(downloaded, modified_data);

        let clean_hash = upload_file(&config, &modified_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [chunk0][chunk1][chunk2][chunk3][...]
    // dirty:            [0xBB ]        [0xBB ]
    //                   ^--- same content, same hash -> dedup collision
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_two_regions_identical_hash_collision() {
        // Two dirty regions that produce the same content (and thus the same hash)
        // must not collide in the mdb_by_hash mapping. The shard manager deduplicates
        // MDBFileInfo entries by file_hash, so both regions share the same entry.
        //
        // We use chunk-aligned dirty ranges with identical fill to guarantee
        // the cleaner produces identical hashes for both regions.
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Create an original file of pseudo-random bytes so CDC produces multiple chunks.
        // 512 KB of random data to reliably produce >= 4 CDC chunks.
        let original_data = random_data(47, 512 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let chunks = cas_client.get_file_chunk_hashes(&original_hash).await.unwrap();
        assert!(chunks.len() >= 4, "expected at least 4 chunks, got {}", chunks.len());

        let mut offsets = vec![0u64];
        for (_, size) in &chunks {
            offsets.push(offsets.last().unwrap() + size);
        }

        // Region 1: overwrite chunk[1] entirely. Region 2: overwrite chunk[3] entirely.
        // Both get the same 0xBB fill, and since each spans exactly one full chunk
        // boundary, the cleaner input is byte-identical -> same hash.
        let r1_start = offsets[1] as usize;
        let r1_end = offsets[2] as usize;
        let r2_start = offsets[3] as usize;
        let r2_end = offsets[4].min(original_size) as usize;

        let mut modified_data = original_data.clone();
        modified_data[r1_start..r1_end].fill(0xBB);
        modified_data[r2_start..r2_end].fill(0xBB);

        let mut source = Cursor::new(&modified_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(r1_start as u64, r1_end as u64), (r2_start as u64, r2_end as u64)],
            &mut source,
            modified_data.len() as u64,
        )
        .await
        .unwrap();

        let downloaded =
            download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), modified_data.len() as u64).await;
        assert_eq!(downloaded.len(), modified_data.len(), "downloaded length mismatch");
        assert_eq!(&downloaded[..], &modified_data[..], "content mismatch: file was corrupted");

        let clean_hash = upload_file(&config, &modified_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [chunk0][chunk1][chunk2][...]
    // result:   [chunk0]
    //                   ^ cut exactly on chunk boundary, no re-upload needed
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_truncation_on_chunk_boundary() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(99, 256 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        let chunks = cas_client.get_file_chunk_hashes(&original_hash).await.unwrap();
        assert!(chunks.len() >= 2, "need at least 2 chunks for this test");

        // Truncate exactly at the boundary after the first chunk.
        let truncated_size: u64 = chunks[0].1;
        let truncated_data = &original_data[..truncated_size as usize];

        let mut source = Cursor::new(truncated_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[],
            &mut source,
            truncated_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), truncated_size);

        let downloaded = download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), truncated_size).await;
        assert_eq!(&downloaded[..], truncated_data);

        let clean_hash = upload_file(&config, truncated_data).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }

    // original: [======== 100 KB ========]
    // staging:  [======== 100 KB ========][000][=4K written=]
    //                                     ^ gap (zeros from seek past EOF)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_append_with_gap_before_dirty_range() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let original_data = random_data(50, 100 * 1024);
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Simulate: seek to original_size + 500, write 4096 bytes of pseudo-random data.
        // The gap [original_size, original_size + 500) contains zeros from the sparse file.
        let gap = 500u64;
        let write_data = random_data(101, 4096);
        let write_len = write_data.len() as u64;
        let total_size = original_size + gap + write_len;

        let mut full_data = original_data.clone();
        full_data.extend(vec![0x00u8; gap as usize]); // sparse hole = zeros
        full_data.extend(&write_data);

        let dirty_start = original_size + gap;
        let dirty_end = total_size;

        let mut source = Cursor::new(&full_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(dirty_start, dirty_end)],
            &mut source,
            total_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), total_size);

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
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
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

        let mut source = Cursor::new(&sparse_staging);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(original_size, total_size)],
            &mut source,
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
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
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
            let chunks = cas_client.get_file_chunk_hashes(&original_hash).await.unwrap();
            if chunks.len() >= 3 {
                let boundary: u64 = chunks[0].1 + chunks[1].1;
                let dirty_end = boundary + chunks[2].1;
                let mut expected = original.clone();
                expected[boundary as usize..dirty_end as usize].fill(0xFF);
                let size = original.len() as u64;
                let mut source = Cursor::new(&expected);
                let result = upload_ranges(
                    config.clone(),
                    cas_client.clone(),
                    original_hash,
                    size,
                    &[(boundary, dirty_end)],
                    &mut source,
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
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(70, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;

        let mut source = Cursor::new(&data);
        let result = upload_ranges(config, cas_client, hash, size, &[], &mut source, size)
            .await
            .unwrap();

        assert_eq!(result.hash(), hash.hex());
        assert_eq!(result.file_size(), size);
    }

    // dirty_range end > total_size -> rejected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_dirty_range_past_total_size() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(71, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;

        let mut source = Cursor::new(&data);
        let err = upload_ranges(config, cas_client, hash, size, &[(100, size + 1)], &mut source, size).await;
        assert!(err.is_err(), "dirty range past total_size should be rejected");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_overlapping_dirty_ranges() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(60, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;

        let mut source = Cursor::new(&data);
        let err = upload_ranges(config, cas_client, hash, size, &[(100, 300), (200, 400)], &mut source, size).await;
        assert!(err.is_err(), "overlapping ranges should be rejected");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_empty_dirty_range() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(61, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;

        let mut source = Cursor::new(&data);
        let err = upload_ranges(config, cas_client, hash, size, &[(100, 100)], &mut source, size).await;
        assert!(err.is_err(), "empty range (start == end) should be rejected");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rejects_unsorted_dirty_ranges() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        let data = random_data(62, 256 * 1024);
        let hash = upload_file(&config, &data).await;
        let size = data.len() as u64;

        let mut source = Cursor::new(&data);
        let err = upload_ranges(config, cas_client, hash, size, &[(300, 400), (100, 200)], &mut source, size).await;
        assert!(err.is_err(), "unsorted ranges should be rejected");
    }

    #[test]
    fn test_build_dirty_regions_coalesces_adjacent() {
        // 5 chunks of 100 bytes each: offsets [0, 100, 200, 300, 400, 500]
        let chunk_offsets = vec![0u64, 100, 200, 300, 400, 500];
        let num_chunks = 5;
        let original_size = 500;
        let total_size = 500;

        // Three adjacent dirty ranges, all inside chunk[2] = [200, 300).
        let ranges = vec![(210u64, 230), (230, 250), (250, 270)];
        let regions = build_dirty_regions(&ranges, &chunk_offsets, num_chunks, original_size, total_size).unwrap();

        // All three touch the same chunk, so they must coalesce into one region.
        assert_eq!(regions.len(), 1, "expected 1 coalesced region, got {}", regions.len());
        assert_eq!(regions[0].dirty_start, 210);
        assert_eq!(regions[0].dirty_end, 270);
    }

    #[test]
    fn test_build_dirty_regions_no_coalesce_when_separated() {
        // 5 chunks of 100 bytes each.
        let chunk_offsets = vec![0u64, 100, 200, 300, 400, 500];
        let num_chunks = 5;
        let original_size = 500;
        let total_size = 500;

        // Two dirty ranges in non-adjacent chunks: chunk[1] and chunk[3].
        let ranges = vec![(110u64, 130), (310, 330)];
        let regions = build_dirty_regions(&ranges, &chunk_offsets, num_chunks, original_size, total_size).unwrap();

        assert_eq!(regions.len(), 2, "expected 2 separate regions, got {}", regions.len());
    }

    #[test]
    fn test_build_dirty_regions_rejects_inconsistent_chunks() {
        // chunk_offsets = [0, 100] but dirty range ends at 200 (clamped to original_size=100).
        // No chunk has start < 100 except chunk[0] at offset 0... actually chunk[0]
        // starts at 0 < 100, so that works. Use an empty chunk list instead.
        let chunk_offsets = vec![0u64]; // 0 chunks, only the initial offset
        let num_chunks = 0;
        let original_size = 0;
        let total_size = 100;

        let ranges = vec![(0u64, 100)];
        let result = build_dirty_regions(&ranges, &chunk_offsets, num_chunks, original_size, total_size);
        assert!(result.is_err(), "should fail with inconsistent/empty chunk data");
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
        let session = FileUploadSession::new(config.clone(), None).await.unwrap();
        let mut cleaner = session
            .start_clean(Some("test".into()), data.len() as u64, Sha256Policy::Skip, Ulid::new())
            .await;
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
        session.download_file(&xfi, &out, Ulid::new()).await.unwrap();
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
        let mut source = Cursor::new(expected);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_data.len() as u64,
            dirty_ranges,
            &mut source,
            total_size,
        )
        .await
        .unwrap();

        assert_eq!(result.file_size(), total_size, "file size mismatch");
        let downloaded = download_file(config, MerkleHash::from_hex(result.hash()).unwrap(), total_size).await;
        assert_eq!(downloaded.len(), expected.len(), "downloaded length mismatch");
        assert_eq!(&downloaded[..], &expected[..], "content mismatch");

        let clean_hash = upload_file(config, expected).await;
        assert_eq!(result.hash(), clean_hash.hex(), "hash mismatch with clean upload");
    }
}
