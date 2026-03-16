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

/// Upload modified ranges of an existing file, composing the result with
/// the original file's CAS segments. Only the dirty regions (plus CDC boundary
/// chunks) are re-uploaded; stable regions between and around dirty ranges are
/// reused from the original file's reconstruction plan.
///
/// # Arguments
///
/// * `config` - Translator configuration for creating upload sessions.
/// * `cas_client` - CAS client for fetching original file metadata and downloading boundary chunks.
/// * `original_hash` - Merkle hash of the original file in CAS.
/// * `original_size` - Size of the original file in bytes.
/// * `dirty_ranges` - Sorted, non-overlapping `(start, end)` byte ranges that were modified.
/// * `dirty_source` - Seekable reader for the dirty bytes (e.g. a staging file).
/// * `total_size` - Total size of the modified file (may be larger than `original_size` for appends).
pub async fn upload_ranges(
    config: Arc<TranslatorConfig>,
    cas_client: Arc<dyn Client>,
    original_hash: MerkleHash,
    original_size: u64,
    dirty_ranges: &[(u64, u64)],
    dirty_source: &mut dyn ReadSeek,
    total_size: u64,
) -> Result<XetFileInfo> {
    // Precondition: dirty_ranges must be sorted, non-overlapping, and non-empty intervals.
    debug_assert!(
        dirty_ranges.windows(2).all(|w| w[0].1 <= w[1].0),
        "dirty_ranges must be sorted and non-overlapping, got: {dirty_ranges:?}"
    );
    debug_assert!(
        dirty_ranges.iter().all(|&(s, e)| s < e),
        "dirty_ranges must be non-empty intervals, got: {dirty_ranges:?}"
    );

    if dirty_ranges.is_empty() && total_size == original_size {
        return Ok(XetFileInfo::new(original_hash.hex(), original_size));
    }

    // 1. Fetch chunk hashes and reconstruction info in parallel.
    let (original_chunks, recon_result) = tokio::try_join!(
        cas_client.get_file_chunk_hashes(&original_hash),
        cas_client.get_file_reconstruction_info(&original_hash),
    )?;

    let (original_mdb, _) = recon_result
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
    chunk_offsets.push(0);
    for (_, size) in &original_chunks {
        chunk_offsets.push(chunk_offsets.last().unwrap() + size);
    }

    // 3. Build effective dirty ranges: start from caller's ranges, then handle truncation/append.
    let mut effective_ranges: Vec<(u64, u64)> = dirty_ranges.to_vec();

    // For truncation: the boundary chunk at the cut point must be re-uploaded as a partial chunk.
    // Find the last full chunk before total_size and add [last_full_chunk_end, total_size) as dirty.
    //
    // Scenario: file truncated from 450 bytes to 250 bytes.
    // Original chunks: [100, 200, 150]  -->  chunk_offsets = [0, 100, 300, 450]
    //                   ^     ^     ^                            ^   ^    ^    ^
    //
    //   total_size = 250 falls inside chunk[1] (which spans [100, 300))
    //
    //   last_full = rposition of offsets <= 250
    //             = index 1 (because chunk_offsets[1] = 100 <= 250)
    //   boundary = chunk_offsets[1] = 100
    //
    //   Since boundary (100) < total_size (250), we have a partial chunk:
    //     trunc_range = (100, 250)  <-- this part of chunk[1] must be re-uploaded
    //
    //   Then we truncate:
    //     original_chunks keeps [chunk[0], chunk[1]] and discards chunk[2]
    //     chunk_offsets becomes [0, 100, 300]  (stops at chunk[1]'s end)
    //
    // Visual before:
    //   file:   [====chunk[0]====][========chunk[1]========][====chunk[2]====]
    //   bytes:  0               100                       300               450
    //
    // Visual after (with truncation to 250):
    //   dirty:             [== re-upload ==]
    //   stable:            [====chunk[0]====][stable part]
    //   bytes:  0               100           250
    if total_size < original_size {
        let last_full = chunk_offsets.iter().rposition(|&o| o <= total_size).unwrap_or(0);
        let boundary = chunk_offsets[last_full];
        if boundary < total_size {
            // Partial chunk [boundary, total_size) needs re-upload from the dirty source.
            let trunc_range = (boundary, total_size);
            // Merge with last dirty range if they overlap/touch.
            if let Some(last) = effective_ranges.last_mut() {
                if last.1 >= boundary {
                    last.1 = last.1.max(total_size);
                } else {
                    effective_ranges.push(trunc_range);
                }
            } else {
                effective_ranges.push(trunc_range);
            }
        }
        // Remember the truncation point for composition (step 6).
        // We keep chunk_offsets and original_chunks unmodified for boundary downloads.
    }
    // If the file grew, the region beyond original_size is always dirty.
    // Scenario: file grew from 450 bytes to 550 bytes
    //
    //   Before:
    //   file:   [====chunk[0]====][========chunk[1]========][====chunk[2]====]
    //   bytes:  0               100                       300               450
    //
    //   After (append 100 bytes):
    //   file:   [====chunk[0]====][========chunk[1]========][====chunk[2]====][====NEW====]
    //   bytes:  0               100                       300               450           550
    //                                                                        ^             ^
    //                                                                        append_start  total_size
    //
    //   The region [450, 550) is added to effective_ranges for re-upload.
    if total_size > original_size {
        let append_start = original_size;
        // Merge with last dirty range if they touch, otherwise add a new range.
        if let Some(last) = effective_ranges.last_mut() {
            if last.1 >= append_start {
                last.1 = last.1.max(total_size);
            } else {
                effective_ranges.push((append_start, total_size));
            }
        } else {
            effective_ranges.push((append_start, total_size));
        }
    }

    let num_chunks = original_chunks.len();
    // For truncation, limit the composition to chunks that fit within total_size.
    let compose_num_chunks = if total_size < original_size {
        chunk_offsets.iter().rposition(|&o| o <= total_size).unwrap_or(0)
    } else {
        num_chunks
    };

    // Note: if effective_ranges is empty here, it means pure truncation (no dirty ranges,
    // file shrunk). We still proceed to compose a new file from the truncated chunk set.

    // 4. Build the composition plan: alternating stable/dirty regions. A Region is either Stable (reuse segments) or
    //    Dirty (re-upload through cleaner).
    //
    // For each dirty byte range, find which chunks it spans. We need this mapping to decide
    // which chunks to re-upload and which to reuse from the original file.
    /// A dirty byte range expanded to chunk-aligned boundaries.
    struct DirtyRegion {
        dirty_start: u64,   // byte offset where this dirty region starts
        dirty_end: u64,     // byte offset where this dirty region ends
        first_chunk: usize, // first chunk index affected (inclusive)
        last_chunk: usize,  // last chunk index affected (exclusive)
    }

    /// Result of uploading a single dirty region through the cleaner.
    struct UploadedRegion {
        region: DirtyRegion,
        info: XetFileInfo,
        chunks: ChunkHashList,
    }

    // Example: dirty range [150, 350), chunks [0..3] with offsets [0, 100, 300, 450]
    //   dirty_start = 150, dirty_end = 350
    //   150 falls in chunk[1] (offset 100..300), so first_chunk = 1
    //   350 falls in chunk[2] (offset 300..450), so last_chunk = 3 (exclusive)
    //   This means chunks [1, 2] must be re-uploaded (the region spans these chunks)
    let dirty_regions = {
        let mut raw = Vec::with_capacity(effective_ranges.len());
        for &(dirty_start, dirty_end) in &effective_ranges {
            // Find the first chunk whose end offset exceeds dirty_start.
            let first_chunk = chunk_offsets[1..].partition_point(|&o| o <= dirty_start).min(num_chunks);
            // Find the last chunk (exclusive) that starts before dirty_end.
            let last_chunk = (0..num_chunks)
                .rev()
                .find(|&i| chunk_offsets[i] < dirty_end.min(total_size).min(original_size))
                .map(|i| i + 1)
                .unwrap_or(first_chunk);
            raw.push(DirtyRegion {
                dirty_start,
                dirty_end,
                first_chunk,
                last_chunk,
            });
        }

        // Coalesce dirty regions whose chunk ranges overlap or are adjacent.
        // This prevents uploading the same boundary chunks twice.
        //
        // Scenario: two dirty ranges that span overlapping chunks:
        //   Region 1: [150, 250), chunks [1..2]
        //   Region 2: [250, 350), chunks [1..3]
        //   After merging: [150, 350), chunks [1..3]  (upload once, not twice)
        let mut merged: Vec<DirtyRegion> = Vec::with_capacity(raw.len());
        for region in raw {
            if let Some(last) = merged.last_mut()
                && region.first_chunk <= last.last_chunk
            {
                // Overlap detected: extend the last region to cover both
                last.dirty_end = last.dirty_end.max(region.dirty_end);
                last.last_chunk = last.last_chunk.max(region.last_chunk);
                continue;
            }
            merged.push(region);
        }
        merged
    };

    // 5. Process each dirty region: download boundary, stream dirty bytes, upload. Collect the resulting middle file
    //    infos and chunk hashes. A single upload session is shared across all dirty regions.
    let session = FileUploadSession::new(config.clone(), None).await?;

    let mut uploaded_regions: Vec<UploadedRegion> = Vec::new();

    for region in dirty_regions {
        let boundary_start = chunk_offsets.get(region.first_chunk).copied().unwrap_or(original_size);
        let boundary_end = chunk_offsets.get(region.last_chunk).copied().unwrap_or(original_size);

        // Download boundary bytes from CAS into memory.
        //
        // NOTE: FileReconstructor is heavier than needed for small boundary downloads
        // (~256KB): it spawns prefetch tasks, manages buffer semaphores, etc. A direct
        // byte-range fetch via presigned URLs would be lighter but would require
        // reimplementing xorb decompression and chunk extraction. Acceptable for now
        // since boundary regions are typically 1-2 CDC chunks.
        //
        // NOTE: boundary_data is fully buffered before being fed to the cleaner. Streaming
        // it directly (CAS async stream -> cleaner) would avoid the buffer, but requires
        // interleaving async CAS reads with sync dirty_source reads in a single ordered
        // stream, which adds significant complexity. The buffer is bounded by the boundary
        // size (typically a few hundred KB), so memory impact is negligible.
        let mut boundary_data = Vec::new();
        if boundary_start < boundary_end && boundary_end <= original_size {
            let reconstructor = FileReconstructor::new(&cas_client, original_hash)
                .with_byte_range(FileRange::new(boundary_start, boundary_end));
            let mut stream = reconstructor.reconstruct_to_stream();
            while let Some(chunk) = stream.next().await? {
                boundary_data.extend_from_slice(&chunk);
            }
            debug!(
                "upload_ranges: downloaded boundary ({} bytes) for dirty [{}, {})",
                boundary_data.len(),
                region.dirty_start,
                region.dirty_end
            );
        }

        // Stream boundary prefix + dirty bytes + boundary suffix into the cleaner.
        // middle_end must cover both the boundary region and the dirty bytes
        // (dirty_end may extend past boundary_end for truncation or appends).
        let middle_end = boundary_end.max(region.dirty_end);
        let middle_size = middle_end.saturating_sub(boundary_start);

        // The cleaner processes a "middle" file that spans [boundary_start, middle_end).
        // We feed it in three parts:
        //   a) Prefix:  stable bytes from the downloaded boundary
        //   b) Dirty:   modified bytes from dirty_source
        //   c) Suffix:  stable bytes from the downloaded boundary
        //
        // Example: dirty region [200, 400), boundary [100, 500) (5 chunks spanning this)
        //   Downloaded boundary_data = bytes [100..500) from CAS
        //   We feed the cleaner:
        //     [100..200)  from boundary_data  (prefix, not modified)
        //     [200..400)  from dirty_source   (modified by caller)
        //     [400..500)  from boundary_data  (suffix, not modified)
        //   The cleaner runs CDC + compression on this [100, 500) stream and produces new chunks.
        let mut cleaner = session.start_clean(None, middle_size, Sha256Policy::Skip, Ulid::new()).await;

        // a) Boundary bytes BEFORE the dirty range.
        // If the dirty range doesn't start at boundary_start, we need to include the stable prefix.
        let pre_dirty_end = region.dirty_start.max(boundary_start);
        if pre_dirty_end > boundary_start {
            let len = (pre_dirty_end - boundary_start) as usize;
            debug_assert!(
                len <= boundary_data.len(),
                "boundary prefix ({len} bytes) exceeds downloaded boundary data ({} bytes)",
                boundary_data.len()
            );
            cleaner.add_data(&boundary_data[..len.min(boundary_data.len())]).await?;
        }

        // b) Dirty bytes from source, streamed in blocks.
        // Read the modified bytes from [read_start, read_end) and feed them to the cleaner.
        // We stream this in STREAM_BLOCK_SIZE chunks to avoid buffering the entire dirty region.
        let read_start = region.dirty_start.max(boundary_start);
        let read_end = region.dirty_end.min(total_size);
        if read_end > read_start {
            dirty_source.seek(SeekFrom::Start(read_start))?;
            let mut remaining = (read_end - read_start) as usize;
            let mut buf = vec![0u8; STREAM_BLOCK_SIZE.min(remaining)];
            while remaining > 0 {
                let to_read = buf.len().min(remaining);
                dirty_source.read_exact(&mut buf[..to_read])?;
                cleaner.add_data(&buf[..to_read]).await?;
                remaining -= to_read;
            }
        }

        // c) Boundary bytes AFTER the dirty range.
        // If the dirty range doesn't extend to boundary_end, include the stable suffix.
        let post_dirty_start = region.dirty_end.min(boundary_end);
        if post_dirty_start < boundary_end {
            let offset = (post_dirty_start - boundary_start) as usize;
            debug_assert!(
                offset < boundary_data.len(),
                "boundary suffix offset ({offset}) out of range ({} bytes)",
                boundary_data.len()
            );
            if offset < boundary_data.len() {
                cleaner.add_data(&boundary_data[offset..]).await?;
            }
        }

        let (info, chunks, _metrics) = cleaner.finish().await?;
        uploaded_regions.push(UploadedRegion { region, info, chunks });
    }

    // Checkpoint: flush xorbs without consuming the session, then retrieve MDBFileInfos.
    session.checkpoint().await?;
    let middle_file_infos = session.file_info_list().await?;

    // Pair each uploaded region with its MDBFileInfo from the session.
    // Match by content hash. Note: if two regions produce identical content after CDC,
    // they will have the same hash. We use a Vec to handle this, matching them in order
    // of their file info discovery to preserve uploaded_regions order.
    let mut mdb_by_hash: HashMap<MerkleHash, Vec<MDBFileInfo>> = HashMap::new();
    for mdb in middle_file_infos {
        mdb_by_hash.entry(mdb.metadata.file_hash).or_default().push(mdb);
    }

    struct ComposedRegion {
        region: DirtyRegion,
        mdb: MDBFileInfo,
        chunks: ChunkHashList,
    }

    let mut composed_regions: Vec<ComposedRegion> = Vec::new();
    for uploaded in uploaded_regions {
        let middle_hash = MerkleHash::from_hex(uploaded.info.hash())?;
        let mdb_list = mdb_by_hash.get_mut(&middle_hash).ok_or_else(|| {
            DataProcessingError::InternalError(format!("no MDBFileInfo for middle hash {}", middle_hash.hex()))
        })?;
        let mdb = mdb_list.remove(0);
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
    let mut chunk_cursor = 0usize; // current chunk position in the original file
    let mut seg_cursor = 0usize; // current segment position (passed to extract_segments for O(S) total)

    for cr in &composed_regions {
        // Stable region before this dirty region: chunks [chunk_cursor, first_chunk).
        // These chunks are reused directly from the original file's segments.
        if cr.region.first_chunk > chunk_cursor {
            let (segs, vers) =
                extract_segments(&original_mdb, &original_chunks, chunk_cursor, cr.region.first_chunk, &mut seg_cursor);
            all_chunks.extend_from_slice(&original_chunks[chunk_cursor..cr.region.first_chunk]);
            all_segments.extend(segs);
            all_verification.extend(vers);
        }

        // Middle (dirty) region: these chunks were re-uploaded and cleaner'd by the session.
        // We insert the new chunks and segments from the cleaner output.
        all_chunks.extend_from_slice(&cr.chunks);
        all_segments.extend_from_slice(&cr.mdb.segments);
        all_verification.extend_from_slice(&cr.mdb.verification);

        chunk_cursor = cr.region.last_chunk;
    }

    // Stable suffix after the last dirty region.
    // If there are chunks after the last dirty region, reuse them from the original.
    if chunk_cursor < compose_num_chunks {
        let (segs, vers) =
            extract_segments(&original_mdb, &original_chunks, chunk_cursor, compose_num_chunks, &mut seg_cursor);
        all_chunks.extend_from_slice(&original_chunks[chunk_cursor..compose_num_chunks]);
        all_segments.extend(segs);
        all_verification.extend(vers);
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

    for seg in &original_mdb.segments[*seg_cursor..] {
        let seg_count = (seg.chunk_index_end - seg.chunk_index_start) as usize;
        let seg_end = chunk_cursor + seg_count;

        // Past the requested range: stop.
        if chunk_cursor >= chunk_end {
            break;
        }

        let overlap_start = chunk_cursor.max(chunk_start);
        let overlap_end = seg_end.min(chunk_end);
        if overlap_start < overlap_end {
            let count = overlap_end - overlap_start;
            let mut truncated = seg.clone();
            truncated.chunk_index_start += (overlap_start - chunk_cursor) as u32;
            truncated.chunk_index_end = truncated.chunk_index_start + count as u32;
            let bytes: u64 = original_chunks[overlap_start..overlap_end].iter().map(|(_, s)| s).sum();
            truncated.unpacked_segment_bytes = bytes as u32;
            segments.push(truncated);

            let hashes: Vec<MerkleHash> = original_chunks[overlap_start..overlap_end].iter().map(|(h, _)| *h).collect();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_mid_file_edit() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let endpoint = server.http_endpoint().to_string();
        let config = Arc::new(TranslatorConfig::test_server_config(&endpoint, base_dir.path()).unwrap());

        // Use the server directly as the CAS client (bypasses HTTP for get_file_chunk_hashes).
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // 1. Upload an original file: 256 KB of 0xAA bytes.
        let original_data = vec![0xAAu8; 256 * 1024];
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
        let xfi = crate::processing::XetFileInfo::new(composed_hash.hex(), total_size);
        let out_path = base_dir.path().join("output");
        session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();
        let downloaded = std::fs::read(&out_path).unwrap();

        assert_eq!(downloaded.len(), modified_data.len());
        assert_eq!(downloaded, modified_data);
    }

    /// Helper to upload a file and return its hash.
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

    /// Helper to download a file and return its contents.
    async fn download_file(config: &Arc<TranslatorConfig>, hash: MerkleHash, size: u64) -> Vec<u8> {
        let session = FileDownloadSession::new(config.clone(), None).await.unwrap();
        let xfi = crate::processing::XetFileInfo::new(hash.hex(), size);
        let dir = TempDir::new().unwrap();
        let out = dir.path().join("out");
        session.download_file(&xfi, &out, Ulid::new()).await.unwrap();
        std::fs::read(&out).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_truncation() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = vec![0xCCu8; 256 * 1024];
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_append() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 100 KB file.
        let original_data = vec![0xDDu8; 100 * 1024];
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Append 50 KB of 0xEE.
        let mut full_data = original_data.clone();
        full_data.extend(vec![0xEEu8; 50 * 1024]);
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_at_file_start() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = vec![0xAAu8; 256 * 1024];
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_ranges_multiple_regions() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Upload 256 KB file.
        let original_data = vec![0xAAu8; 256 * 1024];
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
    }

    // ── Data integrity regression tests ──────────────────────────────
    //
    // All corruption scenarios share a single LocalTestServer to keep
    // test runtime reasonable (~2s total instead of ~1s per scenario).

    /// Helper: upload original, apply modifications via upload_ranges, download composed, verify.
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_two_regions_identical_hash_collision() {
        // Regression test: Two dirty regions that produce the same content (and thus the same hash)
        // must not collide in the mdb_by_hash mapping. Before the fix, the second region would
        // incorrectly use the MDBFileInfo from the first region, causing silent corruption.
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // Create an original file: 300 KB of 0xAA.
        let original_data = vec![0xAAu8; 300 * 1024];
        let original_hash = upload_file(&config, &original_data).await;
        let original_size = original_data.len() as u64;

        // Create modified data with two identical "dirty" regions:
        //   Region 1: bytes [50_000, 60_000) filled with 0xBB
        //   Region 2: bytes [150_000, 160_000) also filled with 0xBB
        // If both regions produce identical CDC chunks, they will have the same hash.
        // The bug would cause the second region to incorrectly use Region 1's MDBFileInfo.
        let mut modified_data = original_data.clone();
        modified_data[50_000..60_000].fill(0xBB);
        modified_data[150_000..160_000].fill(0xBB);

        let mut source = Cursor::new(&modified_data);
        let result = upload_ranges(
            config.clone(),
            cas_client.clone(),
            original_hash,
            original_size,
            &[(50_000, 60_000), (150_000, 160_000)],
            &mut source,
            modified_data.len() as u64,
        )
        .await
        .unwrap();

        // Verify the composed file matches our expected modifications.
        let downloaded =
            download_file(&config, MerkleHash::from_hex(result.hash()).unwrap(), modified_data.len() as u64).await;
        assert_eq!(downloaded.len(), modified_data.len(), "downloaded length mismatch");
        assert_eq!(&downloaded[..], &modified_data[..], "content mismatch: file was corrupted");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_data_integrity_scenarios() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        let cas_client: Arc<dyn Client> = Arc::new(server);

        // ── Truncation + overlapping dirty range ────────────────────
        {
            let original = vec![0xAAu8; 256 * 1024];
            let mut expected = original[..100_000].to_vec();
            expected[90_000..100_000].fill(0xBB);
            assert_range_edit(&config, &cas_client, &original, &expected, &[(90_000, 100_000)], 100_000).await;
        }

        // ── Full overwrite (no stable prefix or suffix) ─────────────
        {
            let original = vec![0xAAu8; 128 * 1024];
            let expected = vec![0xBBu8; 128 * 1024];
            let size = original.len() as u64;
            assert_range_edit(&config, &cas_client, &original, &expected, &[(0, size)], size).await;
        }

        // ── Three adjacent dirty ranges (coalescing) ────────────────
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

        // ── Append without explicit dirty range ─────────────────────
        {
            let original = vec![0xAAu8; 100 * 1024];
            let mut expected = original.clone();
            expected.extend(vec![0xEEu8; 50 * 1024]);
            let total = expected.len() as u64;
            assert_range_edit(&config, &cas_client, &original, &expected, &[], total).await;
        }

        // ── Dirty range exactly on chunk boundary ───────────────────
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
            }
        }
    }
}
