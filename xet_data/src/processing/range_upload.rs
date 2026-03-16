use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use tracing::{debug, info};
use ulid::Ulid;
use xet_client::cas_client::Client;

/// Trait alias for a seekable byte source (e.g. `std::fs::File`).
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}
use xet_client::cas_types::FileRange;
use xet_core_structures::merklehash::{MerkleHash, file_hash};
use xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks;
use xet_core_structures::metadata_shard::file_structs::{FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo};

use super::XetFileInfo;
use super::configurations::TranslatorConfig;
use super::errors::{DataProcessingError, Result};
use super::file_cleaner::Sha256Policy;
use super::file_upload_session::FileUploadSession;
use crate::file_reconstruction::FileReconstructor;

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
    if dirty_ranges.is_empty() && total_size <= original_size {
        return Ok(XetFileInfo::new(original_hash.hex(), original_size));
    }

    // 1. Fetch chunk hashes and reconstruction info in parallel.
    let (original_chunks, recon_result) = tokio::try_join!(
        async {
            cas_client
                .get_file_chunk_hashes(&original_hash)
                .await
                .map_err(|e| DataProcessingError::InternalError(format!("get_file_chunk_hashes: {e}")))
        },
        async {
            cas_client
                .get_file_reconstruction_info(&original_hash)
                .await
                .map_err(|e| DataProcessingError::InternalError(format!("get_file_reconstruction_info: {e}")))
        },
    )?;

    let (original_mdb, _) = recon_result
        .ok_or_else(|| DataProcessingError::InternalError("no reconstruction info for original file".into()))?;

    // 2. Map chunks to cumulative byte offsets.
    let num_chunks = original_chunks.len();
    let mut chunk_offsets: Vec<u64> = Vec::with_capacity(num_chunks + 1);
    chunk_offsets.push(0);
    for (_, size) in &original_chunks {
        chunk_offsets.push(chunk_offsets.last().unwrap() + size);
    }

    // 3. Expand dirty ranges to include appended bytes and find affected chunk regions. Each dirty range becomes a
    //    DirtyRegion with chunk-aligned boundaries.
    let mut effective_ranges: Vec<(u64, u64)> = dirty_ranges.to_vec();
    // If the file grew, the region beyond original_size is always dirty.
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

    if effective_ranges.is_empty() {
        return Ok(XetFileInfo::new(original_hash.hex(), original_size));
    }

    // 4. Build the composition plan: alternating stable/dirty regions. A Region is either Stable (reuse segments) or
    //    Dirty (re-upload through cleaner).
    struct DirtyRegion {
        dirty_start: u64,
        dirty_end: u64,
        first_chunk: usize, // first chunk index affected (inclusive)
        last_chunk: usize,  // last chunk index affected (exclusive)
    }

    let mut dirty_regions = Vec::with_capacity(effective_ranges.len());
    for &(ds, de) in &effective_ranges {
        let first = (0..num_chunks).find(|&i| chunk_offsets[i + 1] > ds).unwrap_or(num_chunks);
        let last = (0..num_chunks)
            .rev()
            .find(|&i| chunk_offsets[i] < de.min(original_size))
            .map(|i| i + 1)
            .unwrap_or(first);
        dirty_regions.push(DirtyRegion {
            dirty_start: ds,
            dirty_end: de,
            first_chunk: first,
            last_chunk: last,
        });
    }

    // 5. Process each dirty region: download boundary, stream dirty bytes, upload. Collect the resulting middle file
    //    infos and chunk hashes.
    struct MiddleResult {
        mdb: MDBFileInfo,
        chunks: Vec<(MerkleHash, u64)>,
    }

    let mut middle_results: Vec<(DirtyRegion, MiddleResult)> = Vec::new();

    for region in dirty_regions {
        let boundary_start = chunk_offsets.get(region.first_chunk).copied().unwrap_or(original_size);
        let boundary_end = chunk_offsets.get(region.last_chunk).copied().unwrap_or(original_size);

        // Download boundary bytes from CAS.
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
        let middle_end = if region.dirty_end > original_size {
            boundary_end.max(region.dirty_end)
        } else {
            boundary_end
        };
        let middle_size = middle_end.saturating_sub(boundary_start);

        let session = FileUploadSession::new(config.clone(), None).await?;
        let mut cleaner = session
            .start_clean(None, Some(middle_size), Sha256Policy::Skip, Ulid::new())
            .await;

        // a) Boundary bytes BEFORE the dirty range.
        let pre_dirty_end = region.dirty_start.max(boundary_start);
        if pre_dirty_end > boundary_start {
            let len = (pre_dirty_end - boundary_start) as usize;
            if len <= boundary_data.len() {
                cleaner.add_data(&boundary_data[..len]).await?;
            }
        }

        // b) Dirty bytes from source, streamed in blocks.
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
        let post_dirty_start = region.dirty_end.min(boundary_end);
        if post_dirty_start < boundary_end {
            let offset = (post_dirty_start - boundary_start) as usize;
            if offset < boundary_data.len() {
                cleaner.add_data(&boundary_data[offset..]).await?;
            }
        }

        let (middle_info, _metrics) = cleaner.finish().await?;
        let (_metrics, middle_file_infos) = session.finalize_with_file_info().await?;
        let middle_mdb = middle_file_infos
            .into_iter()
            .next()
            .ok_or_else(|| DataProcessingError::InternalError("finalize returned no file info".into()))?;

        let middle_hash = MerkleHash::from_hex(middle_info.hash())?;
        let middle_chunks = cas_client
            .get_file_chunk_hashes(&middle_hash)
            .await
            .map_err(|e| DataProcessingError::InternalError(format!("get middle chunk hashes: {e}")))?;

        middle_results.push((
            region,
            MiddleResult {
                mdb: middle_mdb,
                chunks: middle_chunks,
            },
        ));
    }

    // 6. Compose the final file: interleave stable regions with middle results.
    let mut all_chunks: Vec<(MerkleHash, u64)> = Vec::new();
    let mut all_segments = Vec::new();
    let mut all_verification = Vec::new();
    let mut cursor = 0usize; // current chunk position in the original file

    for (region, middle) in &middle_results {
        // Stable region before this dirty region: chunks [cursor, first_chunk).
        if region.first_chunk > cursor {
            let (segs, vers) = extract_segments(&original_mdb, &original_chunks, cursor, region.first_chunk);
            all_chunks.extend_from_slice(&original_chunks[cursor..region.first_chunk]);
            all_segments.extend(segs);
            all_verification.extend(vers);
        }

        // Middle (dirty) region.
        all_chunks.extend_from_slice(&middle.chunks);
        all_segments.extend_from_slice(&middle.mdb.segments);
        all_verification.extend_from_slice(&middle.mdb.verification);

        cursor = region.last_chunk;
    }

    // Stable suffix after the last dirty region.
    if cursor < num_chunks {
        let (segs, vers) = extract_segments(&original_mdb, &original_chunks, cursor, num_chunks);
        all_chunks.extend_from_slice(&original_chunks[cursor..]);
        all_segments.extend(segs);
        all_verification.extend(vers);
    }

    let combined_hash = file_hash(&all_chunks);

    debug!(
        "upload_ranges: composed hash={}, {} segments, {} dirty regions",
        combined_hash.hex(),
        all_segments.len(),
        middle_results.len()
    );

    let composed_mdb = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(combined_hash, all_segments.len(), true, false),
        segments: all_segments,
        verification: all_verification,
        metadata_ext: None,
    };

    // 7. Register composed file and finalize.
    let register_session = FileUploadSession::new(config, None).await?;
    register_session.register_composed_file(composed_mdb).await?;
    register_session.finalize().await?;

    let total_dirty: u64 = effective_ranges.iter().map(|(s, e)| e - s).sum();
    info!(
        "upload_ranges: hash={} size={} (original={}, {} dirty regions, {} dirty bytes)",
        combined_hash.hex(),
        total_size,
        original_size,
        middle_results.len(),
        total_dirty
    );

    Ok(XetFileInfo::new(combined_hash.hex(), total_size))
}

/// Extract segments and verification entries for chunks [chunk_start, chunk_end)
/// from the original reconstruction plan, truncating segments at boundaries.
fn extract_segments(
    original_mdb: &MDBFileInfo,
    original_chunks: &[(MerkleHash, u64)],
    chunk_start: usize,
    chunk_end: usize,
) -> (
    Vec<xet_core_structures::metadata_shard::file_structs::FileDataSequenceEntry>,
    Vec<FileVerificationEntry>,
) {
    let mut segments = Vec::new();
    let mut verification = Vec::new();
    let mut cursor = 0usize;

    for seg in &original_mdb.segments {
        let seg_count = (seg.chunk_index_end - seg.chunk_index_start) as usize;
        let seg_end = cursor + seg_count;

        let overlap_start = cursor.max(chunk_start);
        let overlap_end = seg_end.min(chunk_end);
        if overlap_start < overlap_end {
            let count = overlap_end - overlap_start;
            let mut truncated = seg.clone();
            truncated.chunk_index_start += (overlap_start - cursor) as u32;
            truncated.chunk_index_end = truncated.chunk_index_start + count as u32;
            let bytes: u64 = original_chunks[overlap_start..overlap_end].iter().map(|(_, s)| s).sum();
            truncated.unpacked_segment_bytes = bytes as u32;
            segments.push(truncated);

            let hashes: Vec<MerkleHash> = original_chunks[overlap_start..overlap_end].iter().map(|(h, _)| *h).collect();
            verification.push(FileVerificationEntry::new(range_hash_from_chunks(&hashes)));
        }

        cursor = seg_end;
        if cursor >= chunk_end {
            break;
        }
    }

    (segments, verification)
}
