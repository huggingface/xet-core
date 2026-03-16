use std::sync::Arc;

use tracing::{debug, info};
use ulid::Ulid;
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_core_structures::merklehash::{MerkleHash, file_hash};
use xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks;
use xet_core_structures::metadata_shard::file_structs::{FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo};

use super::XetFileInfo;
use super::configurations::TranslatorConfig;
use super::errors::Result;
use super::file_cleaner::Sha256Policy;
use super::file_upload_session::FileUploadSession;
use crate::file_reconstruction::FileReconstructor;

/// Upload a modified range of an existing file, composing the result with
/// the original file's CAS segments. Only the dirty region (plus CDC boundary
/// chunks) is re-uploaded; prefix and suffix segments are reused from the
/// original file's reconstruction plan.
///
/// Returns the [`XetFileInfo`] for the combined file.
///
/// # Arguments
///
/// * `config` - Translator configuration for creating upload sessions.
/// * `cas_client` - CAS client for fetching original file metadata and downloading boundary chunks.
/// * `original_hash` - Merkle hash of the original file in CAS.
/// * `original_size` - Size of the original file in bytes.
/// * `dirty_data` - The bytes covering the modified region `[dirty_start, dirty_start + dirty_data.len())`.
/// * `dirty_start` - Byte offset where the modification begins.
/// * `total_size` - Total size of the modified file (may be larger than `original_size` for appends).
pub async fn upload_range(
    config: Arc<TranslatorConfig>,
    cas_client: Arc<dyn Client>,
    original_hash: MerkleHash,
    original_size: u64,
    dirty_data: &[u8],
    dirty_start: u64,
    total_size: u64,
) -> Result<XetFileInfo> {
    // 1. Fetch chunk hashes and reconstruction info for the original file in parallel.
    let (original_chunks, recon_result) = tokio::try_join!(
        async {
            cas_client.get_file_chunk_hashes(&original_hash).await.map_err(|e| {
                super::errors::DataProcessingError::InternalError(format!("get_file_chunk_hashes failed: {e}"))
            })
        },
        async {
            cas_client.get_file_reconstruction_info(&original_hash).await.map_err(|e| {
                super::errors::DataProcessingError::InternalError(format!("get_file_reconstruction_info failed: {e}"))
            })
        },
    )?;

    let (original_mdb, _) = recon_result.ok_or_else(|| {
        super::errors::DataProcessingError::InternalError("no reconstruction info for original file".into())
    })?;

    // 2. Compute effective dirty range.
    let dirty_end = dirty_start + dirty_data.len() as u64;
    let effective_dirty_start = dirty_start.min(total_size);
    let effective_dirty_end = if total_size > original_size {
        dirty_end.max(total_size) // include appended bytes
    } else {
        dirty_end.min(total_size) // clamp to file size
    };

    // If the dirty range is empty and the file didn't grow, return the original file info.
    if effective_dirty_start >= effective_dirty_end && total_size <= original_size {
        return Ok(XetFileInfo::new(original_hash.hex(), original_size));
    }

    // 3. Map chunks to cumulative byte offsets and identify dirty boundaries.
    let num_chunks = original_chunks.len();
    let mut chunk_offsets: Vec<u64> = Vec::with_capacity(num_chunks + 1);
    chunk_offsets.push(0);
    for (_, size) in &original_chunks {
        chunk_offsets.push(chunk_offsets.last().unwrap() + size);
    }

    // Find first chunk that overlaps the dirty range.
    let first_dirty_chunk = (0..num_chunks)
        .find(|&i| chunk_offsets[i + 1] > effective_dirty_start)
        .unwrap_or(num_chunks);
    // Find last chunk (exclusive) that overlaps the dirty range.
    let last_dirty_chunk = (0..num_chunks)
        .rev()
        .find(|&i| chunk_offsets[i] < effective_dirty_end.min(original_size))
        .map(|i| i + 1)
        .unwrap_or(first_dirty_chunk);

    let prefix_chunks = &original_chunks[..first_dirty_chunk];
    let suffix_chunks = &original_chunks[last_dirty_chunk..];

    debug!(
        "upload_range: chunks={}, dirty=[{}, {}), first_dirty={}, last_dirty={}, \
         prefix={} chunks, suffix={} chunks",
        num_chunks,
        effective_dirty_start,
        effective_dirty_end,
        first_dirty_chunk,
        last_dirty_chunk,
        prefix_chunks.len(),
        suffix_chunks.len(),
    );

    // 4. Extract stable prefix and suffix segments from reconstruction info.
    let extract_segments = |chunk_start: usize, chunk_end: usize| -> (Vec<_>, Vec<_>) {
        let mut segments = Vec::new();
        let mut verification = Vec::new();
        let mut cursor = 0usize;
        for seg in &original_mdb.segments {
            let seg_chunk_count = (seg.chunk_index_end - seg.chunk_index_start) as usize;
            let seg_end = cursor + seg_chunk_count;

            let overlap_start = cursor.max(chunk_start);
            let overlap_end = seg_end.min(chunk_end);
            if overlap_start < overlap_end {
                let effective_count = overlap_end - overlap_start;
                let mut truncated_seg = seg.clone();
                truncated_seg.chunk_index_start += (overlap_start - cursor) as u32;
                truncated_seg.chunk_index_end = truncated_seg.chunk_index_start + effective_count as u32;
                let seg_bytes: u64 = original_chunks[overlap_start..overlap_end].iter().map(|(_, size)| size).sum();
                truncated_seg.unpacked_segment_bytes = seg_bytes as u32;
                segments.push(truncated_seg);

                let chunk_hashes: Vec<MerkleHash> =
                    original_chunks[overlap_start..overlap_end].iter().map(|(h, _)| *h).collect();
                verification.push(FileVerificationEntry::new(range_hash_from_chunks(&chunk_hashes)));
            }

            cursor = seg_end;
        }
        (segments, verification)
    };

    let (prefix_segments, prefix_verification) = extract_segments(0, first_dirty_chunk);
    let (suffix_segments, suffix_verification) = extract_segments(last_dirty_chunk, num_chunks);

    // 5. Download boundary chunk data from CAS and assemble middle buffer.
    let boundary_start = chunk_offsets.get(first_dirty_chunk).copied().unwrap_or(original_size);
    let boundary_end = chunk_offsets.get(last_dirty_chunk).copied().unwrap_or(original_size);

    let mut boundary_data = Vec::new();
    if boundary_start < boundary_end && boundary_end <= original_size {
        let reconstructor = FileReconstructor::new(&cas_client, original_hash)
            .with_byte_range(FileRange::new(boundary_start, boundary_end));
        let mut stream = reconstructor.reconstruct_to_stream();
        while let Some(chunk) = stream.next().await? {
            boundary_data.extend_from_slice(&chunk);
        }
        debug!(
            "upload_range: downloaded boundary bytes ({} bytes) from [{}, {})",
            boundary_data.len(),
            boundary_start,
            boundary_end
        );
    }

    // The middle region to re-chunk is [boundary_start, max(boundary_end, total_size)).
    let middle_end = if total_size > original_size {
        boundary_end.max(total_size)
    } else {
        boundary_end
    };
    let middle_size = (middle_end - boundary_start) as usize;
    let mut middle_data = vec![0u8; middle_size];

    // Fill with original boundary data first.
    if !boundary_data.is_empty() {
        let copy_len = boundary_data.len().min(middle_size);
        middle_data[..copy_len].copy_from_slice(&boundary_data[..copy_len]);
    }

    // Overlay dirty bytes at the correct offset within the middle buffer.
    {
        let staging_read_start = dirty_start.max(boundary_start);
        let staging_read_end = effective_dirty_end.min(total_size);
        if staging_read_end > staging_read_start {
            let read_len = (staging_read_end - staging_read_start) as usize;
            let offset_in_middle = (staging_read_start - boundary_start) as usize;
            let offset_in_dirty = (staging_read_start - dirty_start) as usize;
            middle_data[offset_in_middle..offset_in_middle + read_len]
                .copy_from_slice(&dirty_data[offset_in_dirty..offset_in_dirty + read_len]);
        }
    }

    // 6. Upload middle data through SingleFileCleaner.
    let middle_session = FileUploadSession::new(config.clone(), None).await?;
    let mut cleaner = middle_session
        .start_clean(None, Some(middle_data.len() as u64), Sha256Policy::Skip, Ulid::new())
        .await;
    cleaner.add_data(&middle_data).await?;
    let (middle_file_info, _metrics) = cleaner.finish().await?;

    // 7. Get the middle file's reconstruction info and chunk hashes.
    let (_metrics, middle_file_infos) = middle_session.finalize_with_file_info().await?;

    let middle_mdb = middle_file_infos
        .into_iter()
        .next()
        .ok_or_else(|| super::errors::DataProcessingError::InternalError("finalize returned no file info".into()))?;

    let middle_hash = MerkleHash::from_hex(middle_file_info.hash())?;
    let middle_chunks = cas_client.get_file_chunk_hashes(&middle_hash).await.map_err(|e| {
        super::errors::DataProcessingError::InternalError(format!("get middle chunk hashes failed: {e}"))
    })?;

    // 8. Compose combined file: prefix + middle + suffix.
    let mut all_chunks: Vec<(MerkleHash, u64)> = prefix_chunks.to_vec();
    all_chunks.extend_from_slice(&middle_chunks);
    all_chunks.extend_from_slice(suffix_chunks);
    let combined_hash = file_hash(&all_chunks);

    let prefix_seg_count = prefix_segments.len();
    let mut combined_segments = prefix_segments;
    combined_segments.extend_from_slice(&middle_mdb.segments);
    combined_segments.extend_from_slice(&suffix_segments);

    let mut combined_verification = prefix_verification;
    combined_verification.extend_from_slice(&middle_mdb.verification);
    combined_verification.extend_from_slice(&suffix_verification);

    debug!(
        "upload_range composition: combined_hash={}, segments={}, verification={}, \
         prefix_segs={}, middle_segs={}, suffix_segs={}",
        combined_hash.hex(),
        combined_segments.len(),
        combined_verification.len(),
        prefix_seg_count,
        middle_mdb.segments.len(),
        suffix_segments.len()
    );

    let composed_mdb = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(
            combined_hash,
            combined_segments.len(),
            true, // CAS requires verification entries
            false,
        ),
        segments: combined_segments,
        verification: combined_verification,
        metadata_ext: None,
    };

    // 9. Register composed file and finalize.
    let register_session = FileUploadSession::new(config, None).await?;
    register_session.register_composed_file(composed_mdb).await?;
    register_session.finalize().await?;

    let dirty_size = effective_dirty_end - effective_dirty_start;
    info!(
        "upload_range: composed file hash={} size={} (original={}, dirty_range=[{}, {}), dirty_size={})",
        combined_hash.hex(),
        total_size,
        original_size,
        effective_dirty_start,
        effective_dirty_end,
        dirty_size
    );

    Ok(XetFileInfo::new(combined_hash.hex(), total_size))
}
