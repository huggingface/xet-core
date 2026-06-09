// Smoke for session-level (in-commit) deduplication.
//
// Upload the SAME 65 MiB buffer twice in a single XetUploadCommit. The
// payload must exceed MAX_XORB_BYTES (64 MiB in xet_core_structures) so
// the first file's processing forces an xorb cut; the cut xorb's chunks
// land in session_shard via add_xorb_block, and the second file's
// chunk-by-chunk dedup query then hits those entries.
//
// 1 MiB payloads do NOT exercise dedup here: with both files' data
// resident in `current_session_data` and no xorb cut triggered, the
// session_shard stays empty until commit-time finalization — by which
// point the dedup decision for the second file's chunks has already
// been made.
//
// Note on global dedup: a deterministic test of the global-dedup query
// path (cross-commit, CAS-indexed shards) would need a chunk whose hash
// satisfies `hash % 1024 == 0`. Random payloads only have ~0.1% chance
// per chunk; not worth the flake. The session-shard branch of
// chunk_hash_dedup_query is on the same surface and covers the
// regression class.

import { openUploadCommit, randomBytes } from '../common.mjs';

const PAYLOAD_SIZE = 65 * 1024 * 1024; // > MAX_XORB_BYTES (64 MiB) to force a cut between files

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  const payload = randomBytes(PAYLOAD_SIZE);

  console.log('uploadBytes #1 (first.bin)...');
  const metaFirst = await commit.uploadBytes(payload, 'compute', 'first.bin');

  console.log('uploadBytes #2 (second.bin, same payload)...');
  const metaSecond = await commit.uploadBytes(payload, 'compute', 'second.bin');

  console.log('commit()...');
  const commitReport = await commit.commit();
  console.log('commitReport.dedup_metrics:', JSON.stringify(commitReport.dedup_metrics));

  return {
    ok: true,
    first: metaFirst,
    second: metaSecond,
    commitReport,
    payloadSize: PAYLOAD_SIZE,
  };
}
