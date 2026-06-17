// Smoke for session-level (in-commit) deduplication.
//
// Upload the SAME 65 MiB buffer twice in a single XetUploadCommit. The payload
// must exceed MAX_XORB_BYTES (64 MiB in xet_core_structures) so the first
// file's processing forces an xorb cut; the cut xorb's chunks land in
// session_shard via add_xorb_block, and the second file's chunk-by-chunk dedup
// query then hits those entries. (A 1 MiB payload would NOT dedup: with no cut
// triggered, session_shard stays empty until commit-time finalization, after
// the second file's chunks have already been decided.)
//
// Global dedup (cross-commit, CAS-indexed shards) is covered by the
// global-dedup scenario.

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
