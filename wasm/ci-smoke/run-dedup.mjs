// CI smoke: session-level (in-commit) deduplication on wasm.
//
// Uploads identical bytes as two files in one XetUploadCommit. The second
// file's chunk-hash dedup queries should hit the session_shard populated by
// the first file, producing deduped_bytes >= payload and a single-payload
// xorb push (not double).
//
// Token source: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8777', 10);
const TEST_TIMEOUT_MS = 5 * 60 * 1000;
const EXPECTED_PAYLOAD_SIZE = 65 * 1024 * 1024;
const XORB_CUT_BYTES = 64 * 1024 * 1024; // MAX_XORB_BYTES in xet_core_structures

const token = process.env.HF_SMOKE_TEST_TOKEN || process.env.HF_TOKEN || '';
if (!token) {
  console.error(
    'FAIL: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN required for dedup smoke ' +
      '(needs write scope to dataset xet-team/xet-wasm-test)',
  );
  process.exit(1);
}

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/dedup.html',
    runArg: token,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) throw new Error(result.error);

  const dedup = result.commitReport?.dedup_metrics;
  if (!dedup || typeof dedup !== 'object') {
    throw new Error(`commitReport.dedup_metrics missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }

  // Both files were ingested → total_bytes counts both copies.
  if (dedup.total_bytes !== 2 * EXPECTED_PAYLOAD_SIZE) {
    throw new Error(
      `dedup_metrics.total_bytes=${dedup.total_bytes}, expected ${2 * EXPECTED_PAYLOAD_SIZE} (both files counted): ` +
        JSON.stringify(dedup),
    );
  }

  // The second file's chunks must dedup against the first xorb cut during
  // file #1's processing. That cut happens at MAX_XORB_BYTES (64 MiB), so
  // ~64 MiB of file #2 should dedup; the trailing ~1 MiB may not (it ends
  // up in current_session_data on file #1, which the dedup-query path
  // doesn't see).
  if (!(dedup.deduped_bytes >= XORB_CUT_BYTES * 0.95)) {
    throw new Error(
      `dedup_metrics.deduped_bytes=${dedup.deduped_bytes}, expected >= 0.95 * ${XORB_CUT_BYTES} ` +
        `(second file's chunks should dedup against the xorb cut during file #1): ${JSON.stringify(dedup)}`,
    );
  }

  // And critically: xorb_bytes_uploaded should be roughly one payload's
  // worth (one xorb plus the small leftover), not two — proves the dedup
  // actually skipped the second push, not just bumped the counter.
  if (!(dedup.xorb_bytes_uploaded < 1.5 * EXPECTED_PAYLOAD_SIZE)) {
    throw new Error(
      `dedup_metrics.xorb_bytes_uploaded=${dedup.xorb_bytes_uploaded}, expected < 1.5 * ${EXPECTED_PAYLOAD_SIZE} ` +
        `(only one payload's worth of bytes should hit CAS): ${JSON.stringify(dedup)}`,
    );
  }

  console.log(
    `PASS (total=${dedup.total_bytes}, deduped=${dedup.deduped_bytes}, xorb_uploaded=${dedup.xorb_bytes_uploaded})`,
  );
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
