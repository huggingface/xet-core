// CI smoke: concurrent multi-file download in one XetDownloadStreamGroup
// against the same pinned hf-internal-testing/tiny-random-bert commit the
// single-file download smoke uses.
//
// Hits prod hub for paths-info / xet-read-token and prod CAS for xorb
// retrieval. Asserts each downloaded byteCount matches the corresponding
// paths-info size — a stream-fan-out bug that crossed buffers would
// produce mismatched lengths.
//
// Token is optional (public read repo); HF_TOKEN forwarded if set to
// avoid stricter anonymous rate limits.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8775', 10);
const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_FILE_COUNT = 2;

const token = process.env.HF_TOKEN || '';

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/download-multi.html',
    runArg: token,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) throw new Error(result.error);
  if (!Array.isArray(result.downloads) || result.downloads.length !== EXPECTED_FILE_COUNT) {
    throw new Error(`expected ${EXPECTED_FILE_COUNT} downloads, got ${JSON.stringify(result.downloads)}`);
  }
  for (const d of result.downloads) {
    if (d.byteCount !== d.expectedSize) {
      throw new Error(`download ${d.path}: byteCount ${d.byteCount} != expected ${d.expectedSize}`);
    }
    if (!(d.byteCount > 0)) {
      throw new Error(`download ${d.path}: byteCount=${d.byteCount} — expected > 0`);
    }
  }

  console.log(`PASS (${result.downloads.length} concurrent downloads)`);
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
