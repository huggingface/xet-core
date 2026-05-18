// CI smoke: download a known file through XetSession in headless Chromium
// and assert byte count + content SHA-256.
//
// Pins to hf-internal-testing/tiny-random-bert at a specific commit SHA (see
// ci-smoke/download.html) so silent drift on `main` cannot break the smoke
// without surfacing the cause. Failures here are signal that the wasm
// download path is broken against prod hub + CAS.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8765', 10);
const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_SIZE = 540217;
const EXPECTED_SHA256 = '9922e8996d0c7e24c7f4e7a5d9c5b7303549f4ee94de0f1138b103014b51be13';

// Token is optional: the pinned file lives in a public repo, so the hub
// paths-info / xet-read-token endpoints work anonymously. We still forward
// HF_TOKEN when set (avoids stricter anonymous rate limits).
const token = process.env.HF_TOKEN || '';

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/download.html',
    runArg: token,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) throw new Error(result.error);
  if (result.byteCount !== EXPECTED_SIZE) {
    throw new Error(`byte count ${result.byteCount} != expected ${EXPECTED_SIZE}`);
  }
  if (result.sha256 !== EXPECTED_SHA256) {
    throw new Error(`sha256 ${result.sha256} != expected ${EXPECTED_SHA256}`);
  }
  console.log('PASS');
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
