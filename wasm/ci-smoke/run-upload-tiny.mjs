// CI smoke: empty (0-byte) and tiny (1-byte) file uploads in one commit
// against xet-team/xet-wasm-test on prod hub.
//
// Catches regressions to the empty-xorb suppression and the no-chunks
// path in the chunker — both are easy targets for off-by-one bugs.
//
// Token source: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8780', 10);
const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_NORMAL_SIZE = 64 * 1024;

const token = process.env.HF_SMOKE_TEST_TOKEN || process.env.HF_TOKEN || '';
if (!token) {
  console.error(
    'FAIL: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN required for upload-tiny smoke ' +
      '(needs write scope to dataset xet-team/xet-wasm-test)',
  );
  process.exit(1);
}

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/upload-tiny.html',
    runArg: token,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) throw new Error(result.error);

  if (result.empty?.xet_info?.file_size !== 0) {
    throw new Error(`empty file: expected file_size=0, got ${result.empty?.xet_info?.file_size}: ${JSON.stringify(result.empty)}`);
  }
  if (result.oneByte?.xet_info?.file_size !== 1) {
    throw new Error(`one-byte file: expected file_size=1, got ${result.oneByte?.xet_info?.file_size}: ${JSON.stringify(result.oneByte)}`);
  }
  if (result.normal?.xet_info?.file_size !== EXPECTED_NORMAL_SIZE) {
    throw new Error(`normal file: expected file_size=${EXPECTED_NORMAL_SIZE}, got ${result.normal?.xet_info?.file_size}: ${JSON.stringify(result.normal)}`);
  }
  if (!result.empty?.xet_info?.hash || !result.oneByte?.xet_info?.hash || !result.normal?.xet_info?.hash) {
    throw new Error(`missing hash on one of the uploads: ${JSON.stringify({ e: result.empty, o: result.oneByte, n: result.normal })}`);
  }

  const uploads = result.commitReport?.uploads;
  if (!uploads || typeof uploads !== 'object') {
    throw new Error(`commitReport.uploads missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  const uploadKeys = Object.keys(uploads);
  if (uploadKeys.length !== 3) {
    throw new Error(`commitReport.uploads has ${uploadKeys.length} entries, expected 3: ${JSON.stringify(uploads)}`);
  }

  // All three returned hashes should appear in the commit report.
  const reportedHashes = new Set(uploadKeys.map((k) => uploads[k]?.xet_info?.hash));
  for (const meta of [result.empty, result.oneByte, result.normal]) {
    if (!reportedHashes.has(meta.xet_info.hash)) {
      throw new Error(`hash ${meta.xet_info.hash} not present in commitReport.uploads: ${JSON.stringify([...reportedHashes])}`);
    }
  }

  console.log(`PASS (0-byte, 1-byte, ${EXPECTED_NORMAL_SIZE}-byte all committed)`);
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
