// CI smoke: two sequential XetUploadCommits from one XetSession, each
// uploading distinct content and committing independently.
//
// Token source: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8782', 10);
const TEST_TIMEOUT_MS = 3 * 60 * 1000;
const EXPECTED_PAYLOAD_SIZE = 256 * 1024;

const token = process.env.HF_SMOKE_TEST_TOKEN || process.env.HF_TOKEN || '';
if (!token) {
  console.error(
    'FAIL: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN required for upload-multi-commit smoke ' +
      '(needs write scope to dataset xet-team/xet-wasm-test)',
  );
  process.exit(1);
}

function checkSide(side, label) {
  const meta = side?.meta;
  if (meta?.xet_info?.file_size !== EXPECTED_PAYLOAD_SIZE) {
    throw new Error(`${label}: file_size=${meta?.xet_info?.file_size}, expected ${EXPECTED_PAYLOAD_SIZE}`);
  }
  if (!meta?.xet_info?.hash) {
    throw new Error(`${label}: missing hash: ${JSON.stringify(meta)}`);
  }
  const report = side?.report;
  if (!report || typeof report !== 'object') {
    throw new Error(`${label}: commitReport missing/invalid: ${JSON.stringify(report)}`);
  }
  if (!report.uploads || Object.keys(report.uploads).length !== 1) {
    throw new Error(`${label}: expected 1 upload in commitReport, got ${JSON.stringify(report.uploads)}`);
  }
  const dedup = report.dedup_metrics;
  if (!(dedup?.xorb_bytes_uploaded > 0)) {
    throw new Error(`${label}: xorb_bytes_uploaded=${dedup?.xorb_bytes_uploaded}, expected > 0`);
  }
  return meta.xet_info.hash;
}

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/upload-multi-commit.html',
    runArg: token,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) throw new Error(result.error);

  const hashA = checkSide(result.commitA, 'commitA');
  const hashB = checkSide(result.commitB, 'commitB');

  if (hashA === hashB) {
    throw new Error(`distinct random payloads produced the same hash ${hashA} — commit isolation bug?`);
  }

  console.log(`PASS (commitA=${hashA.slice(0, 12)}… commitB=${hashB.slice(0, 12)}…)`);
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
