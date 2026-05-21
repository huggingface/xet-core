// CI smoke: heterogeneous upload handles in a single XetUploadCommit —
// uploadBytes + uploadStream concurrently, then commit().
//
// Token source: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8781', 10);
const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_BYTES_SIZE = 256 * 1024;
const EXPECTED_STREAM_SIZE = 256 * 1024;

const token = process.env.HF_SMOKE_TEST_TOKEN || process.env.HF_TOKEN || '';
if (!token) {
  console.error(
    'FAIL: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN required for upload-mixed smoke ' +
      '(needs write scope to dataset xet-team/xet-wasm-test)',
  );
  process.exit(1);
}

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/upload-mixed.html',
    runArg: token,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) throw new Error(result.error);

  if (result.bytesMeta?.xet_info?.file_size !== EXPECTED_BYTES_SIZE) {
    throw new Error(`bytes file_size=${result.bytesMeta?.xet_info?.file_size}, expected ${EXPECTED_BYTES_SIZE}`);
  }
  if (result.streamMeta?.xet_info?.file_size !== EXPECTED_STREAM_SIZE) {
    throw new Error(`stream file_size=${result.streamMeta?.xet_info?.file_size}, expected ${EXPECTED_STREAM_SIZE}`);
  }

  const bytesHash = result.bytesMeta?.xet_info?.hash;
  const streamHash = result.streamMeta?.xet_info?.hash;
  if (!bytesHash || !streamHash) {
    throw new Error(`missing hash: bytes=${bytesHash} stream=${streamHash}`);
  }
  if (bytesHash === streamHash) {
    throw new Error(`distinct random payloads produced the same hash ${bytesHash} — handle-tracking bug?`);
  }

  const uploads = result.commitReport?.uploads;
  if (!uploads || typeof uploads !== 'object') {
    throw new Error(`commitReport.uploads missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  const uploadKeys = Object.keys(uploads);
  if (uploadKeys.length !== 2) {
    throw new Error(`commitReport.uploads has ${uploadKeys.length} entries, expected 2: ${JSON.stringify(uploads)}`);
  }
  const reportedHashes = new Set(uploadKeys.map((k) => uploads[k]?.xet_info?.hash));
  if (!reportedHashes.has(bytesHash)) {
    throw new Error(`bytes hash ${bytesHash} missing from commitReport.uploads: ${JSON.stringify([...reportedHashes])}`);
  }
  if (!reportedHashes.has(streamHash)) {
    throw new Error(`stream hash ${streamHash} missing from commitReport.uploads: ${JSON.stringify([...reportedHashes])}`);
  }

  const dedup = result.commitReport.dedup_metrics;
  if (!dedup || typeof dedup !== 'object') {
    throw new Error(`commitReport.dedup_metrics missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  if (!(dedup.xorb_bytes_uploaded > 0)) {
    throw new Error(`xorb_bytes_uploaded=${dedup.xorb_bytes_uploaded}, expected > 0: ${JSON.stringify(dedup)}`);
  }

  console.log(`PASS (bytes + stream both in commitReport.uploads, xorb_uploaded=${dedup.xorb_bytes_uploaded})`);
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
