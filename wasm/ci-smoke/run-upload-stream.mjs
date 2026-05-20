// CI smoke: drives the wasm streaming upload path end-to-end in headless
// Chromium against xet-team/xet-wasm-test on prod hub.
//
// Same Hub + CAS surface as run-upload.mjs, but exercises uploadStream /
// XetStreamUpload::{write, finish} instead of uploadBytes. The Hub commit
// API is never called — orphan xorbs are reclaimed by CAS GC.
//
// Token source: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN. Either must
// have write scope to xet-team/xet-wasm-test.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8770', 10);
const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_PAYLOAD_SIZE = 1 * 1024 * 1024;

const token = process.env.HF_SMOKE_TEST_TOKEN || process.env.HF_TOKEN || '';
if (!token) {
  console.error(
    'FAIL: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN required for upload-stream smoke ' +
      '(needs write scope to dataset xet-team/xet-wasm-test)',
  );
  process.exit(1);
}

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/upload-stream.html',
    runArg: token,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) {
    if (result.error && /Not initialized with handle set/i.test(result.error)) {
      throw new Error(`REGRESSION: XetRuntime::spawn_blocking panicked on wasm — ${result.error}`);
    }
    throw new Error(result.error);
  }
  if (!result.hash || typeof result.hash !== 'string' || result.hash.length === 0) {
    throw new Error(`metadata missing/invalid hash: ${JSON.stringify(result.metadata)}`);
  }
  if (result.fileSize !== EXPECTED_PAYLOAD_SIZE) {
    throw new Error(`metadata.file_size ${result.fileSize} != expected ${EXPECTED_PAYLOAD_SIZE}`);
  }
  if (!result.commitReport || typeof result.commitReport !== 'object') {
    throw new Error(`commitReport missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  const uploads = result.commitReport.uploads;
  if (!uploads || typeof uploads !== 'object' || Object.keys(uploads).length === 0) {
    throw new Error(
      `commitReport.uploads empty — expected at least one xorb push (random 1 MiB payload should not 100%-dedup): ${JSON.stringify(result.commitReport)}`,
    );
  }
  const dedup = result.commitReport.dedup_metrics;
  if (!dedup || typeof dedup !== 'object') {
    throw new Error(`commitReport.dedup_metrics missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  if (!(dedup.xorb_bytes_uploaded > 0)) {
    throw new Error(
      `commitReport.dedup_metrics.xorb_bytes_uploaded=${dedup.xorb_bytes_uploaded} — expected > 0 for a fresh random payload: ${JSON.stringify(dedup)}`,
    );
  }
  if (!(dedup.shard_bytes_uploaded > 0)) {
    throw new Error(
      `commitReport.dedup_metrics.shard_bytes_uploaded=${dedup.shard_bytes_uploaded} — expected > 0: ${JSON.stringify(dedup)}`,
    );
  }
  if (!(dedup.total_bytes_uploaded > 0)) {
    throw new Error(
      `commitReport.dedup_metrics.total_bytes_uploaded=${dedup.total_bytes_uploaded} — expected > 0: ${JSON.stringify(dedup)}`,
    );
  }

  console.log('PASS');
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
