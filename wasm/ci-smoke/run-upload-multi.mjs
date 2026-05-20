// CI smoke: concurrent multi-file uploadBytes in a single XetUploadCommit
// against xet-team/xet-wasm-test on prod hub.
//
// Validates that the wasm upload path correctly handles multiple in-flight
// uploads in the same commit (separate xorb tasks, separate per-file metadata,
// all rolled up into one commitReport).
//
// Token source: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8772', 10);
const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_FILE_COUNT = 3;
const EXPECTED_FILE_SIZE = 512 * 1024;

const token = process.env.HF_SMOKE_TEST_TOKEN || process.env.HF_TOKEN || '';
if (!token) {
  console.error(
    'FAIL: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN required for upload-multi smoke ' +
      '(needs write scope to dataset xet-team/xet-wasm-test)',
  );
  process.exit(1);
}

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/upload-multi.html',
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
  if (!Array.isArray(result.hashes) || result.hashes.length !== EXPECTED_FILE_COUNT) {
    throw new Error(`expected ${EXPECTED_FILE_COUNT} hashes, got ${JSON.stringify(result.hashes)}`);
  }
  for (const h of result.hashes) {
    if (!h || typeof h !== 'string' || h.length === 0) {
      throw new Error(`invalid hash in result.hashes: ${JSON.stringify(result.hashes)}`);
    }
  }
  const distinct = new Set(result.hashes);
  if (distinct.size !== EXPECTED_FILE_COUNT) {
    throw new Error(`expected ${EXPECTED_FILE_COUNT} distinct hashes, got ${distinct.size}: ${JSON.stringify(result.hashes)}`);
  }
  for (const s of result.fileSizes) {
    if (s !== EXPECTED_FILE_SIZE) {
      throw new Error(`file_size ${s} != expected ${EXPECTED_FILE_SIZE} (sizes=${JSON.stringify(result.fileSizes)})`);
    }
  }
  if (!result.commitReport || typeof result.commitReport !== 'object') {
    throw new Error(`commitReport missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  const uploads = result.commitReport.uploads;
  if (!uploads || typeof uploads !== 'object') {
    throw new Error(`commitReport.uploads missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  const uploadKeys = Object.keys(uploads);
  if (uploadKeys.length !== EXPECTED_FILE_COUNT) {
    throw new Error(
      `commitReport.uploads has ${uploadKeys.length} entries, expected ${EXPECTED_FILE_COUNT}: ${JSON.stringify(uploads)}`,
    );
  }
  const reportedHashes = new Set(uploadKeys.map((k) => uploads[k]?.xet_info?.hash));
  for (const expectedHash of result.hashes) {
    if (!reportedHashes.has(expectedHash)) {
      throw new Error(
        `hash ${expectedHash} from metadata not present in commitReport.uploads hashes ${JSON.stringify([...reportedHashes])}`,
      );
    }
  }
  const dedup = result.commitReport.dedup_metrics;
  if (!dedup || typeof dedup !== 'object') {
    throw new Error(`commitReport.dedup_metrics missing/invalid: ${JSON.stringify(result.commitReport)}`);
  }
  if (!(dedup.xorb_bytes_uploaded > 0)) {
    throw new Error(
      `commitReport.dedup_metrics.xorb_bytes_uploaded=${dedup.xorb_bytes_uploaded} — expected > 0: ${JSON.stringify(dedup)}`,
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
