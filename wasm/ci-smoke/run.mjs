// Unified Node runner for the wasm CI smoke scenarios.
//
// Usage: node run.mjs <scenario-name>
//
// Spawns the static server + headless Chromium via lib.mjs, loads
// harness.html?scenario=<name> (which dynamically imports
// scenarios/<name>.mjs), and asserts on the returned result per the
// SCENARIOS table below. Exits 0 on PASS, 1 on FAIL — CI relies on the
// exit status.
//
// Scenarios run sequentially in CI, so a single default port is enough;
// override with PORT if needed.
//
// Token sourcing:
//   - needsWriteToken scenarios require HF_SMOKE_TEST_TOKEN (preferred) or
//     HF_TOKEN with write scope to dataset xet-team/xet-wasm-test.
//   - readToken scenarios forward HF_TOKEN when set (the pinned files live
//     in a public repo, so anonymous works — the token just avoids stricter
//     anonymous rate limits).

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8765', 10);
const MINUTE_MS = 60 * 1000;
const DEFAULT_TIMEOUT_MS = 2 * MINUTE_MS;

// ---------------------------------------------------------------------------
// Shared assertion helpers
// ---------------------------------------------------------------------------

// Top-level dedup_metrics is the session-aggregated view. xorb_bytes_uploaded
// and shard_bytes_uploaded must both be > 0 for a fresh random payload —
// anything 0 here means a metric capture race in file_upload_session::finalize_impl
// (take(deduplication_metrics) ordered before join of xorb_upload_tasks) or
// a regression in the wasm CAS push path.
function assertDedupBytesUploaded(commitReport) {
  const dedup = commitReport.dedup_metrics;
  if (!dedup || typeof dedup !== 'object') {
    throw new Error(`commitReport.dedup_metrics missing/invalid: ${JSON.stringify(commitReport)}`);
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
  return dedup;
}

// Single-file upload scenarios: metadata hash/size + non-empty uploads map
// (proves a xorb was actually pushed, not a 100% dedup hit) + positive
// dedup byte counters.
function assertSingleUpload(result, expectedSize) {
  if (!result.hash || typeof result.hash !== 'string' || result.hash.length === 0) {
    throw new Error(`metadata missing/invalid hash: ${JSON.stringify(result.metadata)}`);
  }
  if (result.fileSize !== expectedSize) {
    throw new Error(`metadata.file_size ${result.fileSize} != expected ${expectedSize}`);
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
  assertDedupBytesUploaded(result.commitReport);
}

// Multi-file upload scenarios: distinct per-file hashes, expected sizes,
// uploads map entries matching the returned hashes, positive dedup counters.
function assertMultiUpload(result, expectedFileCount, expectedFileSize) {
  if (!Array.isArray(result.hashes) || result.hashes.length !== expectedFileCount) {
    throw new Error(`expected ${expectedFileCount} hashes, got ${JSON.stringify(result.hashes)}`);
  }
  for (const h of result.hashes) {
    if (!h || typeof h !== 'string' || h.length === 0) {
      throw new Error(`invalid hash in result.hashes: ${JSON.stringify(result.hashes)}`);
    }
  }
  const distinct = new Set(result.hashes);
  if (distinct.size !== expectedFileCount) {
    throw new Error(`expected ${expectedFileCount} distinct hashes, got ${distinct.size}: ${JSON.stringify(result.hashes)}`);
  }
  for (const s of result.fileSizes) {
    if (s !== expectedFileSize) {
      throw new Error(`file_size ${s} != expected ${expectedFileSize} (sizes=${JSON.stringify(result.fileSizes)})`);
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
  if (uploadKeys.length !== expectedFileCount) {
    throw new Error(
      `commitReport.uploads has ${uploadKeys.length} entries, expected ${expectedFileCount}: ${JSON.stringify(uploads)}`,
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
  assertDedupBytesUploaded(result.commitReport);
}

// ---------------------------------------------------------------------------
// Scenario table
// ---------------------------------------------------------------------------
//
// Per scenario:
//   needsWriteToken — requires HF_SMOKE_TEST_TOKEN / HF_TOKEN (write scope);
//                     missing token is an immediate FAIL.
//   readToken       — forwards HF_TOKEN when set; anonymous otherwise.
//   spawnBlockingGuard — map "Not initialized with handle set" errors to an
//                     explicit XetRuntime::spawn_blocking regression message.
//   timeoutMs       — overrides the 2-minute default for the in-page run.
//   assert(result)  — throws on failure; may return a suffix for the PASS line.

const SCENARIOS = {
  // validate_session_inputs rejects bad token / endpoint / tokenExpiry inputs
  // in the wasm wrapper. Fully local — no Hub or CAS network calls.
  'invalid-inputs': {
    timeoutMs: 1 * MINUTE_MS,
    assert(result) {
      const expected = 9 * 2; // 9 invalid input cases × 2 methods
      if (result.casesChecked !== expected) {
        throw new Error(`casesChecked=${result.casesChecked}, expected ${expected}`);
      }
      return `${result.casesChecked} cases`;
    },
  },

  // Download a known pinned file and assert byte count + content SHA-256.
  // Failures here are signal that the wasm download path is broken against
  // prod hub + CAS. Bump these alongside READ_REPO in common.mjs when
  // re-pinning.
  download: {
    readToken: true,
    assert(result) {
      const EXPECTED_SIZE = 540217;
      const EXPECTED_SHA256 = '9922e8996d0c7e24c7f4e7a5d9c5b7303549f4ee94de0f1138b103014b51be13';
      if (result.byteCount !== EXPECTED_SIZE) {
        throw new Error(`byte count ${result.byteCount} != expected ${EXPECTED_SIZE}`);
      }
      if (result.sha256 !== EXPECTED_SHA256) {
        throw new Error(`sha256 ${result.sha256} != expected ${EXPECTED_SHA256}`);
      }
    },
  },

  // Concurrent multi-file download in one XetDownloadStreamGroup. Asserts
  // each downloaded byteCount matches the corresponding paths-info size —
  // a stream-fan-out bug that crossed buffers would produce mismatched
  // lengths.
  'download-multi': {
    readToken: true,
    assert(result) {
      const EXPECTED_FILE_COUNT = 2;
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
      return `${result.downloads.length} concurrent downloads`;
    },
  },

  // End-to-end uploadBytes + commit(). Failures mean either the wasm
  // spawn_blocking path regressed, the upload data-prep / CAS push path is
  // broken, or the Hub / CAS leg flaked (step is continue-on-error in CI for
  // that reason).
  upload: {
    needsWriteToken: true,
    spawnBlockingGuard: true,
    assert(result) {
      assertSingleUpload(result, 1 * 1024 * 1024);
    },
  },

  // Streaming variant: uploadStream + XetStreamUpload::{write, finish}.
  'upload-stream': {
    needsWriteToken: true,
    spawnBlockingGuard: true,
    assert(result) {
      assertSingleUpload(result, 1 * 1024 * 1024);
    },
  },

  // Concurrent multi-file uploadBytes in a single XetUploadCommit — separate
  // xorb tasks, separate per-file metadata, all rolled up into one
  // commitReport.
  'upload-multi': {
    needsWriteToken: true,
    spawnBlockingGuard: true,
    assert(result) {
      assertMultiUpload(result, 3, 512 * 1024);
    },
  },

  // Same shape as upload-multi but using the streaming path for each of the
  // parallel files.
  'upload-stream-multi': {
    needsWriteToken: true,
    spawnBlockingGuard: true,
    assert(result) {
      assertMultiUpload(result, 3, 512 * 1024);
    },
  },

  // Heterogeneous handles in a single commit: uploadBytes + uploadStream
  // concurrently, then commit().
  'upload-mixed': {
    needsWriteToken: true,
    assert(result) {
      const EXPECTED_BYTES_SIZE = 256 * 1024;
      const EXPECTED_STREAM_SIZE = 256 * 1024;
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
      return `bytes + stream both in commitReport.uploads, xorb_uploaded=${dedup.xorb_bytes_uploaded}`;
    },
  },

  // Empty (0-byte) and tiny (1-byte) files in one commit — catches
  // regressions to the empty-xorb suppression and the no-chunks path in the
  // chunker, both easy targets for off-by-one bugs.
  'upload-tiny': {
    needsWriteToken: true,
    assert(result) {
      const EXPECTED_NORMAL_SIZE = 64 * 1024;
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
      return `0-byte, 1-byte, ${EXPECTED_NORMAL_SIZE}-byte all committed`;
    },
  },

  // Two sequential XetUploadCommits from one XetSession, each uploading
  // distinct content and committing independently — catches XetSession-level
  // resource leaks that would surface as the second newUploadCommit()
  // hanging or panicking.
  'upload-multi-commit': {
    needsWriteToken: true,
    timeoutMs: 3 * MINUTE_MS,
    assert(result) {
      const EXPECTED_PAYLOAD_SIZE = 256 * 1024;
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
      const hashA = checkSide(result.commitA, 'commitA');
      const hashB = checkSide(result.commitB, 'commitB');
      if (hashA === hashB) {
        throw new Error(`distinct random payloads produced the same hash ${hashA} — commit isolation bug?`);
      }
      return `commitA=${hashA.slice(0, 12)}… commitB=${hashB.slice(0, 12)}…`;
    },
  },

  // Session-level (in-commit) deduplication: identical 65 MiB bytes uploaded
  // as two files in one commit. See scenarios/dedup.mjs for why the payload
  // must exceed MAX_XORB_BYTES (64 MiB).
  dedup: {
    needsWriteToken: true,
    timeoutMs: 5 * MINUTE_MS,
    assert(result) {
      const EXPECTED_PAYLOAD_SIZE = 65 * 1024 * 1024;
      const XORB_CUT_BYTES = 64 * 1024 * 1024; // MAX_XORB_BYTES in xet_core_structures
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
      return `total=${dedup.total_bytes}, deduped=${dedup.deduped_bytes}, xorb_uploaded=${dedup.xorb_bytes_uploaded}`;
    },
  },
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const name = process.argv[2];
const scenario = SCENARIOS[name];
if (!scenario) {
  console.error(`FAIL: unknown scenario ${JSON.stringify(name)}`);
  console.error(`usage: node run.mjs <${Object.keys(SCENARIOS).join('|')}>`);
  process.exit(1);
}

let token = null;
if (scenario.needsWriteToken) {
  token = process.env.HF_SMOKE_TEST_TOKEN || process.env.HF_TOKEN || '';
  if (!token) {
    console.error(
      `FAIL: HF_SMOKE_TEST_TOKEN (preferred) or HF_TOKEN required for ${name} smoke ` +
        '(needs write scope to dataset xet-team/xet-wasm-test)',
    );
    process.exit(1);
  }
} else if (scenario.readToken) {
  token = process.env.HF_TOKEN || '';
}

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: `ci-smoke/harness.html?scenario=${name}`,
    runArg: token,
    timeoutMs: scenario.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) {
    if (Array.isArray(result.failures) && result.failures.length > 0) {
      for (const f of result.failures) {
        console.error(`  ${f.label}: ${f.reason}`);
      }
    }
    // Anything mentioning the spawn_blocking expect message means the critical
    // bug is back. Surface that explicitly so the failure cause is unambiguous.
    if (scenario.spawnBlockingGuard && result.error && /Not initialized with handle set/i.test(result.error)) {
      throw new Error(`REGRESSION: XetRuntime::spawn_blocking panicked on wasm — ${result.error}`);
    }
    throw new Error(result.error);
  }

  const suffix = scenario.assert(result);
  console.log(suffix ? `PASS (${suffix})` : 'PASS');
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
