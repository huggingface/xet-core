// CI smoke: drives the wasm upload data-prep path in headless Chromium.
//
// We do not commit data anywhere — a real CAS round-trip needs an xet-write-token
// for a sandbox repo, and the bug we're guarding against (XetRuntime::spawn_blocking
// panicking on wasm because handle_ref is unset) fires locally during chunking +
// sha256 + xorb serialization, before any network call. `uploadBytes` is
// documented as local-only, so a placeholder casUrl/token is enough.
//
// Failures here mean either the wasm spawn_blocking path regressed or the
// upload data-prep path is otherwise broken.

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8766', 10);
const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_PAYLOAD_SIZE = 1 * 1024 * 1024;

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/upload.html',
    runArg: null,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) {
    // Anything mentioning the spawn_blocking expect message means the critical
    // bug is back. Surface that explicitly so the failure cause is unambiguous.
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

  console.log('PASS');
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
