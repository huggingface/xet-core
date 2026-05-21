// CI smoke: validate_session_inputs rejects bad inputs in the wasm wrapper.
//
// Fully local — no Hub or CAS network calls. Catches regressions to the
// validation surface (e.g., accidental re-introduction of the 0→u64::MAX
// tokenExpiry sentinel, weakened token / endpoint checks).

import { runBrowserSmoke } from './lib.mjs';

const PORT = parseInt(process.env.PORT || '8774', 10);
const TEST_TIMEOUT_MS = 60 * 1000;
const EXPECTED_CASE_COUNT = 9 * 2; // 9 invalid input cases × 2 methods

let exitCode = 1;
try {
  const result = await runBrowserSmoke({
    pagePath: 'ci-smoke/invalid-inputs.html',
    runArg: null,
    timeoutMs: TEST_TIMEOUT_MS,
    port: PORT,
  });

  if (!result.ok) {
    if (Array.isArray(result.failures) && result.failures.length > 0) {
      for (const f of result.failures) {
        console.error(`  ${f.label}: ${f.reason}`);
      }
    }
    throw new Error(result.error);
  }
  if (result.casesChecked !== EXPECTED_CASE_COUNT) {
    throw new Error(`casesChecked=${result.casesChecked}, expected ${EXPECTED_CASE_COUNT}`);
  }

  console.log(`PASS (${result.casesChecked} cases)`);
  exitCode = 0;
} catch (e) {
  console.error(`FAIL: ${e?.message || e}`);
  if (e?.stack) console.error(e.stack);
}

process.exit(exitCode);
