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

import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CRATE_ROOT = path.resolve(__dirname, '../..');
const PORT = parseInt(process.env.PORT || '8766', 10);

const TEST_TIMEOUT_MS = 2 * 60 * 1000;
const EXPECTED_PAYLOAD_SIZE = 1 * 1024 * 1024;

const server = spawn('node', [path.join(__dirname, 'server.mjs'), CRATE_ROOT], {
  env: { ...process.env, PORT: String(PORT) },
  stdio: ['ignore', 'inherit', 'inherit'],
});
const cleanup = () => { try { server.kill('SIGTERM'); } catch {} };
process.on('exit', cleanup);
process.on('SIGINT', () => { cleanup(); process.exit(130); });
process.on('SIGTERM', () => { cleanup(); process.exit(143); });

await sleep(500);

const browser = await chromium.launch({ headless: true });
let exitCode = 1;
try {
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  page.on('console', (msg) => console.log(`[browser ${msg.type()}] ${msg.text()}`));
  page.on('pageerror', (e) => console.error(`[browser error] ${e.message}\n${e.stack || ''}`));

  await page.goto(`http://127.0.0.1:${PORT}/tests/ci-smoke/index.html`, { waitUntil: 'load' });

  if (!(await page.evaluate(() => self.crossOriginIsolated))) {
    throw new Error('page is not crossOriginIsolated — COOP/COEP headers missing or wrong');
  }

  const result = await Promise.race([
    page.evaluate(async () => await window.runTest()),
    sleep(TEST_TIMEOUT_MS).then(() => { throw new Error(`runTest timed out after ${TEST_TIMEOUT_MS}ms`); }),
  ]);

  console.log('result:', JSON.stringify(result, null, 2));

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
} finally {
  await browser.close().catch(() => {});
  cleanup();
}

process.exit(exitCode);
