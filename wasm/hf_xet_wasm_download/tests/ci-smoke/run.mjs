// CI smoke: download a known file through XetSession in headless Chromium
// and assert byte count + content SHA-256.
//
// Pins to xet-team/xet-spec-reference-files @ main (resolved server-side at
// download time). Failures here are signal that the wasm download path is
// broken against prod hub + CAS.

import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CRATE_ROOT = path.resolve(__dirname, '../..');
const PORT = parseInt(process.env.PORT || '8765', 10);

const EXPECTED_SIZE = 63527244;
const EXPECTED_SHA256 = 'f41255b252f776125f2b657136654e4d4d5d2ccf8ef4db0ec186bd1981b69734';
const TEST_TIMEOUT_MS = 5 * 60 * 1000;

const token = process.env.HF_TOKEN;
if (!token) {
  console.error('HF_TOKEN not set');
  process.exit(2);
}

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
    page.evaluate(async (tok) => await window.runTest(tok), token),
    sleep(TEST_TIMEOUT_MS).then(() => { throw new Error(`runTest timed out after ${TEST_TIMEOUT_MS}ms`); }),
  ]);

  console.log('result:', JSON.stringify(result, null, 2));

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
} finally {
  await browser.close().catch(() => {});
  cleanup();
}

process.exit(exitCode);
