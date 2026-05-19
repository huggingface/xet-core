// Shared harness for the wasm CI smoke tests.
//
// Spawns the static server (rooted at the parent `wasm/` dir so the
// `hf_xet_wasm/pkg/` build output is reachable), launches headless Chromium,
// loads `pagePath`, waits for `window.runTest` to be defined, invokes it with
// `runArg`, and returns the raw result.
//
// Each runner does its own assertions on the returned value.

import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Server root is the parent `wasm/` dir so the built `hf_xet_wasm/pkg/`
// tree resolves via relative imports from the ci-smoke HTML pages.
const SERVER_ROOT = path.resolve(__dirname, '..');

async function waitForServerReady(port, { intervalMs = 100, timeoutMs = 10_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastErr;
  while (Date.now() < deadline) {
    try {
      // Any HTTP response means the listener is up and serving. We don't care
      // what status — `/` resolves to a non-existent `wasm/index.html` and the
      // server returns 404, which is enough to know it's ready.
      await fetch(`http://127.0.0.1:${port}/`, { signal: AbortSignal.timeout(500) });
      return;
    } catch (e) {
      lastErr = e;
    }
    await sleep(intervalMs);
  }
  throw new Error(`server did not become ready on port ${port} within ${timeoutMs}ms (last: ${lastErr?.message || lastErr})`);
}

export async function runBrowserSmoke({ pagePath, runArg, timeoutMs, port }) {
  const server = spawn('node', [path.join(__dirname, 'server.mjs'), SERVER_ROOT], {
    env: { ...process.env, PORT: String(port) },
    stdio: ['ignore', 'inherit', 'inherit'],
  });
  const cleanup = () => { try { server.kill('SIGTERM'); } catch {} };
  process.on('exit', cleanup);
  process.on('SIGINT', () => { cleanup(); process.exit(130); });
  process.on('SIGTERM', () => { cleanup(); process.exit(143); });

  await waitForServerReady(port);

  const browser = await chromium.launch({ headless: true });
  try {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();
    page.on('console', (msg) => console.log(`[browser ${msg.type()}] ${msg.text()}`));
    page.on('pageerror', (e) => console.error(`[browser error] ${e.message}\n${e.stack || ''}`));

    await page.goto(`http://127.0.0.1:${port}/${pagePath}`, { waitUntil: 'load' });

    if (!(await page.evaluate(() => self.crossOriginIsolated))) {
      throw new Error('page is not crossOriginIsolated — COOP/COEP headers missing or wrong');
    }

    // The module script uses top-level `await init()`; the `load` event can
    // fire before that resolves. Wait until runTest is registered before
    // invoking it.
    await page.waitForFunction(() => typeof window.runTest === 'function', null, { timeout: 30_000 });

    const result = await Promise.race([
      page.evaluate(async (arg) => await window.runTest(arg), runArg),
      sleep(timeoutMs).then(() => { throw new Error(`runTest timed out after ${timeoutMs}ms`); }),
    ]);

    console.log('result:', JSON.stringify(result, null, 2));
    return result;
  } finally {
    await browser.close().catch(() => {});
    cleanup();
  }
}
