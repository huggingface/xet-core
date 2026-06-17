// Error-path coverage for the wasm download surface: a well-formed but
// nonexistent hash must reject (CAS has no reconstruction for it), and
// malformed fileInfo objects must reject at the serde boundary. Rejection may
// surface at downloadStream() or at the first next() — both accepted — but a
// case that hangs or resolves with data is a failure. Pins down "errors reject
// promptly" on wasm, where the error crosses the tokio_with_wasm bridge.

import { XetSession, READ_REPO, fetchXetReadToken } from '../common.mjs';

const REJECT_TIMEOUT_MS = 30_000;

async function expectStreamReject(group, label, fileInfo) {
  try {
    const outcome = await Promise.race([
      (async () => {
        const stream = await group.downloadStream(fileInfo);
        const chunk = await stream.next();
        return { chunk };
      })(),
      new Promise((resolve) => setTimeout(() => resolve({ timedOut: true }), REJECT_TIMEOUT_MS)),
    ]);
    if (outcome.timedOut) {
      return { label, passed: false, reason: `did not reject within ${REJECT_TIMEOUT_MS}ms` };
    }
    const got = outcome.chunk === undefined ? 'undefined (clean EOF)' : `a ${outcome.chunk.byteLength}-byte chunk`;
    return { label, passed: false, reason: `resolved with ${got} instead of rejecting` };
  } catch (e) {
    return { label, passed: true, msg: String(e?.message || e) };
  }
}

export async function run(hfToken) {
  console.log('xet-read-token...');
  const { accessToken, exp, casUrl } = await fetchXetReadToken({ hfToken, ...READ_REPO });

  const session = new XetSession();
  const group = await session.newDownloadStreamGroup(casUrl, accessToken, exp);

  const cases = [
    // Valid 64-hex hash that no CAS file has — exercises the remote error path.
    { label: 'nonexistent hash', fileInfo: { hash: 'a'.repeat(64), file_size: 1234 } },
    // serde_wasm_bindgen boundary: missing fields / unparseable hash.
    { label: 'empty fileInfo object', fileInfo: {} },
    { label: 'non-hex hash', fileInfo: { hash: 'not-a-hex-hash', file_size: 1234 } },
  ];

  const failures = [];
  for (const c of cases) {
    console.log(`case: ${c.label}...`);
    const r = await expectStreamReject(group, c.label, c.fileInfo);
    console.log(`  ${r.passed ? `rejected as expected: ${r.msg}` : `FAILED: ${r.reason}`}`);
    if (!r.passed) failures.push(r);
  }

  if (failures.length > 0) {
    return { ok: false, error: `${failures.length} cases failed`, failures };
  }
  return { ok: true, casesChecked: cases.length };
}
