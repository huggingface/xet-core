// Misuse / lifecycle error surface for the wasm upload path — the wasm
// mirror of the native upload_commit state-machine tests, which never run on
// wasm32. Commit A is aborted (after a successful uploadBytes and an aborted
// stream) and must reject further work with UserCancelled; commit B (same
// session — an aborted sibling must not poison it) commits successfully and
// must reject double-commit / post-commit uploads with AlreadyCompleted,
// and its finished stream must reject further writes. Every misuse call must
// reject — resolving or hanging is a failure (a hang trips the page timeout).

import { randomBytes, openUploadCommit } from '../common.mjs';

const PAYLOAD_SIZE = 128 * 1024;

async function expectReject(label, fn, mustMatch) {
  try {
    await fn();
    return { label, passed: false, reason: 'expected rejection but call resolved' };
  } catch (e) {
    const msg = String(e?.message || e);
    if (mustMatch && !mustMatch.test(msg)) {
      return { label, passed: false, reason: `error did not match ${mustMatch}: ${msg}` };
    }
    return { label, passed: true, msg };
  }
}

export async function run(hfToken) {
  const failures = [];
  let casesChecked = 0;
  const check = (r) => {
    casesChecked++;
    console.log(`  ${r.label}: ${r.passed ? 'rejected as expected' : `FAILED — ${r.reason}`}`);
    if (!r.passed) failures.push(r);
  };

  console.log('commit A (abort path)...');
  const { session, commit: commitA, casUrl, accessToken, exp } = await openUploadCommit(hfToken);

  const metaA = await commitA.uploadBytes(randomBytes(PAYLOAD_SIZE), 'compute', 'lifecycle-a.bin');
  if (!metaA?.xet_info?.hash) {
    return { ok: false, error: `commit A uploadBytes returned no hash: ${JSON.stringify(metaA)}` };
  }

  const streamA = await commitA.uploadStream('lifecycle-a-stream.bin', 'compute');
  await streamA.write(randomBytes(64 * 1024));
  streamA.abort();
  check(await expectReject('streamA.write after stream abort', () => streamA.write(randomBytes(1024))));

  commitA.abort();
  check(await expectReject('commitA.uploadBytes after abort', () => commitA.uploadBytes(randomBytes(1024), 'compute', 'x.bin'), /cancel/i));
  check(await expectReject('commitA.commit after abort', () => commitA.commit(), /cancel/i));

  console.log('commit B (commit path, same session)...');
  const commitB = await session.newUploadCommit(casUrl, accessToken, exp);
  const metaB = await commitB.uploadBytes(randomBytes(PAYLOAD_SIZE), 'compute', 'lifecycle-b.bin');
  const streamB = await commitB.uploadStream('lifecycle-b-stream.bin', 'compute');
  await streamB.write(randomBytes(64 * 1024));
  const streamBMeta = await streamB.finish();
  check(await expectReject('streamB.write after finish', () => streamB.write(randomBytes(1024))));

  console.log('commit B commit()...');
  const commitReport = await commitB.commit();
  check(await expectReject('commitB.commit second call', () => commitB.commit(), /already|complet/i));
  check(await expectReject('commitB.uploadBytes after commit', () => commitB.uploadBytes(randomBytes(1024), 'compute', 'y.bin'), /already|complet/i));

  if (failures.length > 0) {
    return { ok: false, error: `${failures.length} lifecycle cases failed`, failures };
  }
  return {
    ok: true,
    casesChecked,
    bytesHash: metaB?.xet_info?.hash,
    streamHash: streamBMeta?.xet_info?.hash,
    commitReport,
  };
}
