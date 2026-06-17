// Smoke for the XetSession→multiple XetUploadCommit flow: from one XetSession,
// mint commit A (upload + commit) then commit B (upload + commit).
//
// Each XetUploadCommit constructs its own FileUploadSession (see
// xet_pkg/.../upload_commit.rs), so they must not share mutable state in a way
// that lets commit A's finalization break commit B's construction. This catches
// XetSession-level resource leaks (task runtime cleanup, locked state, etc.)
// that would surface as the second newUploadCommit() hanging or panicking.

import { openUploadCommit, randomBytes } from '../common.mjs';

const PAYLOAD_SIZE = 256 * 1024; // 256 KiB per commit

async function uploadOne(commit, name, data) {
  const meta = await commit.uploadBytes(data, 'compute', name);
  const report = await commit.commit();
  return { meta, report };
}

export async function run(hfToken) {
  console.log('commit A...');
  const { session, commit: commitA, casUrl, accessToken, exp } = await openUploadCommit(hfToken);
  const dataA = randomBytes(PAYLOAD_SIZE);
  const a = await uploadOne(commitA, 'commit-a.bin', dataA);
  console.log(`commit A hash=${a.meta?.xet_info?.hash}`);

  console.log('commit B (reusing session)...');
  const commitB = await session.newUploadCommit(casUrl, accessToken, exp);
  const dataB = randomBytes(PAYLOAD_SIZE);
  const b = await uploadOne(commitB, 'commit-b.bin', dataB);
  console.log(`commit B hash=${b.meta?.xet_info?.hash}`);

  return {
    ok: true,
    commitA: a,
    commitB: b,
    payloadSize: PAYLOAD_SIZE,
  };
}
