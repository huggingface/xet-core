// Sha256Policy coverage on the wasm uploadBytes path: 'compute' must produce
// the real content SHA-256 and { provided } must be echoed verbatim, while the
// xet content hash is identical for both (the policy must never affect
// chunking or the xet hash). 'skip' is not exercised: for v1 repos, which the
// smoke-test repo is, CAS rejects shards whose file entries carry no SHA-256.
// Invalid policy values must reject locally in parse_sha256_policy, which has
// no other test coverage anywhere (the wrapper crate is wasm-only, so no
// native unit tests exist for it).

import { randomBytes, sha256Hex, openUploadCommit } from '../common.mjs';

const PAYLOAD_SIZE = 256 * 1024;

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  const payload = randomBytes(PAYLOAD_SIZE);
  const localSha = await sha256Hex(payload);
  console.log(`payload sha256=${localSha}`);

  // Invalid policy values reject in parse_sha256_policy before any upload
  // work starts, so the commit stays usable afterwards.
  const invalidCases = [
    { label: 'policy string "bogus"', policy: 'bogus' },
    { label: 'provided non-hex', policy: { provided: 'xyz' } },
    { label: 'object without provided', policy: { something: 1 } },
  ];
  const failures = [];
  for (const c of invalidCases) {
    try {
      await commit.uploadBytes(payload, c.policy, 'should-not-upload.bin');
      failures.push({ label: c.label, reason: 'expected rejection but call resolved' });
    } catch (e) {
      const msg = String(e?.message || e);
      if (!/sha256 policy/i.test(msg)) {
        failures.push({ label: c.label, reason: `error did not mention "sha256 policy": ${msg}` });
      }
    }
  }
  if (failures.length > 0) {
    return { ok: false, error: `${failures.length} invalid-policy cases failed`, failures };
  }
  console.log(`invalid policies rejected: ${invalidCases.length} cases`);

  console.log("uploadBytes x2 ('compute' / { provided })...");
  const metaCompute = await commit.uploadBytes(payload, 'compute', 'policy-compute.bin');
  const metaProvided = await commit.uploadBytes(payload, { provided: localSha }, 'policy-provided.bin');

  console.log('commit()...');
  const commitReport = await commit.commit();

  return {
    ok: true,
    localSha,
    invalidCasesChecked: invalidCases.length,
    computeSha: metaCompute?.xet_info?.sha256 ?? null,
    providedSha: metaProvided?.xet_info?.sha256 ?? null,
    hashes: [metaCompute?.xet_info?.hash, metaProvided?.xet_info?.hash],
    commitReport,
  };
}
