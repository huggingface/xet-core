// No-Hub smoke: validate_session_inputs in the wasm wrapper must reject
// bad token / endpoint / tokenExpiry inputs across both newUploadCommit
// and newDownloadStreamGroup. Catches regressions to the validation
// surface (e.g., accidental re-introduction of the 0→u64::MAX sentinel).
//
// No network calls — `init()` and the constructor are sufficient.
//
// Each case below expects newUploadCommit / newDownloadStreamGroup to
// reject with an Error whose message contains the listed substring.

import { XetSession } from '../common.mjs';

// Far-future Unix timestamp for cases that need a valid tokenExpiry.
const VALID_EXPIRY = 4102444800; // 2100-01-01
const VALID_TOKEN = 'placeholder-token';
const VALID_ENDPOINT = 'https://cas-server.invalid';

function makeCases() {
  return [
    // tokenExpiry validation
    { label: 'expiry=0',          endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: 0,            mustContain: 'tokenExpiry' },
    { label: 'expiry=-1',         endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: -1,           mustContain: 'tokenExpiry' },
    { label: 'expiry=NaN',        endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: Number.NaN,   mustContain: 'tokenExpiry' },
    { label: 'expiry=+Infinity',  endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: Number.POSITIVE_INFINITY, mustContain: 'tokenExpiry' },

    // token validation
    { label: 'token=""',          endpoint: VALID_ENDPOINT, token: '',           expiry: VALID_EXPIRY, mustContain: 'token' },
    { label: 'token="   "',       endpoint: VALID_ENDPOINT, token: '   ',        expiry: VALID_EXPIRY, mustContain: 'token' },

    // endpoint validation
    { label: 'endpoint=""',                  endpoint: '',                     token: VALID_TOKEN, expiry: VALID_EXPIRY, mustContain: 'endpoint' },
    { label: 'endpoint missing scheme',      endpoint: 'cas-server.invalid',   token: VALID_TOKEN, expiry: VALID_EXPIRY, mustContain: 'endpoint' },
    { label: 'endpoint=ftp://...',           endpoint: 'ftp://cas.invalid',    token: VALID_TOKEN, expiry: VALID_EXPIRY, mustContain: 'endpoint' },
  ];
}

async function expectReject(label, method, args, mustContain) {
  try {
    await method(...args);
    return { label, passed: false, reason: 'expected rejection but call resolved' };
  } catch (e) {
    const msg = String(e?.message || e);
    if (!msg.toLowerCase().includes(mustContain.toLowerCase())) {
      return { label, passed: false, reason: `error message did not mention "${mustContain}": ${msg}` };
    }
    return { label, passed: true, msg };
  }
}

export async function run() {
  const session = new XetSession();
  const cases = makeCases();
  const failures = [];

  for (const c of cases) {
    const args = [c.endpoint, c.token, c.expiry];

    const uploadResult = await expectReject(
      `newUploadCommit / ${c.label}`,
      (...a) => session.newUploadCommit(...a),
      args,
      c.mustContain,
    );
    if (!uploadResult.passed) failures.push(uploadResult);

    const dlResult = await expectReject(
      `newDownloadStreamGroup / ${c.label}`,
      (...a) => session.newDownloadStreamGroup(...a),
      args,
      c.mustContain,
    );
    if (!dlResult.passed) failures.push(dlResult);
  }

  if (failures.length > 0) {
    return { ok: false, error: `${failures.length} cases failed`, failures };
  }
  return { ok: true, casesChecked: cases.length * 2 };
}
