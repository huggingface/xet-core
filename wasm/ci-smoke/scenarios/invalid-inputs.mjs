// No-Hub smoke: resolve_auth_inputs in the wasm wrapper must reject bad
// token / endpoint / tokenExpiry / tokenRefresh* / customHeaders inputs across
// both newUploadCommit and newDownloadStreamGroup. Catches regressions to the
// validation surface (e.g., accidental re-introduction of the 0→u64::MAX
// sentinel), and that both methods wire their arguments in the same order.
//
// No network calls — `init()` and the constructor are sufficient. The
// tokenRefresh* cases must fail validation before any refresh GET, so the
// refresh URLs below point at an unroutable host.
//
// Each case below expects newUploadCommit / newDownloadStreamGroup to
// reject with an Error whose message contains the listed substring.

import { XetSession } from '../common.mjs';

// Far-future Unix timestamp for cases that need a valid tokenExpiry.
const VALID_EXPIRY = 4102444800; // 2100-01-01
const VALID_TOKEN = 'placeholder-token';
const VALID_ENDPOINT = 'https://cas-server.invalid';
const VALID_REFRESH_URL = 'https://hub.invalid/api/models/ns/repo/xet-read-token/main';

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

    // token / tokenExpiry must be supplied as a pair
    { label: 'expiry without token',  endpoint: VALID_ENDPOINT, token: null,        expiry: VALID_EXPIRY, mustContain: 'together' },
    { label: 'token without expiry',  endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: null,         mustContain: 'together' },

    // with no tokenRefreshUrl, endpoint and token info are both mandatory
    { label: 'no token, no refresh url',    endpoint: VALID_ENDPOINT, token: null, expiry: null, mustContain: 'token' },
    { label: 'no endpoint, no refresh url', endpoint: null,           token: null, expiry: null, mustContain: 'endpoint' },

    // tokenRefreshUrl / tokenRefreshHeaders validation
    {
      label: 'refresh url missing scheme',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      refreshUrl: 'hub.invalid/api/models/ns/repo/xet-read-token/main',
      mustContain: 'tokenRefreshUrl',
    },
    {
      label: 'refresh headers without url',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      refreshHeaders: { Authorization: 'Bearer placeholder' },
      mustContain: 'tokenRefreshHeaders',
    },
    {
      label: 'refresh headers not an object',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      refreshUrl: VALID_REFRESH_URL, refreshHeaders: 'Bearer placeholder',
      mustContain: 'tokenRefreshHeaders',
    },
    {
      label: 'refresh header value not a string',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      refreshUrl: VALID_REFRESH_URL, refreshHeaders: { Authorization: 42 },
      mustContain: 'tokenRefreshHeaders',
    },
    {
      label: 'refresh header name invalid',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      refreshUrl: VALID_REFRESH_URL, refreshHeaders: { 'Bad Header': 'value' },
      mustContain: 'header name',
    },

    // customHeaders validation — same parser as the refresh headers, but with
    // no tokenRefreshUrl requirement
    {
      label: 'custom headers not an object',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      customHeaders: 'X-Trace: 1',
      mustContain: 'customHeaders',
    },
    {
      label: 'custom header value not a string',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      customHeaders: { 'X-Trace': 1 },
      mustContain: 'customHeaders',
    },
    {
      label: 'custom header name invalid',
      endpoint: VALID_ENDPOINT, token: VALID_TOKEN, expiry: VALID_EXPIRY,
      customHeaders: { 'Bad Header': 'value' },
      mustContain: 'header name',
    },
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
    const args = [c.endpoint, c.token, c.expiry, c.refreshUrl, c.refreshHeaders, c.customHeaders];

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
