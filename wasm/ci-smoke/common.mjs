// Browser-side helpers shared by the ci-smoke scenario modules
// (`scenarios/*.mjs`, loaded via `harness.html`).
//
// Upload scenarios target the dataset xet-team/xet-wasm-test on prod hub:
// they mint a real xet-write-token and push xorbs + shard to CAS via
// commit(). Except for the global-dedup scenario's one-time seed bootstrap
// (which commits the seed file via the Hub commit API so it stays visible
// to paths-info and GC-pinned), the Hub commit API is never called — orphan
// xorbs are reclaimed by CAS GC.

import init, { XetSession } from '../hf_xet_wasm/pkg/hf_xet_wasm.js';

export { XetSession };

export async function initWasm() {
  await init();
}

export const HUB_BASE = 'https://huggingface.co';

// Write target for the upload scenarios. The token forwarded by run.mjs must
// have write scope here.
export const WRITE_REPO = {
  repoType: 'dataset',
  namespace: 'xet-team',
  repo: 'xet-wasm-test',
  revision: 'main',
};

// Pinned read source for the download scenarios. Pinned to a specific commit
// of hf-internal-testing/tiny-random-bert so silent drift on `main` cannot
// break the smoke without surfacing the cause. Bump alongside EXPECTED_SHA256
// / EXPECTED_SIZE in the run.mjs scenario table when re-pinning.
export const READ_REPO = {
  repoType: 'model',
  namespace: 'hf-internal-testing',
  repo: 'tiny-random-bert',
  revision: 'f171d7baecaf37b5da5a3616d8833b9969753535',
};

function authHeaders(hfToken, extra = {}) {
  const headers = { ...extra };
  if (hfToken) headers['Authorization'] = `Bearer ${hfToken}`;
  return headers;
}

export async function fetchPathsInfo({ hfToken, repoType, namespace, repo, revision, paths }) {
  const url = `${HUB_BASE}/api/${repoType}s/${namespace}/${repo}/paths-info/${revision}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: authHeaders(hfToken, { 'Content-Type': 'application/json' }),
    body: JSON.stringify({ paths, expand: false }),
  });
  if (!resp.ok) throw new Error(`paths-info ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

export function pathInfoEntry(arr, path) {
  const entry = arr.find((e) => e.path === path);
  if (!entry?.xetHash) {
    throw new Error(`paths-info missing xetHash for ${path}: ${JSON.stringify(entry)}`);
  }
  return { path, xetHash: entry.xetHash, size: entry.size };
}

export async function fetchXetReadToken({ hfToken, repoType, namespace, repo, revision }) {
  const url = `${HUB_BASE}/api/${repoType}s/${namespace}/${repo}/xet-read-token/${revision}`;
  const resp = await fetch(url, { method: 'GET', headers: authHeaders(hfToken) });
  if (!resp.ok) throw new Error(`xet-read-token ${resp.status}: ${await resp.text()}`);
  const json = await resp.json();
  if (!json.accessToken || !json.casUrl) {
    throw new Error(`xet-read-token missing fields: ${JSON.stringify(json)}`);
  }
  return json;
}

export async function fetchXetWriteToken({ hfToken, repoType, namespace, repo, revision }) {
  const url = `${HUB_BASE}/api/${repoType}s/${namespace}/${repo}/xet-write-token/${revision}`;
  const resp = await fetch(url, { method: 'GET', headers: authHeaders(hfToken) });
  if (!resp.ok) throw new Error(`xet-write-token ${resp.status}: ${await resp.text()}`);
  const json = await resp.json();
  if (!json.accessToken || !json.casUrl || !json.exp) {
    throw new Error(`xet-write-token missing fields: ${JSON.stringify(json)}`);
  }
  return json;
}

// crypto.getRandomValues only fills up to 64 KiB per call.
export function randomBytes(n) {
  const buf = new Uint8Array(n);
  for (let off = 0; off < buf.byteLength; off += 65536) {
    crypto.getRandomValues(buf.subarray(off, Math.min(off + 65536, buf.byteLength)));
  }
  return buf;
}

// Mint a write token for WRITE_REPO and open a XetUploadCommit on a fresh
// XetSession. Returns the session and token fields too, for scenarios that
// open additional commits.
export async function openUploadCommit(hfToken) {
  console.log('xet-write-token...');
  const { accessToken, exp, casUrl } = await fetchXetWriteToken({ hfToken, ...WRITE_REPO });
  console.log(`token ok: casUrl=${casUrl} exp=${exp}`);
  const session = new XetSession();
  const commit = await session.newUploadCommit(casUrl, accessToken, exp);
  return { session, commit, casUrl, accessToken, exp };
}

export async function sha256Hex(bytes) {
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

// Download a whole xet file into a single Uint8Array via a download stream
// group on the given session.
export async function downloadAllBytes(session, { accessToken, exp, casUrl }, { hash, file_size }) {
  const group = await session.newDownloadStreamGroup(casUrl, accessToken, exp);
  const stream = await group.downloadStream({ hash, file_size });

  const chunks = [];
  let total = 0;
  while (true) {
    const chunk = await stream.next();
    if (chunk === undefined) break;
    chunks.push(chunk);
    total += chunk.byteLength;
  }

  const buf = new Uint8Array(total);
  let off = 0;
  for (const chunk of chunks) {
    buf.set(chunk, off);
    off += chunk.byteLength;
  }
  return buf;
}

// Commit an already-CAS-uploaded xet file to the Hub via the NDJSON commit
// API, making it visible to paths-info and pinning its xorbs against GC.
// `sha256` is the file's content hash as returned in
// XetFileMetadata.xet_info.sha256 by uploadBytes/finish.
export async function commitFileToHub({ hfToken, repoType, namespace, repo, revision, path, sha256, size, summary }) {
  const url = `${HUB_BASE}/api/${repoType}s/${namespace}/${repo}/commit/${revision}`;
  const body = [
    JSON.stringify({ key: 'header', value: { summary } }),
    JSON.stringify({ key: 'lfsFile', value: { path, algo: 'sha256', oid: sha256, size } }),
  ].join('\n');
  const resp = await fetch(url, {
    method: 'POST',
    headers: authHeaders(hfToken, { 'Content-Type': 'application/x-ndjson' }),
    body,
  });
  if (!resp.ok) throw new Error(`hub commit ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

// Open an uploadStream and write `chunkCount` random buffers of `chunkSize`
// bytes each, then finish(). chunkSize must be <= 64 KiB (the
// crypto.getRandomValues cap).
export async function streamRandomChunks(commit, trackingName, chunkCount, chunkSize) {
  const stream = await commit.uploadStream(trackingName, 'compute');
  for (let i = 0; i < chunkCount; i++) {
    const chunk = new Uint8Array(chunkSize);
    crypto.getRandomValues(chunk);
    await stream.write(chunk);
  }
  return stream.finish();
}
