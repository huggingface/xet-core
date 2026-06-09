// Single-file download through XetSession against the pinned READ_REPO
// commit. Reports byte count + content SHA-256; run.mjs asserts both
// against pinned expectations.

import { XetSession, READ_REPO, fetchPathsInfo, pathInfoEntry, fetchXetReadToken } from '../common.mjs';

const FILEPATH = 'pytorch_model.bin';

function toHex(buf) {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export async function run(hfToken) {
  const params = { hfToken, ...READ_REPO };

  console.log('paths-info...');
  const arr = await fetchPathsInfo({ ...params, paths: [FILEPATH] });
  const { xetHash, size } = pathInfoEntry(arr, FILEPATH);
  console.log(`paths-info ok: xetHash=${xetHash} size=${size}`);

  console.log('xet-read-token...');
  const { accessToken, exp, casUrl } = await fetchXetReadToken(params);
  console.log(`token ok: casUrl=${casUrl}`);

  const session = new XetSession();
  const group = await session.newDownloadStreamGroup(casUrl, accessToken, exp);
  const stream = await group.downloadStream({ hash: xetHash, file_size: size });

  const chunks = [];
  let total = 0;
  while (true) {
    const chunk = await stream.next();
    if (chunk === undefined) break;
    chunks.push(chunk);
    total += chunk.byteLength;
  }
  console.log(`download ok: ${total} bytes`);

  const buf = await new Blob(chunks).arrayBuffer();
  const sha256 = toHex(await crypto.subtle.digest('SHA-256', buf));
  console.log(`sha256=${sha256}`);

  return { ok: true, byteCount: total, sha256, xetHash, expectedSize: size };
}
