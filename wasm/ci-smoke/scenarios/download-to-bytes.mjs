// Single-file download via XetDownloadStreamGroup.downloadToBytes against the
// pinned READ_REPO commit. Reports byte count + content SHA-256; run.mjs
// asserts both against pinned expectations.
//
// Unlike scenarios/download.mjs (which drives the streaming path), this is the
// only wasm coverage of the writer-sink download interface —
// FileDownloadSession::download_to_writer / FileReconstructor::reconstruct_to_writer.

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

  const bytes = await group.downloadToBytes({ hash: xetHash, file_size: size });
  const total = bytes.byteLength;
  console.log(`downloadToBytes ok: ${total} bytes`);

  const sha256 = toHex(await crypto.subtle.digest('SHA-256', bytes));
  console.log(`sha256=${sha256}`);

  return { ok: true, byteCount: total, sha256, xetHash, expectedSize: size };
}
