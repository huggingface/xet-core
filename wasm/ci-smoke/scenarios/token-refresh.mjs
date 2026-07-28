// End-to-end smoke for the wasm URL-based token refresher.
//
// Builds a download stream group from a `tokenRefreshUrl` alone — no endpoint,
// no token, no expiry — so the group can only work if the wrapper resolves all
// three by calling the Hub `xet-read-token` route itself during build(). That
// also exercises the refresh GET through reqwest's wasm backend, which is
// subject to browser CORS on the Hub route.
//
// Downloads the same pinned READ_REPO file as the `download` scenario and
// reports byte count + content SHA-256; run.mjs asserts both against the same
// pinned expectations.

import {
  XetSession,
  READ_REPO,
  fetchPathsInfo,
  pathInfoEntry,
  xetTokenUrl,
  refreshHeaders,
  drainStreamToBytes,
  sha256Hex,
} from '../common.mjs';

const FILEPATH = 'pytorch_model.bin';

export async function run(hfToken) {
  console.log('paths-info...');
  const arr = await fetchPathsInfo({ hfToken, ...READ_REPO, paths: [FILEPATH] });
  const { xetHash, size } = pathInfoEntry(arr, FILEPATH);
  console.log(`paths-info ok: xetHash=${xetHash} size=${size}`);

  const tokenRefreshUrl = xetTokenUrl(READ_REPO, 'read');
  console.log(`tokenRefreshUrl=${tokenRefreshUrl}`);

  // Deliberately no endpoint / token / tokenExpiry: the eager refresh during
  // build() must supply the casUrl and the initial CAS token.
  const session = new XetSession();
  const group = await session.newDownloadStreamGroup(
    null,
    null,
    null,
    tokenRefreshUrl,
    refreshHeaders(hfToken),
  );
  console.log('group built from tokenRefreshUrl alone');

  const stream = await group.downloadStream({ hash: xetHash, file_size: size });
  const bytes = await drainStreamToBytes(stream);
  console.log(`download ok: ${bytes.byteLength} bytes`);

  const sha256 = await sha256Hex(bytes);
  console.log(`sha256=${sha256}`);

  return { ok: true, byteCount: bytes.byteLength, sha256, xetHash, expectedSize: size };
}
