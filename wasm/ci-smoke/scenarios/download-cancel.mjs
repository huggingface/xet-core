// cancel() on an in-flight download stream. Reads one chunk of the large pinned
// file, cancels, and requires the next next() to resolve `undefined` promptly
// (the contract is "subsequent next() returns None"); a hang would mean the
// wasm cancel path stopped tearing down the reconstruction task. Then downloads
// the small pinned file in full from the same group to prove a cancelled stream
// poisons neither the group nor the session.

import { XetSession, READ_REPO, fetchPathsInfo, pathInfoEntry, fetchXetReadToken, sha256Hex, drainStreamToBytes } from '../common.mjs';

const BIG = 'tf_model.h5';
const SMALL = 'pytorch_model.bin';
const POST_CANCEL_TIMEOUT_MS = 15_000;

export async function run(hfToken) {
  const params = { hfToken, ...READ_REPO };

  console.log('paths-info...');
  const arr = await fetchPathsInfo({ ...params, paths: [BIG, SMALL] });
  const big = pathInfoEntry(arr, BIG);
  const small = pathInfoEntry(arr, SMALL);

  console.log('xet-read-token...');
  const { accessToken, exp, casUrl } = await fetchXetReadToken(params);

  const session = new XetSession();
  const group = await session.newDownloadStreamGroup(casUrl, accessToken, exp);

  console.log(`starting ${BIG} download (${big.size} bytes)...`);
  const stream = await group.downloadStream({ hash: big.xetHash, file_size: big.size });
  const first = await stream.next();
  if (first === undefined || !(first.byteLength > 0)) {
    return { ok: false, error: `first chunk invalid: ${first === undefined ? 'undefined' : `${first.byteLength} bytes`}` };
  }
  console.log(`first chunk: ${first.byteLength} bytes; cancelling...`);

  // The pending next() above has resolved, so this cancel() cannot trip the
  // documented wasm-bindgen "recursive use of an object detected" hazard.
  stream.cancel();

  const after = await Promise.race([
    stream.next().then((chunk) => ({ resolved: true, chunk })),
    new Promise((resolve) => setTimeout(() => resolve({ resolved: false }), POST_CANCEL_TIMEOUT_MS)),
  ]);
  if (!after.resolved) {
    return { ok: false, error: `next() did not resolve within ${POST_CANCEL_TIMEOUT_MS}ms after cancel()` };
  }
  if (after.chunk !== undefined) {
    return { ok: false, error: `next() after cancel() returned a ${after.chunk.byteLength}-byte chunk, expected undefined` };
  }
  console.log('next() after cancel resolved undefined');

  console.log(`downloading ${SMALL} from the same group...`);
  const smallStream = await group.downloadStream({ hash: small.xetHash, file_size: small.size });
  const bytes = await drainStreamToBytes(smallStream);
  const smallSha256 = await sha256Hex(bytes);
  console.log(`post-cancel download ok: ${bytes.byteLength} bytes sha256=${smallSha256}`);

  return {
    ok: true,
    firstChunkBytes: first.byteLength,
    postCancelUndefined: true,
    smallByteCount: bytes.byteLength,
    smallSha256,
  };
}
