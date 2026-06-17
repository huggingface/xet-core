// Concurrent multi-file download in one XetDownloadStreamGroup against the
// same pinned READ_REPO commit the single-file download scenario uses.
//
// Only these two files are Xet-stored on this commit (the json/txt sidecars
// are too small to be promoted to Xet). The size delta — 540 KiB vs 26 MiB —
// is intentional: a fan-out bug that crossed stream buffers would corrupt
// content no single-file test could catch. Each download reports its content
// SHA-256, asserted against pinned values in run.mjs.

import { XetSession, READ_REPO, fetchPathsInfo, pathInfoEntry, fetchXetReadToken, sha256Hex, drainStreamToBytes } from '../common.mjs';

const FILEPATHS = ['pytorch_model.bin', 'tf_model.h5'];

export async function run(hfToken) {
  const params = { hfToken, ...READ_REPO };

  console.log(`paths-info for ${FILEPATHS.length} files...`);
  const arr = await fetchPathsInfo({ ...params, paths: FILEPATHS });
  const fileInfos = FILEPATHS.map((p) => pathInfoEntry(arr, p));
  console.log('paths-info ok:', fileInfos.map((f) => `${f.path}=${f.size}`).join(', '));

  console.log('xet-read-token...');
  const { accessToken, exp, casUrl } = await fetchXetReadToken(params);
  console.log(`token ok: casUrl=${casUrl} exp=${exp}`);

  const session = new XetSession();
  const group = await session.newDownloadStreamGroup(casUrl, accessToken, exp);

  console.log(`downloadStream x${FILEPATHS.length} concurrent...`);
  const downloads = await Promise.all(
    fileInfos.map(async (fi) => {
      const stream = await group.downloadStream({ hash: fi.xetHash, file_size: fi.size });
      const bytes = await drainStreamToBytes(stream);
      return { path: fi.path, byteCount: bytes.byteLength, expectedSize: fi.size, sha256: await sha256Hex(bytes) };
    }),
  );
  console.log('downloads done:', downloads.map((d) => `${d.path}=${d.byteCount} sha256=${d.sha256}`).join(', '));

  return { ok: true, downloads };
}
