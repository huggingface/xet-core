// Concurrent multi-file download in one XetDownloadStreamGroup against the
// same pinned READ_REPO commit the single-file download scenario uses.
//
// Only these two files are Xet-stored on this commit (the json/txt sidecars
// are too small to be promoted to Xet). The size delta — 540 KiB vs 26 MiB —
// is intentional: a fan-out bug that crossed stream buffers would produce
// mismatched byteCounts that no single-file test could miss.

import { XetSession, READ_REPO, fetchPathsInfo, pathInfoEntry, fetchXetReadToken } from '../common.mjs';

const FILEPATHS = ['pytorch_model.bin', 'tf_model.h5'];

async function drainStream(stream) {
  let total = 0;
  while (true) {
    const chunk = await stream.next();
    if (chunk === undefined) break;
    total += chunk.byteLength;
  }
  return total;
}

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
      const byteCount = await drainStream(stream);
      return { path: fi.path, byteCount, expectedSize: fi.size };
    }),
  );
  console.log('downloads done:', downloads.map((d) => `${d.path}=${d.byteCount}`).join(', '));

  return { ok: true, downloads };
}
