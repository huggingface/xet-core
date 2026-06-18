// Byte-range downloads through XetDownloadStreamGroup against the pinned
// READ_REPO file. Downloads the full file once as a reference, then three
// ranged streams (prefix, mid-file, suffix ending exactly at file_size) from
// the same group, and reports the SHA-256 of each ranged result alongside the
// SHA-256 of the corresponding reference slice. run.mjs asserts they match —
// a regression in the wasm range-reconstruction path (the only download mode
// no other scenario touches) shows up as a slice mismatch.

import { XetSession, READ_REPO, fetchPathsInfo, pathInfoEntry, fetchXetReadToken, sha256Hex, drainStreamToBytes } from '../common.mjs';

const FILEPATH = 'pytorch_model.bin';

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
  const fileInfo = { hash: xetHash, file_size: size };

  console.log('full download (reference)...');
  const full = await drainStreamToBytes(await group.downloadStream(fileInfo));
  console.log(`reference ok: ${full.byteLength} bytes`);

  // Ranges are start-inclusive, end-exclusive byte offsets. The suffix range
  // ends exactly at file_size to cover the end-boundary case.
  const RANGES = [
    { label: 'prefix', start: 0, end: 65536 },
    { label: 'mid', start: 100_000, end: 300_000 },
    { label: 'suffix', start: size - 50_000, end: size },
  ];

  const ranges = [];
  for (const r of RANGES) {
    console.log(`range download ${r.label} [${r.start}, ${r.end})...`);
    const bytes = await drainStreamToBytes(await group.downloadStream(fileInfo, r.start, r.end));
    ranges.push({
      ...r,
      byteCount: bytes.byteLength,
      sha256: await sha256Hex(bytes),
      expectedSha256: await sha256Hex(full.slice(r.start, r.end)),
    });
    console.log(`range ${r.label} ok: ${bytes.byteLength} bytes`);
  }

  return {
    ok: true,
    fullByteCount: full.byteLength,
    fullSha256: await sha256Hex(full),
    ranges,
  };
}
