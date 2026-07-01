// Concurrent multi-file uploadBytes in a single XetUploadCommit. Each file
// is a distinct random buffer so dedup cannot collapse them; all FILE_COUNT
// uploads must surface in commitReport.uploads with distinct xet hashes
// (asserted in run.mjs).

import { openUploadCommit, randomBytes } from '../common.mjs';

const FILE_COUNT = 3;
const FILE_SIZE = 512 * 1024; // 512 KiB per file → 1.5 MiB total

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  console.log(`uploadBytes x${FILE_COUNT} (${FILE_SIZE} bytes each, concurrent)...`);
  const uploadResults = await Promise.all(
    Array.from({ length: FILE_COUNT }, (_, i) => {
      const data = randomBytes(FILE_SIZE);
      return commit.uploadBytes(data, 'compute', `wasm-smoke-multi-${i}.bin`);
    }),
  );
  console.log('uploads done:', uploadResults.map((m) => m?.xet_info?.hash));

  console.log('commit()...');
  const commitReport = await commit.commit();
  console.log('commitReport:', JSON.stringify(commitReport));

  return {
    ok: true,
    uploadResults,
    commitReport,
    hashes: uploadResults.map((m) => m?.xet_info?.hash),
    fileSizes: uploadResults.map((m) => m?.xet_info?.file_size),
    expectedFileCount: FILE_COUNT,
    expectedFileSize: FILE_SIZE,
  };
}
