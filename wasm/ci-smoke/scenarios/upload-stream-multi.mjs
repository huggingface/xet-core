// Concurrent multi-file uploadStream in a single XetUploadCommit. Each
// stream writes distinct random chunks so dedup cannot collapse them; all
// FILE_COUNT streams must surface in commitReport.uploads with distinct xet
// hashes (asserted in run.mjs).
//
// Each stream lives in its own async task and writes CHUNKS_PER_FILE x
// CHUNK_SIZE bytes interleaved with the others.

import { openUploadCommit, streamRandomChunks } from '../common.mjs';

const FILE_COUNT = 3;
const CHUNK_SIZE = 64 * 1024;     // 64 KiB per write — fits crypto.getRandomValues cap
const CHUNKS_PER_FILE = 8;        // 8 * 64 KiB = 512 KiB per file → 1.5 MiB total
const FILE_SIZE = CHUNK_SIZE * CHUNKS_PER_FILE;

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  console.log(`uploadStream x${FILE_COUNT} (${CHUNKS_PER_FILE}x${CHUNK_SIZE} each, concurrent)...`);
  const uploadResults = await Promise.all(
    Array.from({ length: FILE_COUNT }, (_, i) =>
      streamRandomChunks(commit, `wasm-smoke-stream-multi-${i}.bin`, CHUNKS_PER_FILE, CHUNK_SIZE),
    ),
  );
  console.log('streams done:', uploadResults.map((m) => m?.xet_info?.hash));

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
