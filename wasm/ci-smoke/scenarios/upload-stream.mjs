// End-to-end smoke for the wasm streaming upload path. Same Hub + CAS
// round-trip as upload.mjs, but exercises XetUploadCommit::uploadStream /
// XetStreamUpload::{write, finish} instead of the bytes-in-one-shot path.

import { openUploadCommit, streamRandomChunks } from '../common.mjs';

const CHUNK_SIZE = 64 * 1024; // 64 KiB per write — forces multiple stream chunks
const TOTAL_CHUNKS = 16;       // 16 * 64 KiB = 1 MiB total
const PAYLOAD_SIZE = CHUNK_SIZE * TOTAL_CHUNKS;

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  console.log(`uploadStream(...) opening, ${TOTAL_CHUNKS} chunks of ${CHUNK_SIZE} bytes...`);
  const metadata = await streamRandomChunks(commit, 'wasm-smoke-stream.bin', TOTAL_CHUNKS, CHUNK_SIZE);
  console.log('metadata:', JSON.stringify(metadata));

  console.log('commit()...');
  const commitReport = await commit.commit();
  console.log('commitReport:', JSON.stringify(commitReport));

  return {
    ok: true,
    metadata,
    commitReport,
    hash: metadata?.xet_info?.hash,
    fileSize: metadata?.xet_info?.file_size,
    payloadSize: PAYLOAD_SIZE,
  };
}
