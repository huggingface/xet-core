// Heterogeneous upload handles in a single XetUploadCommit: one file via
// uploadBytes, another via uploadStream, both committed together. Real users
// mix both — this verifies the file-handle aggregation in
// XetUploadCommit::commit() handles both handle types side-by-side and
// merges them into the same commitReport.uploads map.
//
// The two payloads are distinct random buffers so dedup cannot collapse
// them and run.mjs can verify both ended up in the report with distinct
// hashes.

import { openUploadCommit, randomBytes, streamRandomChunks } from '../common.mjs';

const BYTES_SIZE = 256 * 1024;     // 256 KiB single-shot
const STREAM_CHUNK = 64 * 1024;    // 64 KiB per write
const STREAM_CHUNKS = 4;           // 4 * 64 KiB = 256 KiB streamed
const STREAM_SIZE = STREAM_CHUNK * STREAM_CHUNKS;

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  // Kick off both flows concurrently so their finalize_ingestion /
  // stream.finish completions interleave on the single-threaded wasm
  // executor — catches any handle-tracking races where one path
  // clobbers the other's metadata before commit() aggregates them.
  console.log('uploadBytes + uploadStream concurrently...');

  const bytesPromise = (async () => {
    const data = randomBytes(BYTES_SIZE);
    return commit.uploadBytes(data, 'compute', 'via-bytes.bin');
  })();

  const streamPromise = streamRandomChunks(commit, 'via-stream.bin', STREAM_CHUNKS, STREAM_CHUNK);

  const [bytesMeta, streamMeta] = await Promise.all([bytesPromise, streamPromise]);
  console.log(`uploaded: bytes=${bytesMeta?.xet_info?.hash} stream=${streamMeta?.xet_info?.hash}`);

  console.log('commit()...');
  const commitReport = await commit.commit();
  console.log('commitReport:', JSON.stringify(commitReport));

  return {
    ok: true,
    bytesMeta,
    streamMeta,
    commitReport,
    bytesSize: BYTES_SIZE,
    streamSize: STREAM_SIZE,
  };
}
