// End-to-end smoke for the wasm upload path. Drives uploadBytes (chunking +
// sha256 + xorb serialization on tokio_with_wasm::task::spawn_blocking) and
// commit() (xorb + shard push to CAS) against WRITE_REPO.

import { openUploadCommit, randomBytes } from '../common.mjs';

const PAYLOAD_SIZE = 1 * 1024 * 1024; // 1 MiB — enough to force multiple chunks

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  const data = randomBytes(PAYLOAD_SIZE);

  console.log(`uploadBytes(${data.byteLength})...`);
  const metadata = await commit.uploadBytes(data, 'compute', 'wasm-smoke.bin');
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
