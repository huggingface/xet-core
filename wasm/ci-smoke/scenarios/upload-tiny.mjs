// Boundary-condition uploads: 0-byte and 1-byte files. These hit edge cases
// in chunking (no chunk vs single tiny chunk) and xorb cutting (empty xorb
// suppression — see file_upload_session.rs at the
// `if xorb.num_bytes() == 0 { return Ok(true); }` arm).
//
// We upload both in one XetUploadCommit alongside a normal-sized file
// so the commit isn't entirely empty (an all-empty commit would push
// no xorbs, which is its own edge case worth keeping out of this test).

import { openUploadCommit } from '../common.mjs';

const NORMAL_SIZE = 64 * 1024; // 64 KiB — fits in one chunk

export async function run(hfToken) {
  const { commit } = await openUploadCommit(hfToken);

  console.log('uploadBytes(empty)...');
  const empty = new Uint8Array(0);
  const emptyMeta = await commit.uploadBytes(empty, 'compute', 'empty.bin');

  console.log('uploadBytes(1 byte)...');
  const oneByte = new Uint8Array([0x42]);
  const oneByteMeta = await commit.uploadBytes(oneByte, 'compute', 'one-byte.bin');

  console.log(`uploadBytes(${NORMAL_SIZE} bytes)...`);
  const normal = new Uint8Array(NORMAL_SIZE);
  crypto.getRandomValues(normal);
  const normalMeta = await commit.uploadBytes(normal, 'compute', 'normal.bin');

  console.log('commit()...');
  const commitReport = await commit.commit();
  console.log('commitReport:', JSON.stringify(commitReport));

  return {
    ok: true,
    empty: emptyMeta,
    oneByte: oneByteMeta,
    normal: normalMeta,
    commitReport,
    normalSize: NORMAL_SIZE,
  };
}
