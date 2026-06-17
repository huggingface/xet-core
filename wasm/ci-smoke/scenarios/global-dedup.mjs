// Cross-session global dedup against production CAS: download the pre-seeded
// deterministic file from WRITE_REPO, re-upload its bytes in a fresh commit,
// and expect every chunk to dedup against the HMAC-keyed global-dedup shard CAS
// returns for the chunk-0 query — zero new xorbs (run.mjs asserts
// xorb_bytes_uploaded === 0).
//
// This is the e2e regression guard for the keyed-shard lookup in
// xet_data/src/processing/shard_interface/wasm.rs: prod CAS keys the chunk
// hashes in returned shards, so if the wasm dedup cache stops applying the
// per-shard HMAC key at query time the lookup silently never matches and the
// re-upload pushes a full payload's worth of xorbs.
//
// Deterministic because CAS indexes every file's first chunk in the
// global-dedup index and the client always queries chunk 0
// (file_deduplication.rs); the fetched shard then covers the whole file.
//
// First run against an unseeded repo bootstraps: it uploads the seed payload
// and commits it to the Hub (so paths-info sees it and GC keeps its xorbs). If
// the browser-side Hub commit breaks (e.g. CORS), seed manually per ../seed.mjs.

import {
  XetSession,
  WRITE_REPO,
  fetchPathsInfo,
  fetchXetReadToken,
  openUploadCommit,
  downloadAllBytes,
  commitFileToHub,
  sha256Hex,
} from '../common.mjs';
import { SEED_PATH, SEED_SIZE, SEED_SHA256, seedPayload } from '../seed.mjs';

const REUPLOAD_NAME = 'global-dedup-reupload.bin';

export async function run(hfToken) {
  const payload = seedPayload();
  const payloadSha = await sha256Hex(payload);
  if (payloadSha !== SEED_SHA256) {
    throw new Error(
      `seed generator drifted: sha256=${payloadSha}, pinned ${SEED_SHA256} — bump the version in SEED_PATH and re-seed`,
    );
  }

  console.log('paths-info...');
  const arr = await fetchPathsInfo({ hfToken, ...WRITE_REPO, paths: [SEED_PATH] });
  const entry = arr.find((e) => e.path === SEED_PATH);

  if (!entry) {
    console.log(`seed file ${SEED_PATH} absent — bootstrapping`);
    const { commit } = await openUploadCommit(hfToken);
    const meta = await commit.uploadBytes(payload, 'compute', SEED_PATH);
    console.log(`upload ok: hash=${meta.xet_info.hash} sha256=${meta.xet_info.sha256}`);
    await commit.commit();
    console.log('CAS commit ok, committing to hub...');
    await commitFileToHub({
      hfToken,
      ...WRITE_REPO,
      path: SEED_PATH,
      sha256: meta.xet_info.sha256,
      size: SEED_SIZE,
      summary: 'seed file for the global-dedup ci smoke',
    });
    console.log('hub commit ok');
    return { ok: true, bootstrapped: true, seedXetHash: meta.xet_info.hash };
  }

  if (!entry.xetHash || entry.size !== SEED_SIZE) {
    throw new Error(
      `seed file ${SEED_PATH} exists but is unusable (xetHash=${entry.xetHash}, size=${entry.size}, ` +
        `expected size=${SEED_SIZE}) — delete it from ${WRITE_REPO.namespace}/${WRITE_REPO.repo} to re-bootstrap`,
    );
  }
  console.log(`seed ok: xetHash=${entry.xetHash} size=${entry.size}`);

  console.log('xet-read-token...');
  const readToken = await fetchXetReadToken({ hfToken, ...WRITE_REPO });
  // Dedicated session for the download so the re-upload's dedup can only
  // come from the global-dedup query, not shared session state.
  const downloadSession = new XetSession();
  const bytes = await downloadAllBytes(downloadSession, readToken, {
    hash: entry.xetHash,
    file_size: entry.size,
  });
  console.log(`download ok: ${bytes.byteLength} bytes`);

  const downloadedSha = await sha256Hex(bytes);
  if (bytes.byteLength !== SEED_SIZE || downloadedSha !== SEED_SHA256) {
    throw new Error(
      `downloaded seed mismatch: ${bytes.byteLength} bytes sha256=${downloadedSha}, ` +
        `expected ${SEED_SIZE} bytes sha256=${SEED_SHA256}`,
    );
  }

  const { commit } = await openUploadCommit(hfToken);
  const meta = await commit.uploadBytes(bytes, 'compute', REUPLOAD_NAME);
  console.log(`re-upload ingested: hash=${meta.xet_info.hash}`);
  const commitReport = await commit.commit();
  console.log(`commit ok: dedup_metrics=${JSON.stringify(commitReport.dedup_metrics)}`);

  return {
    ok: true,
    bootstrapped: false,
    byteCount: bytes.byteLength,
    seedXetHash: entry.xetHash,
    uploadHash: meta.xet_info.hash,
    commitReport,
  };
}
