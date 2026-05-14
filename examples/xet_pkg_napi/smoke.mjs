// Smoke driver: load the napi addon, fetch a public Xet file's metadata from
// the HuggingFace Hub, then download the file via the binding.
//
// Run after `npm run build` (or `npm run build:debug`):
//
//   node smoke.mjs
//
// Optional env vars (defaults pick a tiny ~540KB public Xet file):
//   HF_ENDPOINT   default: https://huggingface.co
//   HF_REPO_TYPE  default: model       (model | dataset | space)
//   HF_REPO       default: hf-internal-testing/tiny-random-bert
//   HF_BRANCH     default: main
//   HF_FILENAME   default: pytorch_model.bin
//   HF_TOKEN      optional; required for private repos
//   HF_DEST_DIR   default: ./downloads

import { createRequire } from "node:module";
import { mkdirSync, statSync, rmSync } from "node:fs";
import { join, basename } from "node:path";

const require = createRequire(import.meta.url);
const addon = require("./index.js");

console.log("loaded addon, exports:", Object.keys(addon));

const endpoint = process.env.HF_ENDPOINT ?? "https://huggingface.co";
const repoType = process.env.HF_REPO_TYPE ?? "model";
const repoId   = process.env.HF_REPO      ?? "hf-internal-testing/tiny-random-bert";
const branch   = process.env.HF_BRANCH    ?? "main";
const filename = process.env.HF_FILENAME  ?? "pytorch_model.bin";
const token    = process.env.HF_TOKEN ?? null;
const destDir  = process.env.HF_DEST_DIR  ?? "./downloads";

const repoPathSegment = repoType === "model" ? "" : `${repoType}s/`;
const apiTypeSegment  = `${repoType}s`;

const resolveUrl =
  `${endpoint}/${repoPathSegment}${repoId}/resolve/${branch}/${filename}`;
const tokenRefreshUrl =
  `${endpoint}/api/${apiTypeSegment}/${repoId}/xet-read-token/${branch}`;

console.log(`\nFetching xet metadata for ${repoType}:${repoId}/${filename}@${branch}`);
console.log(`  ${resolveUrl}`);

// HEAD against /resolve/ — Cloudfront strips X-Xet-Hash on cache hits served
// to default UAs, so spoof a hf-xet-style User-Agent + cache-bust the URL.
const headResp = await fetch(`${resolveUrl}?_=${Date.now()}`, {
    method: "HEAD",
    redirect: "manual",
    headers: {
        "User-Agent": "xet-pkg-napi-smoke/0.1",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
});

if (headResp.status !== 302 && headResp.status !== 200) {
    throw new Error(
        `HEAD ${resolveUrl} returned ${headResp.status} ${headResp.statusText}` +
        (token ? "" : " — set HF_TOKEN if the repo is private"),
    );
}

const xetHash = headResp.headers.get("x-xet-hash");
const linkedSize = headResp.headers.get("x-linked-size");
const linkedEtag = headResp.headers.get("x-linked-etag");

if (!xetHash || !linkedSize) {
    throw new Error(
        "Hub did not return X-Xet-Hash / X-Linked-Size headers — is this a Xet-stored file?",
    );
}

const sha256 = linkedEtag ? linkedEtag.replace(/"/g, "") : null;
const fileSize = Number(linkedSize);
console.log(`  xet-hash:    ${xetHash}`);
console.log(`  size:        ${fileSize.toLocaleString()} bytes`);
console.log(`  sha256:      ${sha256 ?? "(unknown)"}`);

mkdirSync(destDir, { recursive: true });
const destPath = join(destDir, basename(filename));
rmSync(destPath, { force: true });

addon.initLogging("xet_pkg_napi/0.0.1 smoke");

const smokeResult = addon.smokeTest();
console.log(`smokeTest: ${smokeResult}`);

console.log(`\nDownloading -> ${destPath}`);
const t0 = Date.now();
const result = addon.downloadFile({
    tokenRefreshUrl,
    authToken: token,
    xetHash,
    fileSize,
    sha256,
    destPath,
});
const elapsedSec = (Date.now() - t0) / 1000;

const stat = statSync(destPath);
console.log(`\nResult:`);
console.log(`  bytes downloaded: ${result.bytesDownloaded.toLocaleString()}`);
console.log(`  on-disk size:     ${stat.size.toLocaleString()}`);
console.log(`  elapsed:          ${elapsedSec.toFixed(2)}s`);

if (stat.size !== fileSize) {
    throw new Error(
        `size mismatch: expected ${fileSize}, got ${stat.size} on disk`,
    );
}

console.log("\nOK — file downloaded and size matches.");
