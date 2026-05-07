// Minimal smoke driver for the napi binding.
//
// Run after `npm run build` (or `npm run build:debug`).
// Loads the .node addon emitted by napi-rs and exercises the two functions
// exported from src/lib.rs.

import { createRequire } from "node:module";
const require = createRequire(import.meta.url);

// napi-rs writes a CommonJS shim (`index.js`) plus a per-platform `.node` file.
const addon = require("./index.js");

console.log("loaded addon, exports:", Object.keys(addon));

addon.initLogging("xet_pkg_napi/0.0.1 smoke");
const status = addon.smokeTest();
console.log("smokeTest:", status);
