// Deterministic seed payload for the global-dedup scenario. Kept free of
// wasm imports so it also runs under plain Node for manual seeding:
//
//   node -e "import('./seed.mjs').then(async (s) => {
//     const { writeFile } = await import('node:fs/promises');
//     await writeFile(s.SEED_PATH, s.seedPayload());
//   })"
//   hf upload xet-team/xet-wasm-test global-dedup-seed-v1.bin --repo-type dataset
//
// Bump the version suffix in SEED_PATH (and re-pin SEED_SHA256) whenever
// SEED_SIZE or the PRNG changes — the on-hub file and the generator must
// stay byte-identical.

export const SEED_PATH = 'global-dedup-seed-v1.bin';
export const SEED_SIZE = 16 * 1024 * 1024;
export const SEED_PRNG_INIT = 0x9e3779b9;

// SHA-256 of seedPayload(); pinned so PRNG drift fails loudly instead of
// producing a seed mismatch against the on-hub file.
export const SEED_SHA256 = 'b71f784796bb2712ce75bfb94889b72931a0858ed7235543b7fcd67f253436b4';

// xorshift32, written little-endian so the payload is identical on every
// platform.
export function seedPayload(size = SEED_SIZE, state = SEED_PRNG_INIT) {
  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let s = state >>> 0;
  const whole = size - (size % 4);
  for (let off = 0; off < whole; off += 4) {
    s ^= s << 13;
    s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5;
    s >>>= 0;
    view.setUint32(off, s, true);
  }
  for (let off = whole; off < size; off++) {
    s ^= s << 13;
    s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5;
    s >>>= 0;
    buf[off] = s & 0xff;
  }
  return buf;
}
