This update adds conditional delete/tag-list APIs for simulation deletion controls and tightens simulation integrity checks for global dedup references.

What changed
- `DeletionControlableClient` now owns xorb deletion APIs and adds:
  - `list_xorbs_and_tags() -> Result<Vec<(MerkleHash, ObjectTag)>>`
  - `delete_xorb_if_tag_matches(hash, tag) -> Result<bool>`
  - `list_shards_with_tags() -> Result<Vec<(MerkleHash, ObjectTag)>>`
  - `delete_shard_if_tag_matches(hash, tag) -> Result<bool>`
- New shared tag type:
  - `ObjectTag = [u8; 32]`
  - Disk-backed implementation derives it from a hash of file metadata fields (including high-resolution timestamps and size) to improve entropy.
- `DirectAccessClient` no longer exposes `delete_xorb`.
- Simulation HTTP control surface adds:
  - `GET /simulation/xorbs_with_tags`
  - `POST /simulation/xorbs/{hash}/tag_delete` with `{ "tag": [u8;32] }`
  - `GET /simulation/shards_with_tags`
  - `POST /simulation/shards/{hash}/tag_delete` with `{ "tag": [u8;32] }`
  - Response for tag delete endpoints: `{ "deleted": bool }`
- `verify_integrity()` in local simulation now validates that every shard referenced by `GLOBAL_DEDUP_TABLE` exists on disk.

Why this matters
- GC and deletion workflows can do compare-and-delete style operations for xorbs/shards.
- Integrity checks now catch stale dedup-table references to missing shard files, reducing silent corruption risk in simulation flows.

Migration notes
- Any code calling `delete_xorb` through `DirectAccessClient` must switch to `DeletionControlableClient`.
- For server-backed simulation control clients, use the new tag list and conditional delete endpoints/methods.
