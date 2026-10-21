This update makes `ShardFileManager` evict the oldest shards from its in-memory chunk index once the index is full, instead of silently skipping every shard registered after that point. Callers that register shards now choose what happens at the cap.

What changed
- New public enum `ChunkIndexSizeEnforcement`, defined in `xet_core_structures::metadata_shard::shard_file_manager` and re-exported from `xet_core_structures::metadata_shard` next to `ShardFileManager`:
  - `StopAtCap`: a shard that doesn't fit is registered, but its chunks aren't indexed for dedup.
  - `Eviction`: the oldest indexed shards, ordered by the footer's `shard_creation_timestamp`, are evicted from the chunk index to make room.
- These `ShardFileManager` methods take a new last argument, `index_size_enforcement: ChunkIndexSizeEnforcement`:
  - `register_shards(&self, new_shards, index_size_enforcement)`
  - `register_shards_by_path(&self, new_shards, index_size_enforcement)`
  - `import_shard_from_bytes(&self, shard, index_size_enforcement)`
  - `refresh_shard_dir(&self, prune_expired, prune_cache_to_size, index_size_enforcement)`
- Shards the manager cuts itself (`add_xorb_block`, `add_file_reconstruction_info`, `flush`) are registered with `Eviction`.
- `new_in_session_directory` and `new_in_cache_directory` scan a newly created manager's directory with `StopAtCap`. Reusing a cached manager rescans its directory once with `Eviction`; previously it scanned twice.
- Evicted shards stay registered, so a rescan doesn't register or index them again.
- Each collection's `shard_list` still only grows, but its first 65,536 slots now form a ring: once a single HMAC key has more shards than that, a new shard takes the slot of the oldest one, which is evicted from the chunk index and moved to the end of the list.

Why this matters
- Once the index filled, global dedup kept downloading shards it couldn't use and re-uploaded data the server already had. A 1,000-file `hf buckets sync` batch stopped deduplicating after its first 300 to 400 files.

Migration
- Add the new argument to every call of the four methods above. Choose by where the shards come from:
  - Shards loaded in bulk, such as a directory scan or a resumed session's merged shards: `StopAtCap`. Shards are registered newest first, so stopping keeps the newest instead of evicting what was just indexed.
  - Shards created or downloaded during the session, such as global dedup responses or shards just uploaded and copied into the cache: `Eviction`.
  - Code that only needs the previous behavior, such as simulations or tools: `StopAtCap`.
- Example: `manager.register_shards(&[shard]).await?` becomes `manager.register_shards(&[shard], ChunkIndexSizeEnforcement::StopAtCap).await?`.
