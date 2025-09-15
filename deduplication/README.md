# Deduplication crate

This package contains components and functionality to create chunks from raw data and attempt to deduplicate chunks locally and globally.

## Notable exports

- `Chunker`: Stateful chunk boundary detector and chunk producer.
- `DataAggregator`: Builder that accumulates chunks and file-info into uploadable units (xorbs and shards).
- `FileDeduper<DataInterfaceType>`: Orchestrator for per-file deduplication over a provided data interface.
- `DeduplicationDataInterface`: Trait defining the data-access/upload interface required by the deduper.
- `RawXorbData`: Container for a single upload unit (xorb) with its metadata.
- `constants`: Public constants used to configure chunking and xorb limits.
