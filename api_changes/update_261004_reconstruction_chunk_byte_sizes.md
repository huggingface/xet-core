# Chunk sizes in reconstruction terms

**Date**: 2026-10-04
**Crates**: `xet-client` (`cas_types::XorbReconstructionTerm`, `RemoteClient`), `xet-runtime` (`client` config group)

## What changed

`XorbReconstructionTerm` has a new public field, `chunk_byte_sizes: Vec<u32>`: the uncompressed size
of each chunk of the term's `range`, in chunk order. It is empty unless the client asked for it and
the server provides it. It is skipped when empty and defaults to empty, so the JSON wire format
does not change for clients that do not ask. Code that builds the struct with a literal must set
the field (`chunk_byte_sizes: Vec::new()`).

New config value `client.reconstruction_chunk_byte_sizes` (`HF_XET_CLIENT_RECONSTRUCTION_CHUNK_BYTE_SIZES`,
default `false`). When true, `RemoteClient` adds `?chunk_byte_sizes=true` to its V1 and V2
reconstruction requests. The CAS server lists the sizes only for ranges up to 1 GiB, and leaves the
field empty for longer ranges and for whole files larger than that.

## Why this matters

With the sizes, a client that caches a reconstruction plan can derive the plan of a sub-range
locally and trim its first and last terms at chunk boundaries, as the server does for a range
query. Without them, a derived plan keeps whole terms, which can mean fetching tens of MiB for a
small read.
