This update makes download streams return each decompressed chunk as soon as it arrives, instead of after the whole xorb block (one HTTP range, often a ~64 MB xorb) has downloaded. Clients now write a block's decompressed chunks into a shared buffer that a term's readers consume while the download is still running.

What changed
- New public type `xet_client::cas_client::XorbBlockData`: one block's decompressed data in a single fixed-size allocation. One writer appends chunks in order, moving a "ready" mark forward; readers take zero-copy `Bytes` of any range below the mark. Also new: `BlockByteRange` (a byte range anchored at a chunk index) and `ReadAmount` (`Available` / `All`).
  - Constructors: `XorbBlockData::with_exact_size(size)` (checked by `finish`) and `XorbBlockData::with_size_bound(max_size)`.
  - Writer side: `append_chunks(data, chunk_offsets)`, `append_compressed_chunk(header, compressed)`, `append_serialized_chunks(serialized, &mut skip_chunks)`, `finish()`, `fail(Arc<ClientError>)`.
  - Reader side: `try_read`, `read` (async), `read_blocking` (not on wasm), `contents()` once complete, plus `num_chunks`, `ready_bytes`, `capacity`.
- `Client::get_file_term_data` signature changed:
  - Before: `get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known: Option<usize>) -> Result<(Bytes, Vec<u32>)>`
  - After: `get_file_term_data(url_info, download_permit, progress_callback, output: Arc<XorbBlockData>) -> Result<()>`
  - Implementations append each chunk to `output` as it is decoded and do not call `finish`/`fail`; the caller does. A retried request must skip the chunks already appended (`output.num_chunks()` minus the count at the start of the call): signed URLs only allow the exact signed `Range`, so the retry downloads the whole range again.
  - The size check moved out of the client: `XorbBlockData::finish` fails if a block created `with_exact_size` has a different size. A mismatch is no longer retried.
- `RemoteClient` decodes non-multipart bodies chunk by chunk into `output`; a retry after a partial body skips the chunks already appended. Multipart bodies are still read in full, then decoded part by part.
- `xet_core_structures::xorb_object`: new `decompress_chunk_into(header, compressed, out)`; `deserialize_async::try_read_chunk_header_async` is now public.
- `xet_data::file_reconstruction`:
  - `XorbBlockData` is no longer defined or exported here (it moved to `xet_client::cas_client`). `XorbBlock::data` is now `Arc<XorbBlockData>` (was `OnceCell<Arc<XorbBlockData>>`), built by the new `XorbBlock::new(xorb_hash, chunk_ranges, xorb_block_index, references) -> Result<Self>`. New `XorbBlock::determine_size_bound` sizes a block whose terms overlap; a block that no set of terms covers is rejected as `CorruptedReconstruction`.
  - `DataFuture` is removed. `DataWriter::set_next_term_data_source` takes `reader: TermReader` (new public type, exported from `xet_data::file_reconstruction`) instead of `data_future: DataFuture`. Writers check the reader's length against the byte range when the term is queued.
  - `FileTerm::get_data_task` and `FileTerm::extract_bytes` are removed; terms are read through `TermReader`.
  - `DownloadStream::next` / `blocking_next` and `UnorderedDownloadStream::next` / `blocking_next` may now return a term in several pieces. The unordered stream's offsets are each piece's own offset.

Why this matters
- Streams such as `huggingface_hub` Parquet / WebDataset readers waited for a whole xorb block before receiving any bytes (16 to 25 s to first byte on laptops). They now get the first chunk after about one round trip plus one ~64 KB chunk.
- Non-stream paths (`reconstruct_to_file`, `reconstruct_to_writer`) still write each term once it is fully ready, as one zero-copy `Bytes`. Chunks are now decompressed straight into the block buffer instead of into a per-chunk `Vec` that was then copied.

Migration
- Custom `Client` implementations: change `get_file_term_data` to the new signature. Decode each requested range and call `output.append_serialized_chunks(&serialized_range, &mut 0)?` (or `append_compressed_chunk` per chunk for streamed bodies), and return `Ok(())`. Wrappers forward `output` unchanged.
- Callers of `get_file_term_data`: create `Arc::new(XorbBlockData::with_exact_size(n))` (or `with_size_bound`), pass a clone, then call `output.finish()?` and read with `output.contents()` or the read methods.
- Code that implemented or called `DataWriter`: pass a `TermReader` (from `FileTerm::start_read`, crate-internal) instead of a `DataFuture`.
- Stream consumers must not assume one returned `Bytes` per term.
