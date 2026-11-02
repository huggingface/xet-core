This update adds network (transfer) bytes to the per-item progress report, alongside the decompressed bytes it already had.

What changed
- `ItemProgressReport` has two new public fields, matching `GroupProgressReport`'s totals:
  - `transfer_bytes: u64`: the network bytes the item needs, as far as known. On a download it grows as the reconstruction discovers its terms.
  - `transfer_bytes_completed: u64`: the network bytes transferred so far, including chunk cache hits on downloads.
- `ItemProgress::report()`, and every API returning an `ItemProgressReport` (`FileDownloadSession::item_report`, `item_reports`, the `xet_session` handles' `progress()`), fill them in from the item's existing counters.
- In `hf_xet`, the fields are readable on `ItemProgressReport` from Python.

Why this matters
- A streaming download's `bytes_completed` advances when the consumer reads from the stream, in file order. While a block ahead of the read position is still downloading, it stands still, then jumps when that block lands. Network bytes move as data arrives, so per-file progress bars can follow the download itself.

Migration
- Code that builds an `ItemProgressReport` with a struct literal adds the two fields, for example `transfer_bytes: 0, transfer_bytes_completed: 0`. Code that reads reports needs no change.
