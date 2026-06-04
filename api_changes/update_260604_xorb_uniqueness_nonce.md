This update adds APIs for stamping a per-upload uniqueness nonce into a xorb's serialized footer, so that two otherwise byte-identical serializations of the same xorb can be made to differ in their bytes.

What changed
- New constant `XORB_OBJECT_FORMAT_FOOTER_BUFFER_LEN` (= 16) names the footer's `_buffer` length; used wherever that length is referenced.
- `XorbObjectInfoV1` (`xet_core_structures/src/xorb_object/xorb_object_format.rs`) gains nonce accessors over its existing `_buffer` extensibility field:
  - `set_uniqueness_nonce(&mut self, nonce: [u8; XORB_OBJECT_FORMAT_FOOTER_BUFFER_LEN])`
  - `uniqueness_nonce(&self) -> [u8; XORB_OBJECT_FORMAT_FOOTER_BUFFER_LEN]`
- `XorbObject` gains an associated function to rewrite the nonce in already-serialized bytes without re-serializing:
  - `XorbObject::overwrite_uniqueness_nonce(serialized: &mut [u8], nonce: [u8; XORB_OBJECT_FORMAT_FOOTER_BUFFER_LEN]) -> Result<(), CoreError>`
  - Writes `nonce` into the `_buffer` slot, which is the `XORB_OBJECT_FORMAT_FOOTER_BUFFER_LEN` bytes immediately before the trailing 4-byte `info_length`. Works for both V0 and V1 serialized objects, since both footers end with `_buffer || info_length(u32)`. Validates the trailing `info_length` first and returns `Err(CoreError::MalformedData)` for too-short or implausible inputs.
- The `_buffer` field doc on `XorbObjectInfoV1` is updated to describe the nonce usage.

Why this matters
- The nonce is excluded from the xorb hash (the hash is a merkle tree over chunk contents only), so it does not change the xorb's content hash — only the serialized bytes.
- This lets a caller give every (re-)upload of byte-identical content a distinct serialized byte stream, so two serializations of the same xorb can be told apart.

Downstream usage
- On first xorb upload, call `set_uniqueness_nonce(rand::random())` on the metadata before `XorbObject::serialize_given_info`.
- To refresh an already-serialized object in place, read its bytes and call `XorbObject::overwrite_uniqueness_nonce`, which rewrites only the nonce and preserves the existing chunk layout.

Migration notes
- Purely additive; no changes to serialization layout, `serialized_length`, the wire format, or existing call sites. Objects written before this change have an all-zero `_buffer` and remain valid.
