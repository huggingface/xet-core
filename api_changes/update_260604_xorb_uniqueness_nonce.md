This update adds APIs for stamping a per-upload uniqueness nonce into a xorb's serialized footer, so that two otherwise byte-identical serializations of the same xorb can be made to differ in their bytes.

What changed
- New constants: `XORB_OBJECT_FORMAT_FOOTER_BUFFER_LEN` (= 16, the footer buffer slot) and `XORB_OBJECT_FORMAT_NONCE_LEN` (= 4, the bytes of that slot used for the nonce). The remaining 12 bytes stay zero, reserved for future use.
- `XorbObjectInfoV1` (`xet_core_structures/src/xorb_object/xorb_object_format.rs`) gains nonce accessors over its renamed `_nonce_buffer` extensibility field (was `_buffer`; the legacy `XorbObjectInfoV0` keeps `_buffer` unchanged):
  - `set_uniqueness_nonce(&mut self, nonce: [u8; XORB_OBJECT_FORMAT_NONCE_LEN])` (writes the leading `NONCE_LEN` bytes, leaving the reserved bytes unchanged)
  - `uniqueness_nonce(&self) -> [u8; XORB_OBJECT_FORMAT_NONCE_LEN]`
- `XorbObject` gains an associated function to rewrite the nonce in already-serialized bytes without re-serializing:
  - `XorbObject::overwrite_uniqueness_nonce(serialized: &mut [u8], nonce: [u8; XORB_OBJECT_FORMAT_NONCE_LEN]) -> Result<(), CoreError>`
  - Writes `nonce` into the leading `NONCE_LEN` bytes of the footer buffer (which sits immediately before the trailing 4-byte `info_length`), leaving the reserved bytes and `info_length` untouched. Works for both V0 and V1 serialized objects. Validates the trailing `info_length` first and returns `Err(CoreError::MalformedData)` for too-short or implausible inputs.
- The `_nonce_buffer` field doc on `XorbObjectInfoV1` describes the nonce usage.

Why this matters
- The nonce is excluded from the xorb hash (the hash is a merkle tree over chunk contents only), so it does not change the xorb's content hash — only the serialized bytes.
- This lets a caller give every (re-)upload of byte-identical content a distinct serialized byte stream, so two serializations of the same xorb can be told apart.

Downstream usage
- On first xorb upload, call `set_uniqueness_nonce(rand::random())` on the metadata before `XorbObject::serialize_given_info`.
- To refresh an already-serialized object in place, read its bytes and call `XorbObject::overwrite_uniqueness_nonce`, which rewrites only the nonce and preserves the existing chunk layout.

Migration notes
- Purely additive; no changes to serialization layout, `serialized_length`, the wire format, or existing call sites. The V1 field rename `_buffer` → `_nonce_buffer` is source-only (the field is private and `#[serde(skip)]`). Objects written before this change have an all-zero nonce buffer and remain valid.
