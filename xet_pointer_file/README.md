# xet_pointer_file

Parsing and canonical serialization of the Xet pointer file (`https://hf.co/spec/xet/v1`): the
small text file that stands in for content stored in Xet, naming its file hash and logical size.

```text
version https://hf.co/spec/xet/v1
oid xet:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
size 12345
```

The hash is a `HexHash`: exactly 64 lowercase hexadecimal digits, the form `MerkleHash::hex()`
writes, so every `HexHash` parses with `MerkleHash::from_hex`.

Pointers are UTF-8 with LF line endings and a final newline, and at most `MAX_POINTER_SIZE`
(200) bytes. Store them without end-of-line conversion (for Git, `-text`).
