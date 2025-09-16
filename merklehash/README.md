# merklehash

The `merklehash` crate exports a `Merklehash` type that represents a 32 byte hash throughout all of the xet-core
components.

`merklehash` also exports some hashing functions e.g. `file_hash` and `xorb_hash` to compute `MerkleHash`es.

The `MerkleHash` is internally represented as 4 `u64` (`[u64; 4]`).

The `HexMerkleHash` is also exported and is intended to be used to provide a `serde::Serialize` implementation for a
`MerkleHash` using the string hexadecimal representation of the hash.
