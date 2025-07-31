# Hashing

The xet protocol utilizes a few different hashing types.

All hashes referenced are 32 bytes (256 bits) long.

## Chunk hashes

After cutting a chunk of data, the chunk hash is computed via a blake3 keyed hash with the following key (DATA_KEY):

### DATA_KEY

```json
[
  102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28, 157, 228, 33, 16, 155, 235, 43, 88, 180, 208, 176, 75, 147, 173, 242, 41
]
```

## Xorb Hashes

Xorbs are composed of a series of chunks; given the series of chunks that make up a xorb, to compute the hash or xorb hash we will compute a MerkleHash using a [Merkle Tree](https://en.wikipedia.org/wiki/Merkle_tree) data structure with custom hashing functions. **The xorb hash will be the root node hash of the MerkleTree.**

The leaf node hashes are the chunk hashes as described in the previous section.

The hash function used to compute internal node hashes is as follows:

- concatenate the hashes together such that for each chunk there is a line in order formatted like `{chunk_hash:x} : {size}\n`
  - the hash first in lowercase hex format (64 hex characters e.g. a3f91d6e8b47c20ff9d84a1c77dcb8e5a91e6fbf2b2d483af6d3c1e90ac57843)
  - a space, a colon, a space (` : `)
  - the chunk length number e.g. 64000
  - finally a newline `\n` character
- Then take the bytes from this string and compute a blake3 keyed hash with the following key (INTERNAL_NODE_KEY)

### INTERNAL_NODE_KEY

```json
[
  1, 126, 197, 199, 165, 71, 41, 150, 253, 148, 102, 102, 180, 138, 2, 230, 93, 221, 83, 111, 55, 199, 109, 210, 248, 99, 82, 230, 74, 83, 113, 63
]
```

### Example of data for internal node

Consider that a node were 4 chunks with the following pairs of hashes and lengths:

```txt
1f6a2b8e9d3c4075a2e8c5fd4f0b763e6f3c1d7a9b2e6487de3f91ab7c6d5401 length: 10000

7c94fe2a38bdcf9b4d2a6f7e1e08ac35bc24a7903d6f5a0e7d1c2b93e5f748de length: 20000

cfd18a92e0743bb09e56dbf76ea2c34d99b5a0cf271f8d429b6cd148203df061 length: 25000

e38d7c09a21b4cf8d0f92b3a85e6df19f7c20435e0b1c78a9d635f7b8c2e4da1 length: 64000
```

Then to form the buffer to compute the internal node hash we will create this string (note the `\n` newline at the end):

```txt
"1f6a2b8e9d3c4075a2e8c5fd4f0b763e6f3c1d7a9b2e6487de3f91ab7c6d5401 : 10000
7c94fe2a38bdcf9b4d2a6f7e1e08ac35bc24a7903d6f5a0e7d1c2b93e5f748de : 20000
cfd18a92e0743bb09e56dbf76ea2c34d99b5a0cf271f8d429b6cd148203df061 : 25000
e38d7c09a21b4cf8d0f92b3a85e6df19f7c20435e0b1c78a9d635f7b8c2e4da1 : 64000
"
```

Then compute the blake3 keyed hash with INTERNAL_NODE_KEY to get the final hash.

### Example Python code for the internal hash function

```python
from blake3 import blake3

def internal_hash_function(node):
  buffer = ""
  for chunk in node:
    size = len(chunk)
    chunk_hash = compute_chunk_hash(chunk)
    buffer += f"{chunk_hash:x} : {size}\n"

  blake3(bytes(buffer), key=INTERNAL_NODE_KEY)
```

## File Hashes

After chunking a whole file, to compute the file hash, follow the same procedure used to compute the xorb hash and then take that final hash as data to compute a blake3 keyed hash with a key that is all 0's.

This means create a MerkleTree using the same hashing functions described in the previous section. Then take the root node's hash and compute a blake3 keyed hash with the key being 32 0-value bytes.

## Term Verification Hashes

When uploading a shard, each term in each file info in the shard must have a matching FileVerificationEntry section that contains a hash.

To generate this hash, take the chunk hashes for the specific range of chunks that make up the term and:

1. **Concatenate the raw hash bytes**: Take all the chunk hashes in the range (from `chunk_index_start` to `chunk_index_end` in the xorb specified in the term) and concatenate their raw 32-byte representations together in order.

2. **Apply keyed hash**: Compute a blake3 keyed hash of the concatenated bytes using the following verification key (VERIFICATION_KEY):

### VERIFICATION_KEY

```json
[
  127, 24, 87, 214, 206, 86, 237, 102, 18, 127, 249, 19, 231, 165, 195, 243, 164, 205, 38, 213, 181, 219, 73, 230, 65, 36, 152, 127, 40, 251, 148, 195
]
```

The result of the blake3 keyed hash is the verification hash that should be used in the FileVerificationEntry for the term.

### Example Python code for the verification hash

```python
def verification_hash_function(term):
    buffer = bytes()
    # note chunk ranges are end exclusive
    for chunk_hash in term.xorb.chunk_hashes[term.chunk_index_start : term.chunk_index_end]:
        buffer.extend(bytes(chunk_hash))
    return blake3(buffer, key=VERIFICATION_KEY)
```
