# Hashing

- [Chunk hashes](#chunk-hashes) - compute for each chunk from chunk data.
- [Xorb Hashes](#xorb-hashes) - compute for each xorb from its chunk hashes.
- [File Hashes](#file-hashes) - compute for each file from its chunk hashes.
- [Term Verification Hashes](#term-verification-hashes) - compute for each term in a reconstruction when serializing a shard from the chunk hashes in the xorb that is used in that term.

The Xet protocol utilizes a few different hashing types.

All hashes referenced are 32 bytes (256 bits) long.

## Chunk Hashes

After cutting a chunk of data, the chunk hash is computed via a blake3 keyed hash with the following key (DATA_KEY):

### DATA_KEY

```json
[
  102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28, 157, 228, 33, 16, 155, 235, 43, 88, 180, 208, 176, 75, 147, 173, 242, 41
]
```

[reference implementation](https://github.com/huggingface/xet-core/blob/main/merklehash/src/data_hash.rs#L308-L311)

## Xorb Hashes

Xorbs are composed of a series of chunks; given the series of chunks that make up a xorb, to compute the hash or xorb hash we will compute a MerkleHash using a [Merkle Tree](https://en.wikipedia.org/wiki/Merkle_tree) data structure with custom hashing functions.
**The xorb hash will be the root node hash of the MerkleTree.**

The leaf node hashes are the chunk hashes as described in the previous section.

The hash function used to compute internal node hashes is as follows:

- concatenate the hashes together such that for each chunk there is a line in order formatted like `{chunk_hash:x} : {size}\n`
  - the hash first in lowercase hex format (64 hex characters e.g. `a3f91d6e8b47c20ff9d84a1c77dcb8e5a91e6fbf2b2d483af6d3c1e90ac57843`)
  - a space, a colon, a space (` : `)
  - the chunk length number e.g. 64000
  - finally a newline `\n` character
- Then take the bytes from this string and compute a blake3 keyed hash with the following key (INTERNAL_NODE_KEY)

[reference implementation](https://github.com/huggingface/xet-core/blob/main/merklehash/src/aggregated_hashes.rs#L103-L109)

### INTERNAL_NODE_KEY

```json
[
  1, 126, 197, 199, 165, 71, 41, 150, 253, 148, 102, 102, 180, 138, 2, 230, 93, 221, 83, 111, 55, 199, 109, 210, 248, 99, 82, 230, 74, 83, 113, 63
]
```

### Example of data for internal node

Consider that a node were 4 chunks with the following pairs of hashes and lengths:

```txt
hash,length (bytes)
1f6a2b8e9d3c4075a2e8c5fd4f0b763e6f3c1d7a9b2e6487de3f91ab7c6d5401,10000
7c94fe2a38bdcf9b4d2a6f7e1e08ac35bc24a7903d6f5a0e7d1c2b93e5f748de,20000
cfd18a92e0743bb09e56dbf76ea2c34d99b5a0cf271f8d429b6cd148203df061,25000
e38d7c09a21b4cf8d0f92b3a85e6df19f7c20435e0b1c78a9d635f7b8c2e4da1,64000
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

This means create a MerkleTree using the same hashing functions described in the previous section.
Then take the root node's hash and compute a blake3 keyed hash with the key being 32 0-value bytes.

[reference implementation](https://github.com/huggingface/xet-core/blob/main/merklehash/src/aggregated_hashes.rs#L123-L125)

## Term Verification Hashes

When uploading a shard, each term in each file info in the shard MUST have a matching FileVerificationEntry section that contains a hash.

To generate this hash, take the chunk hashes for the specific range of chunks that make up the term and:

1. **Concatenate the raw hash bytes**: Take all the chunk hashes in the range (from `chunk_index_start` to `chunk_index_end` in the xorb specified in the term) and concatenate their raw 32-byte representations together in order.

2. **Apply keyed hash**: Compute a blake3 keyed hash of the concatenated bytes using the following verification key (VERIFICATION_KEY):

### VERIFICATION_KEY

```json
[
  127, 24, 87, 214, 206, 86, 237, 102, 18, 127, 249, 19, 231, 165, 195, 243, 164, 205, 38, 213, 181, 219, 73, 230, 65, 36, 152, 127, 40, 251, 148, 195
]
```

The result of the blake3 keyed hash is the verification hash that MUST be used in the FileVerificationEntry for the term.

[reference implementation](https://github.com/huggingface/xet-core/blob/main/mdb_shard/src/chunk_verification.rs#L4-L16)

### Example Python code for the verification hash

```python
def verification_hash_function(term):
    buffer = bytes()
    # note chunk ranges are end exclusive
    for chunk_hash in term.xorb.chunk_hashes[term.chunk_index_start : term.chunk_index_end]:
        buffer.extend(bytes(chunk_hash))
    return blake3(buffer, key=VERIFICATION_KEY)
```

## Reference Files

Reference files are provided in Hugging Face Dataset repository [xet-team/xet-spec-reference-files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files).

In this repository there are a number of different samples implementors can use to verify hash computations.

> Note that all hashes are represented as strings.
To get the raw value of these hashes you must invert the endianness of each byte octet in the hash string, reversing the procedure described in [api.md](../spec/api.md#converting-hashes-to-strings).

### Chunk Hashes Sample

There are 3 chunks files, for each file name, the first 64 characters are the string format of the chunk hash of the data in the file:

- [b10aa1dc71c61661de92280c41a188aabc47981739b785724a099945d8dc5ce4.chunk](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/b10aa1dc71c61661de92280c41a188aabc47981739b785724a099945d8dc5ce4.chunk)
- [26255591fa803b6baf25d88c315b8a6f5153d5bcfdf18ec5ef526264e0ccc907.chunk](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/26255591fa803b6baf25d88c315b8a6f5153d5bcfdf18ec5ef526264e0ccc907.chunk)
- [099cb228194fe640e36a6c7d274ee5ed3a714ccd557a0951d9b6b43a7292b5d1.chunk](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/099cb228194fe640e36a6c7d274ee5ed3a714ccd557a0951d9b6b43a7292b5d1.chunk)

### File Hash Sample

The [xet-team/xet-spec-reference-files](https://huggingface.co/datasets/xet-team/xet-spec-reference-files) repository contains the original file [Electric_Vehicle_Population_Data_20250917.csv](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv).

When processed through the Xet upload protocol the chunks that are produced for this file are listed (formatted `<hash> <length>`) in the file [Electric_Vehicle_Population_Data_20250917.csv.chunks](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.chunks).

Using these chunks to compute a file hash of the entire file the result is the hash stored in the file [Electric_Vehicle_Population_Data_20250917.csv.xet-file-hash](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.xet-file-hash) or the raw value `118a53328412787fee04011dcf82fdc4acf3a4a1eddec341c910d30a306aaf97`.

### Xorb Hash Sample

All of the chunks of [Electric_Vehicle_Population_Data_20250917.csv](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv) can fit into 1 single xorb.

The xorb produced with all of the chunks in order for this file can be found serialized in file [eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb).

The hash of this xorb is `eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632`, the value in [Electric_Vehicle_Population_Data_20250917.csv.xet-xorb-hash](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.xet-xorb-hash).

The chunks that make up this xorb are listed in a file [eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb.chunks](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb.chunks); note this file is equivalent to [Electric_Vehicle_Population_Data_20250917.csv.chunks](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv.chunks).

### Range Hash Sample

In the reconstruction of [Electric_Vehicle_Population_Data_20250917.csv](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/Electric_Vehicle_Population_Data_20250917.csv) with xorb [eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb) there is 1 range that contains all 796 chunks.

The verification range hash for this range is the value in [eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb.range-hash](https://huggingface.co/datasets/xet-team/xet-spec-reference-files/blob/main/eea25d6ee393ccae385820daed127b96ef0ea034dfb7cf6da3a950ce334b7632.xorb.range-hash) which is `d81c11b1fc9bc2a25587108c675bbfe65ca2e5d350b0cd92c58329fcc8444178`.
