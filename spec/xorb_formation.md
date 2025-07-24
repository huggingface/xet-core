# Xorb Formation

## Collecting Chunks

Using the chunking algorithm a file is mapped to a series of chunks, once those chunks are found, they need to be collected into collections of chunks called each called a "Xorb" (Xet Orb, pronounced like "zorb").

It is advantageous to collect series of chunks in xorbs such that they can be referred to as a whole range of chunks.

Suppose a file is chunked into chunks A, B, C, D in the order ABCD. Then create a xorb X1 with chunks A, B, C, D in this order (starting at chunk index 0), let's say this xorb's hash is X1. Then to reconstruct the file we ask for xorb X1 chunk range `[0, 4)`.
