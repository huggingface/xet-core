# File Reconstruction

After a file is mapped into a series of chunks, and those chunks are tracked by xorbs, we can define the file reconstruction or the recipe to re-materialize the file.

For every chunk of the file we need to know which xorb this chunk is in and its index (starting at 0) in that xorb and list out all the chunks in this way to re-create the file.

Now since xorbs contain multiple chunks, we can condense the listing of each range of chunks that are contiguous in the same xorb together and this map the listing of chunks that form the file to a series of sub-ranges of chunks within xorbs.

## Example 1

Suppose that a file has 26 chunks, A-Z
Chunks A-G are in order in xorb X1 starting at index 0.
Chunks H-K are in order in xorb X2 starting at index 0.
Chunks K-Z are in order in xorb X3 starting at index 645.

Then to reconstruct the file the following chunk ranges are needed:

Xorb X1 chunks `[0, 8)`. (need chunk A at chunk index 0, through chunk G at chunk index 7)
Xorb X2 chunks `[0, 4)`. (need chunk H at chunk index 0, through chunk J at chunk index 3)
Xorb X3 chunks `[645, 662)`. (need chunk K at chunk index 645, through chunk Z at chunk index 661)
