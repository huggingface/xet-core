#[cfg(test)]
mod component_tests {
    use std::collections::HashSet;
    use std::io;
    use std::io::Cursor;

    use merklehash::{compute_data_hash, MerkleHash};
    use utils::async_iterator::AsyncIterator;

    use crate::async_chunk_iterator::async_low_variance_chunk_target;
    use crate::chunk_iterator::*;
    use crate::constants::*;
    use crate::prelude::*;
    use crate::prelude_v2::*;
    use crate::MerkleMemDB;

    #[test]
    fn simple_test() {
        let mut mdb = MerkleMemDB::default();
        let h1 = compute_data_hash("hello world".as_bytes());
        let h2 = compute_data_hash("pikachu".as_bytes());
        let chunks = vec![ChunkInfo { hash: h1, length: 11 }, ChunkInfo { hash: h2, length: 7 }];

        let mut staging = mdb.start_insertion_staging();
        mdb.add_file(&mut staging, &chunks);
        let res = mdb.finalize(staging);
        let nodes = mdb.find_all_leaves(&res).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(*nodes[0].hash(), chunks[0].hash);
        assert_eq!(*nodes[1].hash(), chunks[1].hash);
        assert_eq!(nodes[0].len(), chunks[0].length);
        assert_eq!(nodes[1].len(), chunks[1].length);
        assert_ne!(nodes[0].id(), nodes[1].id());

        // we should get back original ids if we insert again
        let mut staging = mdb.start_insertion_staging();
        mdb.add_file(&mut staging, &chunks);
        let res = mdb.finalize(staging);
        let nodes2 = mdb.find_all_leaves(&res).unwrap();
        assert_eq!(nodes2[0].id(), nodes[0].id());
        assert_eq!(nodes2[1].id(), nodes[1].id());

        // search by hash()
        let nodes3 = [mdb.find_node(&h1).unwrap(), mdb.find_node(&h2).unwrap()];
        assert_eq!(nodes3[0].id(), nodes[0].id());
        assert_eq!(nodes3[1].id(), nodes[1].id());

        // search by id
        let nodes4 = [
            mdb.find_node_by_id(nodes[0].id()).unwrap(),
            mdb.find_node_by_id(nodes[1].id()).unwrap(),
        ];
        assert_eq!(nodes4[0].id(), nodes[0].id());
        assert_eq!(nodes4[1].id(), nodes[1].id());
        assert!(mdb.all_invariant_checks());
    }

    #[test]
    fn duplicate_file() {
        let mut mdb = MerkleMemDB::default();
        let h1 = compute_data_hash("hello world".as_bytes());
        let h2 = compute_data_hash("pikachu".as_bytes());
        let chunks = vec![ChunkInfo { hash: h1, length: 11 }, ChunkInfo { hash: h2, length: 7 }];

        let mut staging = mdb.start_insertion_staging();
        mdb.add_file(&mut staging, &chunks);
        // add the same file twice
        mdb.add_file(&mut staging, &chunks);
        let res = mdb.finalize(staging);
        // there should still be only 2 nodes
        let nodes = mdb.find_all_leaves(&res).unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(mdb.all_invariant_checks());
    }
    fn generate_random_string(seed: u64, len: usize) -> Vec<u8> {
        use rand::{RngCore, SeedableRng};
        let mut bytes: Vec<u8> = vec![0; len];
        bytes.resize(len, 0_u8);
        rand::rngs::StdRng::seed_from_u64(seed).fill_bytes(&mut bytes);
        bytes
    }

    fn generate_uniform_string(len: usize) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![0; len];
        bytes.resize(len, 0_u8);
        bytes
    }

    const PER_SEED_INPUT_SIZE: usize = 128 * 1024;

    fn generate_random_chunks(seed: u64) -> Vec<ChunkInfo> {
        let input = generate_random_string(seed, PER_SEED_INPUT_SIZE);
        let mut reader = Cursor::new(&input[..]);
        low_variance_chunk_target(&mut reader, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS)
    }

    #[test]
    fn cas_consistency_basic() {
        // make a bunch of random chunks
        // 128k of chunks with seed=0
        // 128k of chunks with seed=1
        // 128k of chunks with seed=2
        // 128k of chunks with seed=3
        // and repeat a bunch of times
        let mut chunks1234: Vec<ChunkInfo> = Vec::new();

        for seed in 0..16_u64 {
            let mut ch = generate_random_chunks(seed % 4);
            chunks1234.append(&mut ch);
        }

        // insert this into the MerkleDB

        let mut mdb = MerkleMemDB::default();
        let mut staging = mdb.start_insertion_staging();
        mdb.add_file(&mut staging, &chunks1234);
        let file_root = mdb.finalize(staging);
        assert!(mdb.all_invariant_checks());

        // check the CAS's produced
        // there should be 1 unique CAS node
        let file_recon = mdb.reconstruct_from_cas(&[file_root.clone()]).unwrap();
        assert_eq!(file_recon[0].0, *file_root.hash());
        let cas_ranges = file_recon[0].1.clone();
        // and the CAS should be chunks of seed 0..3 concatenated 4 times
        assert_eq!(cas_ranges.len(), 4);

        let unique_cas_hashes = cas_ranges
            .iter()
            .map(|x| x.hash) // get the hash
            .collect::<HashSet<_>>() // collect into a hashset
            .into_iter() // iterate through the hashset again consuming
            .collect::<Vec<_>>(); // collect into a vec

        assert_eq!(unique_cas_hashes.len(), 1);
        let cas_node_hash = unique_cas_hashes[0];

        // check that the cas node contains the string for seed=0,1,2,3
        let cas_node = mdb.find_node(&cas_node_hash).unwrap();
        assert_eq!(cas_node.len(), 4 * PER_SEED_INPUT_SIZE); // strings for seed=0,1,2,3

        // ok. validate we that the cas node comprises of exactly the chunks for
        // seed=0,1,2,3
        let cas_leaves = mdb.find_all_leaves(&cas_node).unwrap();
        let mut expected_leaves: Vec<ChunkInfo> = Vec::new();
        for seed in 0..4_u64 {
            expected_leaves.append(&mut generate_random_chunks(seed));
        }
        assert_eq!(expected_leaves.len(), cas_leaves.len());
        for i in 0..expected_leaves.len() {
            assert_eq!(expected_leaves[i].hash, *cas_leaves[i].hash());
        }
    }

    #[test]
    fn cas_consistency_multifile() {
        let mut mdb = MerkleMemDB::default();
        let mut staging = mdb.start_insertion_staging();
        const CAS_BLOCK_SIZE: usize = 300 * 1024;
        staging.set_target_cas_block_size(CAS_BLOCK_SIZE);
        // make 10 files
        for file in 0..10_u64 {
            // each file is 2 random sequences of 128KB worth of chunks.
            // There are 13 such unique sequences
            let mut ch = generate_random_chunks((file * 137) % 13);
            ch.append(&mut generate_random_chunks((file * 67) % 13));
            mdb.add_file(&mut staging, &ch);
        }
        // add one more unique file
        mdb.add_file(&mut staging, &generate_random_chunks(456));
        let file_root = mdb.finalize(staging);
        assert!(mdb.all_invariant_checks());

        // check the CAS's produced are not excessively large. Just
        // a bit bigger than the target CAS block size.
        const PEAK_CAS_BLOCK_SIZE: usize = CAS_BLOCK_SIZE + TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER;
        //
        // there should be 1 unique CAS node
        let file_recon = mdb.reconstruct_from_cas(&[file_root]).unwrap();
        let cas_ranges = file_recon[0].1.clone();
        let unique_cas_hashes = cas_ranges
            .iter()
            .map(|x| x.hash) // get the hash
            .collect::<HashSet<_>>() // collect into a hashset
            .into_iter() // iterate through the hashset again consuming
            .collect::<Vec<_>>(); // collect into a vec

        for hash in unique_cas_hashes {
            let node = mdb.find_node(&hash).unwrap();
            assert!(node.len() < PEAK_CAS_BLOCK_SIZE);
        }
    }

    #[test]
    fn test_db_union() {
        let mut mdb = MerkleMemDB::default();
        let f1hash: MerkleHash;
        let h1 = compute_data_hash("hello world".as_bytes());
        let h2 = compute_data_hash("pikachu".as_bytes());
        {
            let chunks = vec![ChunkInfo { hash: h1, length: 11 }, ChunkInfo { hash: h2, length: 7 }];

            let mut staging = mdb.start_insertion_staging();
            mdb.add_file(&mut staging, &chunks);
            f1hash = *mdb.finalize(staging).hash();
        }

        let h3 = compute_data_hash("poo".as_bytes());
        let mut mdb2 = MerkleMemDB::default();
        let f2hash: MerkleHash;
        {
            let chunks = vec![ChunkInfo { hash: h1, length: 11 }, ChunkInfo { hash: h3, length: 3 }];

            let mut staging = mdb2.start_insertion_staging();
            mdb2.add_file(&mut staging, &chunks);
            f2hash = *mdb2.finalize(staging).hash();
        }

        mdb.union_with(&mdb2);
        mdb.union_finalize().unwrap();
        // file 1 node
        {
            let nodes = mdb.find_all_leaves(&mdb.find_node(&f1hash).unwrap()).unwrap();
            assert_eq!(nodes.len(), 2);
            assert_eq!(*nodes[0].hash(), h1);
            assert_eq!(*nodes[1].hash(), h2);
        }
        // file 2 nodes
        {
            let nodes = mdb.find_all_leaves(&mdb.find_node(&f2hash).unwrap()).unwrap();
            assert_eq!(nodes.len(), 2);
            assert_eq!(*nodes[0].hash(), h1);
            assert_eq!(*nodes[1].hash(), h3);
        }
        assert!(mdb.all_invariant_checks());
    }

    #[test]
    fn test_db_difference() {
        // create db1 which is "hello world" "pika"
        let mut mdb = MerkleMemDB::default();
        let h1 = compute_data_hash("hello world".as_bytes());
        let h2 = compute_data_hash("pikachu".as_bytes());
        let chunks = [ChunkInfo { hash: h1, length: 11 }, ChunkInfo { hash: h2, length: 7 }];

        let nodes: Vec<_> = chunks.iter().map(|x| mdb.add_chunk(x).0).collect();
        let f1hash = *mdb.merge_to_file(&nodes).hash();

        // create db1 which is "hello world" "pika" "poo"
        // but "hello world" and "pika" are merged first
        let h3 = compute_data_hash("poo".as_bytes());
        let mut mdb2 = MerkleMemDB::default();
        let chunks = [ChunkInfo { hash: h1, length: 11 }, ChunkInfo { hash: h2, length: 7 }];

        let nodes: Vec<_> = chunks.iter().map(|x| mdb2.add_chunk(x).0).collect();
        let f1node = mdb2.merge_to_file(&nodes);
        let append_poo: Vec<_> = vec![f1node, mdb2.add_chunk(&ChunkInfo { hash: h3, length: 3 }).0];
        let f2node = mdb2.merge_to_file(&append_poo);
        let f2hash = *f2node.hash();
        mdb2.only_file_invariant_checks();

        let mut diff = MerkleMemDB::default();
        diff.difference(&mdb2, &mdb);
        eprintln!("Old: {:?}\n", mdb);
        eprintln!("New: {:?}\n", mdb2);
        eprintln!("Diff: {:?}\n", diff);

        // diff should contain h3, f2hash and f1hash
        // (seq number is 4 cos id 0 is reserved)
        assert_eq!(diff.get_sequence_number(), 4);

        // check that these two nodes are there
        diff.find_node(&h3).unwrap();
        diff.find_node(&f2hash).unwrap();
        diff.find_node(&f1hash).unwrap();

        mdb.union_with(&diff);
        mdb.union_finalize().unwrap();

        eprintln!("Union: {:?}\n", mdb);

        mdb.only_file_invariant_checks();
        assert_eq!(mdb.get_sequence_number(), mdb2.get_sequence_number());
    }

    #[test]
    fn test_randomized_union_diff() {
        // In this test, we basically make a chain of diffs
        // start from an empty MDB. add a bunch of things to it, take a diff
        // and repeat.
        //
        // We should be able to merge the diffs in an arbitrary order
        // and obtain back the same MDB
        use rand::seq::SliceRandom;
        use rand::{RngCore, SeedableRng};

        use crate::MerkleNodeId;
        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);

        let mut mdb = MerkleMemDB::default();
        // the collection of all the diffs
        let mut diffs: Vec<MerkleMemDB> = Vec::new();
        for _iters in 0..100 {
            // we keep the previous db so we can compute a idff
            let prevmdb = mdb.clone();
            // in each iteration we add a "file" and a "cas" with a different
            // set of chunks in each.
            // We limit to a set of no more than 1000 unique chunks to get
            // a decent amount of intersection in the tree structures
            let filechunks: Vec<ChunkInfo> = (0..(1 + (rng.next_u64() % 10)))
                .map(|_| {
                    let h = compute_data_hash(&generate_random_string(rng.next_u64() % 1000, 100));
                    ChunkInfo { hash: h, length: 100 }
                })
                .collect();
            let filenodes: Vec<_> = filechunks.iter().map(|x| mdb.add_chunk(x).0).collect();
            mdb.merge_to_file(&filenodes);

            let caschunks: Vec<ChunkInfo> = (0..(1 + (rng.next_u64() % 10)))
                .map(|_| {
                    let h = compute_data_hash(&generate_random_string(rng.next_u64() % 1000, 100));
                    ChunkInfo { hash: h, length: 100 }
                })
                .collect();
            let casnodes: Vec<_> = caschunks.iter().map(|x| mdb.add_chunk(x).0).collect();
            mdb.merge_to_cas(&casnodes);
            let mut diff = MerkleMemDB::default();
            diff.difference(&mdb, &prevmdb);
            diffs.push(diff);
        }

        // now we have collected a 100 diffs. we will union them arbitrarily
        diffs.shuffle(&mut rng);
        let mut unionmdb = MerkleMemDB::default();
        for d in diffs {
            unionmdb.union_with(&d);
        }
        unionmdb.union_finalize().unwrap();
        // do all checks
        mdb.only_file_invariant_checks();
        unionmdb.only_file_invariant_checks();
        // now, mdb and unionmdb should be identical.
        // apart form parent attributes which may differ.
        for i in 0..mdb.get_sequence_number() {
            let mdbnode = mdb.find_node_by_id(i as MerkleNodeId).unwrap();
            let unionnode = unionmdb.find_node(mdbnode.hash()).unwrap();
            // check that they have the same length and children count
            assert_eq!(mdbnode.len(), unionnode.len());
            assert_eq!(mdbnode.children().len(), unionnode.children().len());

            // check that is_cas and is_file attributes match
            let mdbnodeattr = mdb.node_attributes(mdbnode.id()).unwrap();
            let unionnodeattr = unionmdb.node_attributes(unionnode.id()).unwrap();
            assert_eq!(mdbnodeattr.is_cas(), unionnodeattr.is_cas());
            assert_eq!(mdbnodeattr.is_file(), unionnodeattr.is_file());
            assert_eq!(mdbnodeattr.has_cas_data(), unionnodeattr.has_cas_data());
            assert_eq!(mdbnodeattr.has_file_data(), unionnodeattr.has_file_data());

            // loop through each child validating they are the same
            for (chidx, chid) in mdbnode.children().iter().enumerate() {
                let unionchid = unionnode.children()[chidx];
                assert_eq!(chid.1, unionchid.1);
                let chnode = mdb.find_node_by_id(chid.0).unwrap();
                let unionchnode = unionmdb.find_node_by_id(unionchid.0).unwrap();
                assert_eq!(chnode.hash(), unionchnode.hash());
            }

            // we just assert that they can both reconstruct (or not)
            // The exact reconstruction is not important.
            assert_eq!(
                mdb.reconstruct_from_file(&[mdbnode.clone()]).is_ok(),
                unionmdb.reconstruct_from_file(&[unionnode.clone()]).is_ok()
            );
            assert_eq!(
                mdb.reconstruct_from_cas(&[mdbnode.clone()]).is_ok(),
                unionmdb.reconstruct_from_cas(&[unionnode.clone()]).is_ok()
            );
        }
    }

    struct AsyncVec {
        pub items: std::collections::VecDeque<Vec<u8>>,
    }
    #[async_trait::async_trait]
    impl AsyncIterator<io::Error> for AsyncVec {
        type Item = Vec<u8>;

        async fn next(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            if self.items.is_empty() {
                return Ok(None);
            }
            Ok(Some(self.items.pop_front().unwrap()))
        }
    }

    #[tokio::test]
    async fn test_async_chunker() {
        let seed = 12345;
        let input = generate_random_string(seed, PER_SEED_INPUT_SIZE);
        let mut reader = Cursor::new(&input[..]);
        let chunks = low_variance_chunk_target(&mut reader, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS);
        let mut v: std::collections::VecDeque<Vec<u8>> = std::collections::VecDeque::new();
        let mut start: usize = 0;

        use rand::{Rng, SeedableRng};
        let mut bytes: Vec<u8> = vec![0; PER_SEED_INPUT_SIZE];
        bytes.resize(PER_SEED_INPUT_SIZE, 0_u8);
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        while start < input.len() {
            let tlen: u16 = rng.random::<u16>() % 4096;
            let mut end = start + tlen as usize;
            if end > input.len() {
                end = input.len();
            }
            v.push_back(input[start..end].into());
            start = end;
        }
        let reader2 = AsyncVec { items: v };

        let mut async_chunks: Vec<ChunkInfo> = Vec::new();

        let mut generator =
            async_low_variance_chunk_target(reader2, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS);
        while let Some(a) = generator.next().await.unwrap() {
            async_chunks.push(a.0);
        }
        eprintln!("aaa {:?}", async_chunks);
        eprintln!("ccc {:?}", chunks);
        assert_eq!(chunks.len(), async_chunks.len());
        for i in 0..chunks.len() {
            assert_eq!(chunks[i].length, async_chunks[i].length);
            assert_eq!(chunks[i].hash, async_chunks[i].hash);
        }
    }

    #[tokio::test]
    async fn test_async_chunker_uniform_string() {
        // this test uses a uniform byte string
        // and a small iterator input size which
        // forces the chunker to possibly have to read multiple times
        // before generating a single chunk
        let seed = 12345;
        let input = generate_uniform_string(PER_SEED_INPUT_SIZE);
        let mut reader = Cursor::new(&input[..]);
        let chunks = low_variance_chunk_target(&mut reader, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS);
        let mut v: std::collections::VecDeque<Vec<u8>> = std::collections::VecDeque::new();
        let mut start: usize = 0;

        use rand::{Rng, SeedableRng};
        let mut bytes: Vec<u8> = vec![0; PER_SEED_INPUT_SIZE];
        bytes.resize(PER_SEED_INPUT_SIZE, 0_u8);
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        while start < input.len() {
            let tlen: u16 = rng.random::<u16>() % 4096;
            let mut end = start + tlen as usize;
            if end > input.len() {
                end = input.len();
            }
            v.push_back(input[start..end].into());
            start = end;
        }
        let reader2 = AsyncVec { items: v };

        let mut async_chunks: Vec<ChunkInfo> = Vec::new();

        let mut generator =
            async_low_variance_chunk_target(reader2, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS);
        while let Some(a) = generator.next().await.unwrap() {
            async_chunks.push(a.0);
        }
        eprintln!("aaa {:?}", async_chunks);
        eprintln!("ccc {:?}", chunks);
        assert_eq!(chunks.len(), async_chunks.len());
        for i in 0..chunks.len() {
            assert_eq!(chunks[i].length, async_chunks[i].length);
            assert_eq!(chunks[i].hash, async_chunks[i].hash);
        }
    }
}
