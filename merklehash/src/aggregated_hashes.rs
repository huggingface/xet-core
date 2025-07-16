use std::cell::RefCell;
use std::fmt::Write;

use crate::{compute_internal_node_hash, MerkleHash};

pub const AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR: u64 = 4;

/// Find the next cut point in a sequence of hashes at which to break.
///
///   
/// We basically loop through the set of nodes tracking a window between
/// cur_children_start_idx and idx (current index).
/// [. . . . . . . . . . . ]
///          ^   ^
///          |   |
///  start_idx   |
///              |
///             idx
///
/// When the current node at idx satisfies the cut condition:
///  - the hash % MEAN_TREE_BRANCHING_FACTOR == 0: assuming a random hash distribution, this implies on average, the
///    number of children is AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR,
///  - OR this is the last node in the list.
///  - subject to each parent must have at least 2 children, and at most AGGREGATED_MEAN_TREE_BRANCHING_FACTOR * 2
///    children: This ensures that the graph always has at most 1/2 the number of parents as children. and we don't have
///    too wide branches.
#[inline]
fn next_merge_cut(hashes: &[(MerkleHash, usize)]) -> usize {
    if hashes.len() <= 2 {
        return hashes.len();
    }

    let end = (2 * AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR as usize + 1).min(hashes.len());

    for i in 2..end {
        let h = unsafe { hashes.get_unchecked(i).0 };

        if h % AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR == 0 {
            return i + 1;
        }
    }

    end
}

/// Merge the hashes together, including the size information and returning the new (hash, size) pair.
#[inline]
fn merged_hash_of_sequence(hash: &[(MerkleHash, usize)]) -> (MerkleHash, usize) {
    // Use a threadlocal buffer to avoid the overhead of reallocations.
    thread_local! {
        static BUFFER: RefCell<String> =
        RefCell::new(String::with_capacity(1024));
    }

    BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        let mut total_len = 0;

        for (h, s) in hash.iter() {
            writeln!(buf, "{h:x} : {s}").unwrap();
            total_len += *s;
        }
        (compute_internal_node_hash(buf.as_bytes()), total_len)
    })
}

/// The base calculation for the aggregated node hash.
///
/// Iteratively collapse the list of hashes using the criteria in next_merge_cut
/// until only one hash remains; this is the aggregated hash.
#[inline]
fn aggregated_node_hash(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
    if chunks.is_empty() {
        return MerkleHash::default();
    }

    let mut hv = chunks.to_vec();

    while hv.len() > 1 {
        let mut write_idx = 0;
        let mut read_idx = 0;

        while read_idx != hv.len() {
            // Find the next cut point of hashes at which to merge.
            let next_cut = read_idx + next_merge_cut(&hv[read_idx..]);

            // Get the merged hash of this block.
            hv[write_idx] = merged_hash_of_sequence(&hv[read_idx..next_cut]);
            write_idx += 1;

            read_idx = next_cut;
        }

        hv.resize(write_idx, Default::default());
    }

    hv[0].0
}

/// The xorb hash
#[inline]
pub fn xorb_hash(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
    if chunks.is_empty() {
        return MerkleHash::default();
    }

    aggregated_node_hash(chunks)
}

/// The file hash when a salt is needed.
#[inline]
pub fn file_hash_with_salt(chunks: &[(MerkleHash, usize)], salt: &[u8; 32]) -> MerkleHash {
    if chunks.is_empty() {
        return MerkleHash::default();
    }

    aggregated_node_hash(chunks).hmac(salt.into())
}

/// The file hash calculation from a series of chunks; to be used when there isn't a salt.
#[inline]
pub fn file_hash(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
    file_hash_with_salt(chunks, &[0; 32])
}

#[cfg(test)]
mod tests {

    use super::*;

    // A function to print out the correct values for a variety of hash values.
    // Uncomment this and copy-paste the printed reference code into the correctness test.
    // #[test]
    fn _print_reference_hashes() {
        fn rh(h: u64) -> MerkleHash {
            if h == 0 {
                [0; 4].into()
            } else {
                MerkleHash::random_from_seed(h)
            }
        }

        let base_reference = vec![
            vec![],
            vec![0],
            vec![1],
            vec![1, 2, 3],
            vec![1; 4],
            vec![1, 2, 1, 2],
            vec![1, 2, 1, 2, 3, 4],
            vec![0, 1, 0, 1],
            vec![0, 1, 0, 1, 0],
            vec![1, 0],
            vec![3, 4, 5, 3],
            vec![3, 4, 5, 5, 5],
            (0..8).collect(),
            (0..64).map(|i| i % 8).collect(),
            vec![1; 32],
            (0..8).chain([1, 1, 1, 1]).collect(),
            (0..8).flat_map(|h| [h, h]).collect(),
            ((2u64.pow(33))..(2u64.pow(33) + 100)).collect(),
        ];

        println!("let reference = vec![");

        for (i, v) in base_reference.into_iter().enumerate() {
            print!("(vec![");

            for &hi in v.iter() {
                print!("(\"{:?}\",{}),", rh(hi), hi * 100)
            }
            println!("],");

            let hash_list: Vec<_> = v.iter().map(|&hi| (rh(hi), (hi * 100) as usize)).collect();
            print!("\"{:?}\",", xorb_hash(&hash_list));

            // Now do a few salts along with the 0 salt to ensure we get good coverage there.
            print!("[");

            let salt = [rh(0), rh(1234 + 2 * i as u64), rh(12345 + 2 * i as u64)];

            print!("(\"{:?}\", \"{:?}\"),", salt[0], file_hash_with_salt(&hash_list, &salt[0].into()));
            print!("(\"{:?}\", \"{:?}\"),", salt[1], file_hash_with_salt(&hash_list, &salt[1].into()));
            print!("(\"{:?}\", \"{:?}\"),", salt[2], file_hash_with_salt(&hash_list, &salt[2].into()));
            print!("]),");
        }

        println!("];");
    }

    #[test]
    fn test_correctness() {
        // A reference test of saved hashes that verify the correctness of the cas and file hash functions.
        // These are generated using the function above.
        //
        // The format is ([<node hashes>...], <cas hash>, [(<salt>, <file hash>)... ]), ...
        //
        // This is intended to be used as a reference to ensure that other implementations or ports of these
        // functions produce the correct hashes.
        //

        let reference = vec![
            (
                vec![],
                "0000000000000000000000000000000000000000000000000000000000000000",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    ),
                    (
                        "c1cdea79b61cd4776c9f3f8e08767fd5f42e33f9cec8c13d01b947901fad1954",
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    ),
                    (
                        "8d948a82def8a5683477f953796702a015caa2fce6db8d692cef8853c20c6dd0",
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    ),
                ],
            ),
            (
                vec![("0000000000000000000000000000000000000000000000000000000000000000", 0)],
                "0000000000000000000000000000000000000000000000000000000000000000",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "638a6bc391964a85939d48f008e8bdbae6a7975e7ca2d87a3ce2492f4e4d8a4c",
                    ),
                    (
                        "f4aa7219b7bc6145df344b930c1cf63680037e13236f4b3b8f439aba1520d443",
                        "3e44e4a200e05a69afc979eb2e0507817e4f0ddff128a97e563c0e05fed3bd25",
                    ),
                    (
                        "51d15f80fa3c9b0c42673a3e27206741e6b8881ae902614a8dd45e6e0e4ab1d2",
                        "7a6c58ad3831f6ac16727ebe8e1a4e7d9f509ba8e1f75f5f31c5e67c66a94486",
                    ),
                ],
            ),
            (
                vec![("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100)],
                "cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "8e16257caa3fe079d484d872a8975264b2ff683b0d6db9028cc7c0f968a45661",
                    ),
                    (
                        "285371a6a3c8c4e561469c97766c46f0b9799bd8ea51a4ad27a082df8c28b7da",
                        "6c18994ed5e6a223aeff68a6e386dbba7b3c25de076245dc77892b4ad49ac978",
                    ),
                    (
                        "6e88acf8fc9f70ab609a46a58e93404f221bf6f1b95b4a12364032de9d3fbdf2",
                        "c5af028b084ed1d7f9f131b60724547df167a149fdb39a7fcd6c92fbc777bbcb",
                    ),
                ],
            ),
            (
                vec![
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                ],
                "71ec1275fca074724e2dd666921b3277c7cee603e4d025bcab2d4050015be2bc",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "54e55dccc6653c612bdb5576c5d3cb34bb31bc4e100248abccf4c908b3ef7715",
                    ),
                    (
                        "b286606709ef32a1cfede1e603a39f83fef56772d8234c0e35d8920071d80b69",
                        "337f1f046e4cfc1057f0f3edbd87cf977fdfaf3c2b0c0031ca2d1d2a34aa2270",
                    ),
                    (
                        "20f7357cdb2f6d3727375656e4eaf464fa195094ff501edc08aaccfb1419a07b",
                        "82c5359ad947e91e4da6a00034f94a9159bd3bf24d005ef167648b9d3ba77f8c",
                    ),
                ],
            ),
            (
                vec![
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                ],
                "89f2ada89ff8c96763c6b25010e6dd76a4c05b1466207633ea559acf2093211b",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "2cdba690d0e09563596e0cda626d43eb4c96ef1e994fe72d9b2f5a83cfcd36dd",
                    ),
                    (
                        "705cc0697edd49c5258951922f5df72e16deaa58f85e78c381a574c4e3c9ad94",
                        "a3da5ccdbea87eafe6400ad30e45df94bcad053d72c246ff94a7934a5a35abdd",
                    ),
                    (
                        "8034daa5ecbb2747e07125153b27fd7fe80e0987a90ff35038e65ede487061fb",
                        "39f759769aa24443a9e01d65e168790bbae126d604524d09d8f5bb27cc1f9a19",
                    ),
                ],
            ),
            (
                vec![
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                ],
                "90f8313ef12df385d237a067aded02562c35ded12116e32eba401dbc86c38031",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "284ea045e5a579e99c21ec597c20de1fc0c09e7168162aac00db8f61b3d82dbb",
                    ),
                    (
                        "d08d1b24938b34a558972c6fe99f6153ef502f064b74d8ca890cdfb8eb4f953c",
                        "6812247dc5740215154ace7e4ffe271914da445524492677d36bdb6ec1282142",
                    ),
                    (
                        "210cfc75c396d665afc41376548e139fb62d3af883b94535506f30fe5e5af054",
                        "adbce79dba197ed0f33290c11bb44ecd5bca689dd57997998e3fdaee60846acb",
                    ),
                ],
            ),
            (
                vec![
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                ],
                "52c826f99507aa05d0b45e9837fa1709e0485425cfbcb1e80db3905cf98b3ee9",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "91d21684db364c8883ab8209fa5eb2e781cf07f37e1fa43e731c30839afe422f",
                    ),
                    (
                        "fceabc1f6c6aaaa176bc53b2d6899a3ab99eaebd13b5f0cf36080e6f894bbbf5",
                        "b70b051189a20b58d0c832111e0470396094518b6af8bd8c813c9ccb764ab602",
                    ),
                    (
                        "2bebd697343f19943b09f4d4f72e9b38e75cb77a484fd2b004f911b6ba747f4d",
                        "7891a3024bab1566cd7d421b90c31ca51db694647f7541211a0adeb188f26f4d",
                    ),
                ],
            ),
            (
                vec![
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                ],
                "e1755c85be07fd6b7a8d7becc85e6d8c5f507f2a3d38b316bcb6f97855e72188",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "207caf24fce045ac6d873b4c6186a5a435d600d103e1ae345e55ddf1f4b7f7ef",
                    ),
                    (
                        "031f44db2e13e20c068a83d67c607b5134ddd0e627c2a4f9a6335feb0340625f",
                        "5d343ba4d940ae1a7039774456702e1e45400d47f2a04ae987046984a440b2b5",
                    ),
                    (
                        "024d165e5cc2770217cbfe0af422666f4333f94f35faca6d4450be7eac3233ba",
                        "5b7497691632f794a40d33908473770034a651fe76bd8036a4b1c1c2246b0bfe",
                    ),
                ],
            ),
            (
                vec![
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                ],
                "f36c6a6374a567efd36ed087aeb680b57e3af4e0b5b8e7be2593df4cb647a2de",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "ab2f46d31855e1133b3b5b70e78f841394a648710a208923b221c1a8a0572a1f",
                    ),
                    (
                        "80d0b5a2922c67f61ffdc2b9f6e9272a92542a3ab319ed0e6bfc40224b0e033f",
                        "82505151e4e8ad313359150eaa7c13cff3130458e52cdc33c49a6199e47896d5",
                    ),
                    (
                        "a2ceedc3d08cd002198aca5ef67304f383bd25a159de0f440c19323f86675c1d",
                        "79ff4e9e2c05800de3a15a1a450383f18458f772f5c0dc16b32e3f4f27006c4d",
                    ),
                ],
            ),
            (
                vec![
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                ],
                "e8660f81494ca836a58e395c1395ef97870ed71e2b113eec1fab6b3361f46dd6",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "274d92f7e2acebaa2b8d63c0b5e7a4fc15814a606e3e3825d55609e671bcc5d9",
                    ),
                    (
                        "b0fd0f1d42902c9e8156968aff956ace74cc286d5f4ad84b98865069175e3d1e",
                        "1472e0ec0085c508d31759d0d2df6656e00080d83340130e28772d231bf5a8d7",
                    ),
                    (
                        "200519fb7604080db7f8806fe9f14ca6a4ebac59fc8c99ab678c0dfaccb42bc4",
                        "4d9137d15623213a6feec629ac21a685a1abcfad348134305fcd1389e302c43f",
                    ),
                ],
            ),
            (
                vec![
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                ],
                "73b9dc802ab03a0855c66823a18d95c2a785af594535756abf405038b6270cb8",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "c7d5be75ef5ec12888027ad305141e4109e599494477661177475c84e12de48b",
                    ),
                    (
                        "d255e7a939311ec686dc825070a5e603147539a80035999a4dcaa698afd4aa7f",
                        "7c58c7b4130ac526c98fff562bb4317581846a04e6ba4f08fa8a829e1c033c78",
                    ),
                    (
                        "0ce9f480d557575949fd9d854b768b5de6b90c5875151bb8d2b58d341731fdff",
                        "07fd7ff34d1d4e12b58be73d3736902bca11b37e82ab97b65cfa5b2950a4418a",
                    ),
                ],
            ),
            (
                vec![
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                ],
                "2c1fd1eca47e284512fcb40fcf45a1874939f9b91c774d46aeba5d004d9d263d",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "0799ba7ee2c3fed173a8e8d832cd13970ddc16070ffbc8aa6dcd77476f66dc73",
                    ),
                    (
                        "498ba54b410ea8f05c4b92aa5a6f01165615f6a8d1ec1d7cee76a85fd800ba28",
                        "c98a2a18e6df96f4d6dc5da396e17a2c22867b49b977ab6860de5b3241776baf",
                    ),
                    (
                        "d4386029ad5c96f5b8ba3ccac43498eed2ebe601a82448d9a570d851ff82d7ce",
                        "242c0a9680a8df0894d5f2ab5cbeacf64178ec1ff01eb75cfb5dfec791ccabb1",
                    ),
                ],
            ),
            (
                vec![
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                ],
                "f62abe77e3fb9c954fe52b0028027ddc90c064c45951a4fd2211d87e5c0011db",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "d1b068be5bbdb38992269e8efe61f601881e39f7a7585fd76883cc6ea5c23b44",
                    ),
                    (
                        "6fb1cc393b130809e39c8073f96d9726364080a345a5af4978ab491e7d25faac",
                        "eebe56be63552edcc6da65ff79f79262f83866236dd869f08a900674fc936ad3",
                    ),
                    (
                        "9a251cb6ea998277ec94eeba84850ca3b1a01b66719043938c9695f72457abb1",
                        "8adf902c706ff117e38e10bbd076d5f139aaf6704159652d189d1350ac24b407",
                    ),
                ],
            ),
            (
                vec![
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                ],
                "6554007c9b5d0a5e7918f79596a1b68815c1407535585435f5735db761f21b88",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "a8640ab81d48854e00078e12b1ea8be5d90be0ffb5f73a15b7009981d093ddd8",
                    ),
                    (
                        "0c33b4f4788ad18fb7c5fbec8fa2ce3c3208e2737d8d0fd7c4a1ff97b2c0d866",
                        "3e819808396e8c1960a11be2b952b3b824ab1f4c44a46ba7f753d54127fc1987",
                    ),
                    (
                        "a53474562b6b74982e2d73a9899d63c8e5f99c9fe331b7c1079e37d5e3f27224",
                        "702d28c85f0c8d402436b14ad759850e8c439d382bcbf50578dd52a2a9879022",
                    ),
                ],
            ),
            (
                vec![
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                ],
                "0a0123c1617921883b7e13902095fcb86676e77c49120c33b233003b0af0e0a6",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "53af4711fd1d5e5bdc7f931b6be932314d8d673cb16ad2482f6f5222eaf9e63d",
                    ),
                    (
                        "058034313dd012e3cdb618509708dda40488f573cefd0794b97926a431b60b86",
                        "1b4aa28be3856b3c7a435622ddbcc4c4f81ef19702864a63ceb28653e52deeda",
                    ),
                    (
                        "9a5d9092dab617e2572cc23b4bababd96fd6744fb6682b42392b281386abdc17",
                        "b71e783a51a19f07b62f386b60363a0b96e036bd9dc9e651b9373e0e94b4bf53",
                    ),
                ],
            ),
            (
                vec![
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                ],
                "e7fc30b73ec9930593f80fe777b334e9d3bb73e45e6a79fa5784b2603446158b",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "69e1af2fd6023ce133e0d1b90e32d1d42ef79f5a710f2b0accfc3a735e98de9d",
                    ),
                    (
                        "0d3ba68f70ce4c1f642b12ff526fefe28db27becb0bbf2ea181193f70f6c3f09",
                        "fe45928c54aa340354af57c83cf6eb8056a2d2e623a1f73afdc3e777babe7ba1",
                    ),
                    (
                        "efc42bd1c8484ee777279eb0ea378508e1b3cb2d242664f43355bb971994f42f",
                        "d640ee6a5ebc1975dcc4b860fe4377736c0c46a3d6060dcc3a937ab1866e0c8d",
                    ),
                ],
            ),
            (
                vec![
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("0000000000000000000000000000000000000000000000000000000000000000", 0),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                    ("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700),
                ],
                "636c65db520ec16e8166f9ec7a0cf9f74b26369077ac60160ec0221c86bc9f77",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "6a90ad0d6f5246ac61f1de898c6944c03e14004a1368e6fb23387108c4d039e0",
                    ),
                    (
                        "9cf26338dd0a43d6b8ae238d197a558db083f42918b6abebe9c6008a982459fe",
                        "7f14769cb2a682654aa7aa6b08845643b6973adb313b975fd319d34456bb9159",
                    ),
                    (
                        "d4a66ca34b2a86f1ef1f1bf25ab63b564deaccd1bd1a8d5c8c52524cc7abe32d",
                        "26d4b962af66bacd758377a44cd7ea0d31defde460cdde494053e63456747e45",
                    ),
                ],
            ),
            (
                vec![
                    ("4bb8167baa56fe2e2c9bd231c17da8103d0e5880952f5f95ece4ba36f12f780e", 858993459200),
                    ("264fa414472e8c6a7db4cf4d09e98b06b3dbadbc0b0a22998c37e5a3266cbaeb", 858993459300),
                    ("0143189512fc8ff13863dd29cddfd27843fb33a1eb2d74bb1e88376ad314514d", 858993459400),
                    ("5bd05c0a044e37de8dee2a661c006c4d38cbe2f52b7dfb7886c0e4fe643f1320", 858993459500),
                    ("98b8f089d87826e97d9372589354df5b13d5c44128e42a107d81c2f5ca5b9935", 858993459600),
                    ("41a664ad7bcde31caeb2326def866d21c3e6af93eea93c2e97341954a99645d5", 858993459700),
                    ("1a07f38ecc220857e2ae8992c9dbf687b420b8e6b78ed18553db6107096e6674", 858993459800),
                    ("3e284cd4c5d3742c9c2a30d38592da97db560db3f7b300ed99e2cf0f57493fc8", 858993459900),
                    ("2876ca5532dfd7ee48cd45b397640454156d73a9d9eb8c495d1c946cec641b8d", 858993460000),
                    ("f861844b7cc10d2cb9c8496ec38ab761610ac71494c96e914b477a86a13beee1", 858993460100),
                    ("b90535aa19f785f289cd8a414b27f1eb2862c287ee5352412cfab21a5991d4d0", 858993460200),
                    ("cba9e36df25bac3059e8c40e733365b51d28cff9fe8f2cfd0b8166a21880706e", 858993460300),
                    ("0542bc8841498c90fc6c502cca431f455267fd55b70b57adab64562836c7cff9", 858993460400),
                    ("a43c0a53ff26d2174d9124a6fdeac1ec56775993cf890a0e8a5f23ef0047936e", 858993460500),
                    ("9acb79956803fabf3ad62f3509d472e2b854a0a603c2b1f4fad265bb2086efa6", 858993460600),
                    ("32689cb7dca6b1168f98d627ce793a3fc0f2774ada4014be1ee4a64fb56287af", 858993460700),
                    ("94fdf9880e23103cd1b271e49fa73e6863982772025080a2a33e566dd0ce92c8", 858993460800),
                    ("c689977f4153226f41217ed8a4b2d4a8e77b0767fa3ce2c9aaaba88c852007a9", 858993460900),
                    ("2cdc473fae65555c10cb8e0c8612c1ae7a98f315c63ef022dbc70e447767dd44", 858993461000),
                    ("027717d5c1ead1d2b3794d5c0ff6271712be541c10d402232c2f14d64a03ffd3", 858993461100),
                    ("a54d9bdf618c585ce4b841093a84efc90919ce42e465f30919957e34c144c3bb", 858993461200),
                    ("bda4c65c7d357bcfb15e8de64adfee01bdac58941712b1f3dbd79d56dba2e9ab", 858993461300),
                    ("ac6ced48dcb39ba66edcf2eef64b3e1056efc651babbc1942a8bc2c291a8d912", 858993461400),
                    ("11e3670cb291e19455346923effb9af28e637b0d6bf5829c173b6a702df142d8", 858993461500),
                    ("262cee84de1583b570f7c8ca62657d7bf6805dbd937e3b15899beede88e6d033", 858993461600),
                    ("d7bf4b9589f88e27910726714bedbd3fdf8bc22d6dd5584cbe9e917d81d16d98", 858993461700),
                    ("7746419c6931e1b71995d3b03a9757c0431886bf1e84b6353a8d6a8d4a6acf83", 858993461800),
                    ("b64f118e4db3652e5d729b621c7e1810bcc3ccdd6fdaf11397a24cfe50a5cc0c", 858993461900),
                    ("55a21c2a6c4b547e44abd598ab763af93553f84c7ecee06bb381a1e228a67c40", 858993462000),
                    ("cc956735a3d3e6cbdb5c1f352b3047ba1d43cb0170fe56edef5b1883307d8544", 858993462100),
                    ("ccbf9573bd67c4dcb2bb18faec1a5dcd9930ad7ac44c5fef343502213013d6eb", 858993462200),
                    ("7addab5065154e0af972acdb3381e6dd1d74553ddd34c1aa7e1221e4b5b296b2", 858993462300),
                    ("aa3bc51b8edc616972cda20e2e7c014530dcb32ec8692dc3cdad5f35a462bb96", 858993462400),
                    ("9bdea725dacdbfdfb4e2044c5acceadf5d0c49485fcab89b6ef896d52a4a09d1", 858993462500),
                    ("edde7b9880ec5303ccc8401cc9ec5e6a6a18c62353746da14da3b2bbdd524cad", 858993462600),
                    ("9d15f7098eda6921ca6c26da00ede3d8fc6320aec400ff8b067a7be125602d6b", 858993462700),
                    ("262f8c4fb624bea0a8beb3b95e911fe5050be9702a29319069f4afdbe0044105", 858993462800),
                    ("22efff126110192a0ad352e44df8d2bb1b5e1b2aab20c4c046d8c290081814dc", 858993462900),
                    ("39d72f71defd8cf7ec971c8367cbb89dc4042a5520978c139375fc60fc96c594", 858993463000),
                    ("b99b719c74883275baa7ef5971d64e6ce897ad66ded1801a5a0cf3decae9feb7", 858993463100),
                    ("19f4d995747b316f17e575c365884672345843b689b466a2419a7fe731d1ff33", 858993463200),
                    ("e02df52e816323bbd259f2c98d99e07ddfba0170005915b09cca07ec46493dea", 858993463300),
                    ("47cc8acc3eab9d7e57e3b4b36c452b29817d837ba0744233f66d5abb397066a7", 858993463400),
                    ("eb586c3af09fbaa913969d5d221e79e94262729d7dc7424d4afa54c4ea41c798", 858993463500),
                    ("58819a31ac66ea0e59def9fcdca5910811f9a8076402da15b879b2de6b58dca7", 858993463600),
                    ("124a6d441350fdbf089656a5bc634aa3108013db2460283087c0b2f9698d064a", 858993463700),
                    ("f54815a68307bd7999ca071138041449ac4a06403d1f882b159ca91b0fdb15eb", 858993463800),
                    ("f9fd25cf5ed50d96051b2e093d3e7bffd95e4d8e5bab2bf0a4c0abd44b432e5d", 858993463900),
                    ("f3a84473e24579a7d66abd673bf1c2799f6a7578d243d177a7a5ddec1def524e", 858993464000),
                    ("8a9ef2d746f6a3ee37b03960d8aa04d345f13887d91b16944089f318c3a5db79", 858993464100),
                    ("47d738e05820835e441ccefa363e3674766a8ed9d503d1ca1d088a4eadf81368", 858993464200),
                    ("e72960cf3cb781e96a93cffaa17347a9de98b8ae8c44caf9a277155d26accc4b", 858993464300),
                    ("d2a7a4ab1ca5aa155a4236b9cb70224fa71283787747f0e4c9b92bd353addaf5", 858993464400),
                    ("da077b2eec352d6a06d3113421f29718f49067f4c7fefda0d1c92f02568a0c8d", 858993464500),
                    ("348e59244d0c798ed5e948e53d0adb33b1a333ba742aa2629bc57634ec1c4145", 858993464600),
                    ("c6d5b78963db3be2eb72d5f50e0863905f031a8153a0192014fc70deeffda18d", 858993464700),
                    ("1e0270a4723d0b5137d32963bf698d8d75c6ddbc45b43012ead3d74df421b43a", 858993464800),
                    ("3d9f4da6df392205a748f2dc805d9d528dded20fbeb8d8ee8b08ad98ad012772", 858993464900),
                    ("03b41bc24a857342b05957bccf58c0a5f6ff6892ad0ff286d215dd7b78cb1916", 858993465000),
                    ("79ff727cb6227413efdf3ef49e8b8da518aee0eb4981d0bb8e5121394ee97e8a", 858993465100),
                    ("32453e5156a6e091e7e86a05c4771a84d7d44a1674015a6053f80e339a6f409f", 858993465200),
                    ("9352139ebc1ee29a887c2c421b3b8f4f6a9c411e73856751a058153b90e11a2f", 858993465300),
                    ("f252ef14829f0300aca6f722d2eb77e71bf103aacf5c5b66e1811336e7192697", 858993465400),
                    ("1faa98fd5be526bf554f43bbe725faaa5efc290d6b2399526876a60377df1f0d", 858993465500),
                    ("2f11884b48aec3bcccc12bdb150c2eb410643242d8575168c6a5ba75530053a3", 858993465600),
                    ("e19626b6e8c662266d0b88b4e2d2c04b7985fd27a8c2080eddf1679482aede70", 858993465700),
                    ("ef68559f9f949ebc30fbde3fdc5df52cee3babd722abb40e0fd1b19b26a6c5fd", 858993465800),
                    ("4c0f078b5206b52b2c740810d74c31cf669f87145ecd6a866b53c0fdc92ed545", 858993465900),
                    ("2ba6fbdc8531a945ce767db224542f4bdf68ea23a1d39d6660b93f0b4ac3c886", 858993466000),
                    ("0ca69a8a1045003e23f30de516fd833ea1147e53c71125f6de680ae580bb0b2a", 858993466100),
                    ("0d9ce05040f092adf4c48a8753558d1b003f2a64eef2fc24cdc6a065b42200eb", 858993466200),
                    ("e41d9f14a678ceed00680679664ff7a95ef8e5e2a691b74c7493d36dcd5cce6e", 858993466300),
                    ("fd5e47e45413eaa4ea4455c7e8a508beea9da2ff63e334f809964e5da1d35eb1", 858993466400),
                    ("255fe31523e7f14e854a56a46492dff3fe0a92b13ecb8c5e3c41911cb9802df4", 858993466500),
                    ("1baa420f0e1c2b5b79c05661fc7b15365321599b3cf28629410ba484f3a526be", 858993466600),
                    ("4b904af6bb31a324998f5ed7f8c3961b3c6cfa2fb797a16e9427f32c57c45937", 858993466700),
                    ("07b802a04568538068a9db24a0ce06794fa0ebd59f8b3c284f362b2d62abb6cf", 858993466800),
                    ("915f764361e496714a26c1c314bfd4b3a1e1d5cf2cdb1fea3ce2a46c49d84bfc", 858993466900),
                    ("515971f1ff66b929c173fd423aa17fa3a7f9f26239d5029dcad68a4b8140fdb4", 858993467000),
                    ("e5a9e2625e21de37d2e6886640ef3a6a3d7b4b0f0f360653cc54d69d66f80b3b", 858993467100),
                    ("41601754f8ef7dee3c248e981c3cd0590b6f413ee082b6fdbf678c15142f5e4d", 858993467200),
                    ("693ba2c7b0930ac2d64464c4e33952a2e79866c38b70540b9671fdcc9be78f50", 858993467300),
                    ("09eaefa1e2e8ed20344dfaeae186170cc0d76dc32b80997026609fb981a75ab2", 858993467400),
                    ("6288ca28b610996253ffabd14b47870ef8c1c4c32fa96dabbee50b127dee2387", 858993467500),
                    ("44aa1b86bfd96d5aaed4cf12b8562d4d126a33f33c48ae4513fb538bd397c8be", 858993467600),
                    ("f1a6350bebd252c9413754d80df17a10f91ce7ec662e98f4cee1c82ba4fbede6", 858993467700),
                    ("a1f557153593f38d94d7f6e24c01fc941d9567d17af67e8539f1b81bc51ee264", 858993467800),
                    ("a18a395a1e5e7025f7f37f46f8aef8e0b32fa892b0f4e869b05c7f45c72cb064", 858993467900),
                    ("51b39cb7f3e8b88067fcb4ed7fffd681516dfb55b383a8b413f4237fa773c4ce", 858993468000),
                    ("05d80656464074580c40cfbac2e2e60d761c0600675862ee63701d7a5d3433a2", 858993468100),
                    ("fa80e2c99389bcb0c43228a14b0035a2d885b05f04c3895c92dbb8ef8e00e29a", 858993468200),
                    ("8968e28ce21a2507f55ab8ae865c669ca1d2104cdd911604f6f73927fb7b3d35", 858993468300),
                    ("4a1fc357463fd231064bc5ee6eb32bcc749750072d85e81e2cfbebde4f7e0018", 858993468400),
                    ("4f8fbbf4d8654203636f5f639ddf0c78f2f9575a825f00acbfba1552c6b91dca", 858993468500),
                    ("b38a53037470a9f90fb0a3df72caf9cf58642ee8750bb7244d60efdeb598f79c", 858993468600),
                    ("96dc535b16ea51143ed885e92d7e5e0e010b86e3f93c992ddf52af537e3c4f4c", 858993468700),
                    ("4e6faa2b9c3c8f7075cb12d069ac2cb07f13cfc506239f715e9e8c071ecaf878", 858993468800),
                    ("87ba523ddb6112bf72d5dcff817919312f2650cb4742f47b289ba2345da5daf6", 858993468900),
                    ("34a7e7b01ef534717ac938a586f871a2549cf38adc776557e6d91cf0bf4046ad", 858993469000),
                    ("32d60c12ce51cd05bbdd8d1a192963686523e638015ccbc68c2065eb54232934", 858993469100),
                ],
                "c51247b5782028a5b8fd8191c855e46c02ba5923ceeed828b167614d47fb6686",
                [
                    (
                        "0000000000000000000000000000000000000000000000000000000000000000",
                        "4bacfd61a7f5401a3a55770234bf6ab0ce9a44446d81e9ce9977220fb9022406",
                    ),
                    (
                        "fc2ec3634962518fcccf69fa901b930a1e71bc859a0d877799fbd1d0dddc10d7",
                        "b451a0e35458e1d21445735852844d3776cef13f255a875c4fa37fd76c7f2637",
                    ),
                    (
                        "9c9a3e2c77df0b1f4292d0c34b7c9752c01479bc6953656527aafef81fee6c45",
                        "8ce603595da4bd4518ee33a0e58c28309f78ef5cce23f1b53ad048f360cd25e5",
                    ),
                ],
            ),
        ];

        // Go through and verify that the above values are correct.
        for (v, h_cas, file_hash_list) in reference.into_iter() {
            let v2: Vec<_> = v.into_iter().map(|(h, s)| (MerkleHash::from_hex(h).unwrap(), s)).collect();

            assert_eq!(xorb_hash(&v2).hex(), h_cas);

            for (salt, h) in file_hash_list {
                assert_eq!(file_hash_with_salt(&v2, &MerkleHash::from_hex(salt).unwrap().into()).hex(), h);
            }
        }
    }
}
