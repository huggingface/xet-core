use std::ptr::copy_nonoverlapping;

pub fn bg4_split_separate(data: &[u8]) -> [Vec<u8>; 4] {
    let n = data.len();
    let split = n / 4;
    let rem = n % 4;
    let mut d0 = vec![0u8; split + 1.min(rem)];
    let mut d1 = vec![0u8; split + 1.min(rem.saturating_sub(1))];
    let mut d2 = vec![0u8; split + 1.min(rem.saturating_sub(2))];
    let mut d3 = vec![0u8; split];

    for i in 0..split {
        d0[i] = data[4 * i];
        d1[i] = data[4 * i + 1];
        d2[i] = data[4 * i + 2];
        d3[i] = data[4 * i + 3];
    }

    match rem {
        1 => {
            d0[split] = data[4 * split];
        },
        2 => {
            d0[split] = data[4 * split];
            d1[split] = data[4 * split + 1];
        },
        3 => {
            d0[split] = data[4 * split];
            d1[split] = data[4 * split + 1];
            d2[split] = data[4 * split + 2];
        },
        _ => (),
    }

    [d0, d1, d2, d3]
}

pub fn bg4_split_together(data: &[u8]) -> Vec<u8> {
    let n = data.len();
    let split = n / 4;
    let rem = n % 4;
    let mut d = vec![0u8; n];

    unsafe {
        let data = data.as_ptr();
        let d0 = d.as_mut_ptr();
        let d1 = d0.add(split + 1.min(rem));
        let d2 = d1.add(split + 1.min(rem.saturating_sub(1)));
        let d3 = d2.add(split + 1.min(rem.saturating_sub(2)));

        for i in 0..split {
            let idx = 4 * i;
            *d0.add(i) = *data.add(idx);
            *d1.add(i) = *data.add(idx + 1);
            *d2.add(i) = *data.add(idx + 2);
            *d3.add(i) = *data.add(idx + 3);
        }

        match rem {
            1 => {
                *d0.add(split) = *data.add(4 * split);
            },
            2 => {
                *d0.add(split) = *data.add(4 * split);
                *d1.add(split) = *data.add(4 * split + 1);
            },
            3 => {
                *d0.add(split) = *data.add(4 * split);
                *d1.add(split) = *data.add(4 * split + 1);
                *d2.add(split) = *data.add(4 * split + 2);
            },
            _ => (),
        }
    }

    d
}

#[inline]
pub fn bg4_split(data: &[u8]) -> Vec<u8> {
    bg4_split_together(data)
}

pub fn bg4_regroup_separate(groups: &[Vec<u8>]) -> Vec<u8> {
    let n = groups.iter().map(|g| g.len()).sum();
    let split = n / 4;
    let rem = n % 4;
    let g0 = &groups[0];
    let g1 = &groups[1];
    let g2 = &groups[2];
    let g3 = &groups[3];

    let mut data = vec![0u8; n];

    for i in 0..split {
        data[4 * i] = g0[i];
        data[4 * i + 1] = g1[i];
        data[4 * i + 2] = g2[i];
        data[4 * i + 3] = g3[i];
    }

    match rem {
        1 => {
            data[4 * split] = g0[split];
        },
        2 => {
            data[4 * split] = g0[split];
            data[4 * split + 1] = g1[split];
        },
        3 => {
            data[4 * split] = g0[split];
            data[4 * split + 1] = g1[split];
            data[4 * split + 2] = g2[split];
        },
        _ => (),
    }

    data
}

pub fn bg4_regroup_together(g: &[u8]) -> Vec<u8> {
    let n = g.len();
    let split = n / 4;
    let rem = n % 4;

    let mut data = vec![0u8; n];

    unsafe {
        let data = data.as_mut_ptr();
        let g0 = g.as_ptr();
        let g1 = g0.add(split + 1.min(rem));
        let g2 = g1.add(split + 1.min(rem.saturating_sub(1)));
        let g3 = g2.add(split + 1.min(rem.saturating_sub(2)));

        for i in 0..split {
            *data.add(4 * i) = *g0.add(i);
            *data.add(4 * i + 1) = *g1.add(i);
            *data.add(4 * i + 2) = *g2.add(i);
            *data.add(4 * i + 3) = *g3.add(i);
        }

        match rem {
            1 => {
                *data.add(4 * split) = *g0.add(split);
            },
            2 => {
                *data.add(4 * split) = *g0.add(split);
                *data.add(4 * split + 1) = *g1.add(split);
            },
            3 => {
                *data.add(4 * split) = *g0.add(split);
                *data.add(4 * split + 1) = *g1.add(split);
                *data.add(4 * split + 2) = *g2.add(split);
            },
            _ => (),
        }
    }

    data
}

pub fn bg4_regroup_together_combined_write_4(g: &[u8]) -> Vec<u8> {
    let n = g.len();
    let split = n / 4;
    let rem = n % 4;

    let mut data = vec![0u8; n];

    unsafe {
        let d_ptr = data.as_mut_ptr();
        let g0 = g.as_ptr();
        let g1 = g0.add(split + 1.min(rem));
        let g2 = g1.add(split + 1.min(rem.saturating_sub(1)));
        let g3 = g2.add(split + 1.min(rem.saturating_sub(2)));

        for i in 0..split {
            let fourbytes = [*g0.add(i), *g1.add(i), *g2.add(i), *g3.add(i)];
            copy_nonoverlapping(&fourbytes as *const u8, d_ptr.add(4 * i), 4);
        }

        match rem {
            1 => {
                data[4 * split] = *g0.add(split);
            },
            2 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
            },
            3 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
                data[4 * split + 2] = *g2.add(split);
            },
            _ => (),
        }
    }

    data
}

pub fn bg4_regroup_together_combined_write_8(g: &[u8]) -> Vec<u8> {
    let n = g.len();
    let split = n / 4;
    let rem = n % 4;

    let mut data = vec![0u8; n];

    unsafe {
        let d_ptr = data.as_mut_ptr();
        let g0 = g.as_ptr();
        let g1 = g0.add(split + 1.min(rem));
        let g2 = g1.add(split + 1.min(rem.saturating_sub(1)));
        let g3 = g2.add(split + 1.min(rem.saturating_sub(2)));

        for i in 0..split / 2 {
            let j = i * 2;
            let k = j + 1;
            let eightbytes = [
                *g0.add(j),
                *g1.add(j),
                *g2.add(j),
                *g3.add(j),
                *g0.add(k),
                *g1.add(k),
                *g2.add(k),
                *g3.add(k),
            ];
            copy_nonoverlapping(&eightbytes as *const u8, d_ptr.add(8 * i), 8);
        }

        if split % 2 != 0 {
            let i = split - 1;
            let fourbytes = [*g0.add(i), *g1.add(i), *g2.add(i), *g3.add(i)];
            data[4 * i..4 * i + 4].copy_from_slice(&fourbytes[..]);
        }

        match rem {
            1 => {
                data[4 * split] = *g0.add(split);
            },
            2 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
            },
            3 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
                data[4 * split + 2] = *g2.add(split);
            },
            _ => (),
        }
    }

    data
}

#[inline]
pub fn bg4_regroup(g: &[u8]) -> Vec<u8> {
    bg4_regroup_together(g)
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_split_regroup_separate() {
        let mut rng = rand::rng();

        for n in [64 * 1024, 64 * 1024 - 53, 64 * 1024 + 135] {
            let data: Vec<_> = (0..n).map(|_| rng.random_range(0..255)).collect();
            let groups = bg4_split_separate(&data);
            let regrouped = bg4_regroup_separate(&groups);

            assert_eq!(regrouped, data);
        }
    }

    #[test]
    fn test_split_regroup_together() {
        let mut rng = rand::rng();

        for n in [64 * 1024, 64 * 1024 - 53, 64 * 1024 + 135] {
            let data: Vec<_> = (0..n).map(|_| rng.random_range(0..255)).collect();
            let groups = bg4_split_together(&data);

            let regrouped = bg4_regroup_together(&groups);
            assert_eq!(regrouped, data);

            let regrouped = bg4_regroup_together_combined_write_4(&groups);
            assert_eq!(regrouped, data);

            let regrouped = bg4_regroup_together_combined_write_8(&groups);
            assert_eq!(regrouped, data);
        }
    }
}
