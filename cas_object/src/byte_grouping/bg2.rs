pub fn bg2_split(data: &[u8]) -> Vec<u8> {
    let n = data.len();
    let split = n / 2;
    let rem = n % 2;
    let mut d = vec![0u8; n];

    unsafe {
        let data = data.as_ptr();
        let d0 = d.as_mut_ptr();
        let d1 = d0.add(split + 1.min(rem));

        for i in 0..split {
            let idx = 2 * i;
            *d0.add(i) = *data.add(idx);
            *d1.add(i) = *data.add(idx + 1);
        }

        if rem != 0 {
            *d0.add(split) = *data.add(2 * split);
        }
    }

    d
}

pub fn bg2_regroup(g: &[u8]) -> Vec<u8> {
    let n = g.len();
    let split = n / 2;
    let rem = n % 2;
    let mut data = vec![0u8; n];

    unsafe {
        let data = data.as_mut_ptr();
        let g0 = g.as_ptr();
        let g1 = g0.add(split + 1.min(rem));

        for i in 0..split {
            *data.add(2 * i) = *g0.add(i);
            *data.add(2 * i + 1) = *g1.add(i);
        }

        if rem != 0 {
            *data.add(2 * split) = *g0.add(split);
        }
    }

    data
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_split_regroup() {
        let mut rng = rand::thread_rng();

        for n in [64 * 1024, 64 * 1024 - 53, 64 * 1024 + 135] {
            let data: Vec<_> = (0..n).map(|_| rng.gen_range(0..255)).collect();
            let groups = bg2_split(&data);

            let regrouped = bg2_regroup(&groups);
            assert_eq!(regrouped, data);
        }
    }
}
