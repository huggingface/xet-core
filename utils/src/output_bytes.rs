/// Convert a usize into an output string, chooosing the nearest byte prefix.
///
/// # Arguments
/// * `v` - the size in bytes
pub fn output_bytes(v: u64) -> String {
    let map = vec![
        (1_099_511_627_776, "TiB"),
        (1_073_741_824, "GiB"),
        (1_048_576, "MiB"),
        (1024, "KiB"),
    ];

    if v == 0 {
        return "0 bytes".to_string();
    }

    for (div, s) in map {
        let curr = v as f64 / div as f64;
        if v / div > 0 {
            return if v % div == 0 {
                format!("{} {}", v / div, s)
            } else {
                format!("{curr:.2} {s}")
            };
        }
    }

    format!("{v} bytes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_conversion() {
        assert_eq!("500 bytes", output_bytes(500));
        assert_eq!("999 bytes", output_bytes(999));
        assert_eq!("1 KiB", output_bytes(1024));
        assert_eq!("1.00 KiB", output_bytes(1025));
        assert_eq!("999.99 KiB", output_bytes(1_023_989));
        assert_eq!("1 MiB", output_bytes(1_048_576));
        assert_eq!("1.00 MiB", output_bytes(1048577));
        assert_eq!("999.99 MiB", output_bytes(1_048_565_514));
        assert_eq!("1 GiB", output_bytes(1_073_741_824));
        assert_eq!("1.00 GiB", output_bytes(1_073_741_825));
        assert_eq!("999.99 GiB", output_bytes(1_073_731_086_581));
        assert_eq!("1 TiB", output_bytes(1_099_511_627_776));
        assert_eq!("1.00 TiB", output_bytes(1_099_511_627_777));
        assert_eq!("1234.57 TiB", output_bytes(1_357_424_070_303_416));
    }
}
