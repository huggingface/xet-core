use std::fmt;
use std::num::ParseFloatError;
use std::ops::Deref;
use std::str::FromStr;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ByteSize(u64);

impl ByteSize {
    pub const fn new(b: u64) -> Self {
        ByteSize(b)
    }
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Deref for ByteSize {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for ByteSize {
    fn from(v: u64) -> Self {
        ByteSize(v)
    }
}
impl From<ByteSize> for u64 {
    fn from(v: ByteSize) -> u64 {
        v.0
    }
}

// Implement this only for static strings because of the unwrap; it's nice to write
// "1gb".into() when specifying defaults for this value, but we want users outside of
// this to use the from_str method with proper error checking.
impl From<&'static str> for ByteSize {
    fn from(v: &'static str) -> Self {
        ByteSize::from_str(v).expect("Poorly formed constant ByteSize value.")
    }
}

impl FromStr for ByteSize {
    type Err = ParseFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        // Known suffixes (longest first so we don't cut "MiB" as "B")
        const SUFFIXES: &[(&str, u64)] = &[
            ("pib", 1024u64.pow(5)),
            ("tib", 1024u64.pow(4)),
            ("gib", 1024u64.pow(3)),
            ("mib", 1024u64.pow(2)),
            ("kib", 1024u64),
            ("pb", 1000u64.pow(5)),
            ("tb", 1000u64.pow(4)),
            ("gb", 1000u64.pow(3)),
            ("mb", 1000u64.pow(2)),
            ("kb", 1000),
            ("b", 1),
            ("", 1),
        ];

        let lower = s.to_ascii_lowercase();

        // Find the longest matching suffix
        let (num_str, mult) = SUFFIXES
            .iter()
            .find_map(|&(suf, m)| lower.strip_suffix(suf).map(|num| (num, m)))
            .unwrap_or((s, 1));

        // Trim whitespace and parse as float
        let num_str = num_str.trim();
        let n: f64 = num_str.parse()?;

        // Round to nearest u64
        let val = (n * (mult as f64)).round();
        Ok(ByteSize(val as u64))
    }
}

fn fmt_si(bytes: u64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    const UNITS: &[(&str, f64)] = &[
        ("PB", 1_000_000_000_000_000.0),
        ("TB", 1_000_000_000_000.0),
        ("GB", 1_000_000_000.0),
        ("MB", 1_000_000.0),
        ("kB", 1_000.0),
        ("B", 1.0),
    ];
    let b = bytes as f64;
    for (u, m) in UNITS {
        if b >= *m {
            let v = b / *m;
            if *m == 1.0 || (v - v.trunc()).abs() < 1e-9 {
                return write!(f, "{}{}", v as u64, u);
            } else {
                return write!(f, "{:.3}{}", v, u);
            }
        }
    }
    write!(f, "0B")
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_si(self.0, f)
    }
}

impl fmt::Debug for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_si(self.0, f)
    }
}

#[cfg(test)]
mod tests {

    use super::ByteSize;

    #[test]
    fn parse_case_insensitive_suffixes() {
        assert_eq!("1kb".parse::<ByteSize>().unwrap().as_u64(), 1000);
        assert_eq!("1KB".parse::<ByteSize>().unwrap().as_u64(), 1000);
        assert_eq!("1Kb".parse::<ByteSize>().unwrap().as_u64(), 1000);

        assert_eq!("1MiB".parse::<ByteSize>().unwrap().as_u64(), 1024 * 1024);
        assert_eq!("1mib".parse::<ByteSize>().unwrap().as_u64(), 1024 * 1024);
    }

    #[test]
    fn parse_floats_and_round() {
        assert_eq!("1.5kB".parse::<ByteSize>().unwrap().as_u64(), 1500);
        assert_eq!("2.5KiB".parse::<ByteSize>().unwrap().as_u64(), 2560);
        assert_eq!("0.4MB".parse::<ByteSize>().unwrap().as_u64(), 400_000);
        assert_eq!("0.4MiB".parse::<ByteSize>().unwrap().as_u64(), (0.4f64 * 1024.0 * 1024.0).round() as u64);
    }

    #[test]
    fn parse_plain_numbers() {
        assert_eq!("42".parse::<ByteSize>().unwrap().as_u64(), 42);
        assert_eq!("42B".parse::<ByteSize>().unwrap().as_u64(), 42);
    }

    #[test]
    fn display_and_debug_in_si() {
        let a = ByteSize::new(999);
        assert_eq!(format!("{}", a), "999B");
        assert_eq!(format!("{:?}", a), "999B");

        let b = ByteSize::new(1_000);
        assert_eq!(format!("{}", b), "1kB");
        assert_eq!(format!("{:?}", b), "1kB");

        let c = ByteSize::new(1_500);
        assert_eq!(format!("{}", c), "1.500kB");
        assert_eq!(format!("{:?}", c), "1.500kB");

        let d = ByteSize::new(1_000_000);
        assert_eq!(format!("{}", d), "1MB");
    }
}
