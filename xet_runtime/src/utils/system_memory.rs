//! System-memory probing and memory-derived download buffer defaults.
//!
//! The download buffer configuration defaults (see `config::groups::reconstruction`) are
//! derived from the memory actually usable by this process: the minimum of the host's
//! physical RAM and the effective cgroup memory limit. Inside a container, the cgroup
//! limit is what matters; `sysinfo` (and `psutil` in Python) report the host total, which
//! is how a 1 GiB pod would otherwise size a multi-GB buffer.
//!
//! The cgroup probe walks the cgroup path listed in `/proc/self/cgroup` rather than
//! reading the controller root: under cgroup v2 the limit lives at that path (the
//! controller root has no `memory.max` at all on a host), and under cgroup v1 a nested
//! path (a Slurm step, a systemd slice) reads "unlimited" at the controller root.
//! Ancestors are also consulted, taking the tightest bound.

use std::path::Path;
use std::sync::LazyLock;

use tracing::info;

use crate::utils::ByteSize;
use crate::utils::configuration_utils::parse_bool_value;

/// Round derived values down to a multiple of this, for clean display values.
const ROUND_TO: u64 = 8_000_000;

/// cgroup v1 reports "no limit" as a very large page-aligned value (PAGE_COUNTER_MAX,
/// typically 0x7FFF_FFFF_FFFF_F000). Anything at or above this is treated as unbounded.
const CGROUP_V1_UNLIMITED: u64 = 0x7FFF_FFFF_FFFF_F000;

/// Memory available to this process, as best we can determine.
#[derive(Debug, Clone, Copy)]
pub struct UsableMemory {
    /// Total physical memory of the host (or VM), if determinable.
    pub host_total: Option<u64>,
    /// Effective cgroup memory limit (Linux only), if one is set.
    pub cgroup_limit: Option<u64>,
}

impl UsableMemory {
    /// The memory figure defaults should be derived from: the minimum of the
    /// available readings, or `None` if neither could be determined.
    pub fn usable(&self) -> Option<u64> {
        match (self.host_total, self.cgroup_limit) {
            (Some(host), Some(cgroup)) => Some(host.min(cgroup)),
            (host, cgroup) => host.or(cgroup),
        }
    }
}

/// The three download buffer knobs, derived together so they describe one allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadBufferDefaults {
    pub size: ByteSize,
    pub perfile: ByteSize,
    pub limit: ByteSize,
}

/// Static fallback defaults, used when memory cannot be determined or derivation is
/// disabled via `HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS=0`. These are the historical
/// constant defaults.
pub const STATIC_DEFAULTS: DownloadBufferDefaults = DownloadBufferDefaults {
    size: ByteSize::new(2_000_000_000),
    perfile: ByteSize::new(512_000_000),
    limit: ByteSize::new(8_000_000_000),
};

/// Static fallback for high-performance mode; the historical `HF_XET_HP` constants.
pub const STATIC_HP_DEFAULTS: DownloadBufferDefaults = DownloadBufferDefaults {
    size: ByteSize::new(16_000_000_000),
    perfile: ByteSize::new(2_000_000_000),
    limit: ByteSize::new(64_000_000_000),
};

/// Divisors applied to usable memory for each knob, with shared floors/ceilings.
struct Fractions {
    size_divisor: u64,
    perfile_divisor: u64,
    limit_divisor: u64,
}

/// Standard fractions: anchored so 32 GB of usable memory reproduces the historical
/// defaults, and >= 256 GB reproduces the historical high-performance values.
const STANDARD_FRACTIONS: Fractions = Fractions {
    size_divisor: 16,
    perfile_divisor: 64,
    limit_divisor: 4,
};

/// High-performance fractions: twice as aggressive, same floors and ceilings.
const HIGH_PERFORMANCE_FRACTIONS: Fractions = Fractions {
    size_divisor: 8,
    perfile_divisor: 32,
    limit_divisor: 2,
};

/// Floors keep tiny environments functional; ceilings are the historical
/// high-performance values. All are multiples of `ROUND_TO`, so rounding a clamped
/// value down never violates the clamp. The floors satisfy
/// `size + 8 * perfile <= limit` just like the fractions do.
const SIZE_FLOOR: u64 = 32_000_000;
const SIZE_CEILING: u64 = 16_000_000_000;
const PERFILE_FLOOR: u64 = 8_000_000;
const PERFILE_CEILING: u64 = 2_000_000_000;
const LIMIT_FLOOR: u64 = 128_000_000;
const LIMIT_CEILING: u64 = 64_000_000_000;

/// Derive the three buffer values from a usable-memory figure.
fn derive(usable: u64, fractions: &Fractions) -> DownloadBufferDefaults {
    let apply = |divisor: u64, floor: u64, ceiling: u64| -> u64 {
        let value = (usable / divisor).clamp(floor, ceiling);
        (value / ROUND_TO) * ROUND_TO
    };
    DownloadBufferDefaults {
        size: ByteSize::new(apply(fractions.size_divisor, SIZE_FLOOR, SIZE_CEILING)),
        perfile: ByteSize::new(apply(fractions.perfile_divisor, PERFILE_FLOOR, PERFILE_CEILING)),
        limit: ByteSize::new(apply(fractions.limit_divisor, LIMIT_FLOOR, LIMIT_CEILING)),
    }
}

/// Read the limit file at `base/<cg_path>` and each of its ancestors up to and
/// including `base`, feeding every bounded numeric value found to `consider`.
/// Missing files or directories are skipped: a namespaced or partially-visible
/// hierarchy degrades to reading the root, never to an error.
fn walk_limit_files(base: &Path, cg_path: &str, file_name: &str, consider: &mut impl FnMut(u64)) {
    let mut dir = base.join(cg_path.trim_matches('/'));
    loop {
        if let Ok(contents) = std::fs::read_to_string(dir.join(file_name)) {
            // Non-numeric contents (the literal "max" under cgroup v2) mean unbounded.
            if let Ok(value) = contents.trim().parse::<u64>() {
                consider(value);
            }
        }
        if dir == *base || !dir.pop() {
            break;
        }
    }
}

/// Parse `/proc/self/cgroup` contents and resolve the effective memory limit by walking
/// the named cgroup path (and its ancestors) under `cgroup_fs_root`.
///
/// Handles cgroup v2 (`0::<path>` entries, `memory.max` files, literal `max` meaning
/// unbounded), cgroup v1 (`<n>:memory:<path>` entries, `memory.limit_in_bytes` files,
/// PAGE_COUNTER_MAX meaning unbounded), and hybrid setups (minimum of both). Missing
/// files and directories are skipped, so a namespaced or partially-visible hierarchy
/// degrades to reading the root, never to an error.
pub fn cgroup_effective_memory_limit(proc_cgroup_contents: &str, cgroup_fs_root: &Path) -> Option<u64> {
    let mut effective: Option<u64> = None;
    let mut consider = |value: u64| {
        if value < CGROUP_V1_UNLIMITED {
            effective = Some(effective.map_or(value, |current: u64| current.min(value)));
        }
    };

    for line in proc_cgroup_contents.lines() {
        let mut parts = line.splitn(3, ':');
        let (Some(_hierarchy), Some(controllers), Some(cg_path)) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };

        if controllers.is_empty() {
            // cgroup v2 unified entry: "0::<path>".
            walk_limit_files(cgroup_fs_root, cg_path, "memory.max", &mut consider);
        } else if controllers.split(',').any(|c| c == "memory") {
            // cgroup v1: the memory controller hierarchy is mounted at <root>/memory.
            walk_limit_files(&cgroup_fs_root.join("memory"), cg_path, "memory.limit_in_bytes", &mut consider);
        }
    }

    effective
}

#[cfg(not(target_family = "wasm"))]
fn probe_usable_memory() -> UsableMemory {
    use sysinfo::{MemoryRefreshKind, RefreshKind, System};

    let system =
        System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()));
    let host_total = match system.total_memory() {
        0 => None,
        total => Some(total),
    };

    #[cfg(target_os = "linux")]
    let cgroup_limit = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|contents| cgroup_effective_memory_limit(&contents, Path::new("/sys/fs/cgroup")));
    #[cfg(not(target_os = "linux"))]
    let cgroup_limit = None;

    UsableMemory {
        host_total,
        cgroup_limit,
    }
}

#[cfg(target_family = "wasm")]
fn probe_usable_memory() -> UsableMemory {
    UsableMemory {
        host_total: None,
        cgroup_limit: None,
    }
}

/// Probe the system once and cache the result for the process lifetime.
pub fn usable_system_memory() -> &'static UsableMemory {
    static USABLE_MEMORY: LazyLock<UsableMemory> = LazyLock::new(probe_usable_memory);
    &USABLE_MEMORY
}

/// The derived (standard, high-performance) default pairs, computed once and logged.
fn derived_defaults() -> &'static (DownloadBufferDefaults, DownloadBufferDefaults) {
    static DERIVED: LazyLock<(DownloadBufferDefaults, DownloadBufferDefaults)> = LazyLock::new(|| {
        let memory = usable_system_memory();
        match memory.usable() {
            Some(usable) => {
                let standard = derive(usable, &STANDARD_FRACTIONS);
                let high_performance = derive(usable, &HIGH_PERFORMANCE_FRACTIONS);
                info!(
                    host_total = ?memory.host_total,
                    cgroup_limit = ?memory.cgroup_limit,
                    usable,
                    "Derived download buffer defaults from system memory: size={} perfile={} limit={}",
                    standard.size,
                    standard.perfile,
                    standard.limit
                );
                (standard, high_performance)
            },
            None => {
                info!("System memory could not be determined; using static download buffer defaults");
                (STATIC_DEFAULTS, STATIC_HP_DEFAULTS)
            },
        }
    });
    &DERIVED
}

/// Whether memory-derived defaults are enabled. Read on every call (cheap) so the
/// switch is testable in-process; only the probe and derived values are cached.
fn memory_derived_defaults_enabled() -> bool {
    match std::env::var("HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS") {
        Ok(value) => parse_bool_value(&value).unwrap_or(true),
        Err(_) => true,
    }
}

/// The memory-derived defaults for the standard configuration.
///
/// Falls back to [`STATIC_DEFAULTS`] when memory cannot be determined, on wasm, or when
/// disabled via `HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS=0`.
pub fn default_download_buffer_sizes() -> DownloadBufferDefaults {
    if memory_derived_defaults_enabled() {
        derived_defaults().0
    } else {
        STATIC_DEFAULTS
    }
}

/// The memory-derived defaults for high-performance mode (`HF_XET_HP`).
///
/// Falls back to [`STATIC_HP_DEFAULTS`] under the same conditions as
/// [`default_download_buffer_sizes`].
pub fn high_performance_download_buffer_sizes() -> DownloadBufferDefaults {
    if memory_derived_defaults_enabled() {
        derived_defaults().1
    } else {
        STATIC_HP_DEFAULTS
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn triple(d: DownloadBufferDefaults) -> (u64, u64, u64) {
        (d.size.as_u64(), d.perfile.as_u64(), d.limit.as_u64())
    }

    #[test]
    fn test_usable_memory_min() {
        let mem = |h, c| UsableMemory {
            host_total: h,
            cgroup_limit: c,
        };
        assert_eq!(mem(Some(10), Some(4)).usable(), Some(4));
        assert_eq!(mem(Some(4), Some(10)).usable(), Some(4));
        assert_eq!(mem(Some(10), None).usable(), Some(10));
        assert_eq!(mem(None, Some(4)).usable(), Some(4));
        assert_eq!(mem(None, None).usable(), None);
    }

    #[test]
    fn test_standard_derivation_table() {
        // (usable, (size, perfile, limit))
        let cases: &[(u64, (u64, u64, u64))] = &[
            (268_435_456, (32_000_000, 8_000_000, 128_000_000)), // 256 MiB: floors
            (512_000_000, (32_000_000, 8_000_000, 128_000_000)), // floor boundary
            (1_073_741_824, (64_000_000, 16_000_000, 264_000_000)), // 1 GiB container
            (2_000_000_000, (120_000_000, 24_000_000, 496_000_000)),
            (8_000_000_000, (496_000_000, 120_000_000, 2_000_000_000)),
            (16_000_000_000, (1_000_000_000, 248_000_000, 4_000_000_000)),
            (32_000_000_000, (2_000_000_000, 496_000_000, 8_000_000_000)), // ~historical defaults
            (64_000_000_000, (4_000_000_000, 1_000_000_000, 16_000_000_000)),
            (128_000_000_000, (8_000_000_000, 2_000_000_000, 32_000_000_000)),
            (256_000_000_000, (16_000_000_000, 2_000_000_000, 64_000_000_000)), // ceilings
            (2_000_000_000_000, (16_000_000_000, 2_000_000_000, 64_000_000_000)), // 2 TB host
        ];
        for &(usable, expected) in cases {
            assert_eq!(triple(derive(usable, &STANDARD_FRACTIONS)), expected, "standard, usable={usable}");
        }
        // A huge host derives exactly the historical high-performance constants.
        assert_eq!(derive(2_000_000_000_000, &STANDARD_FRACTIONS), STATIC_HP_DEFAULTS);
    }

    #[test]
    fn test_high_performance_derivation_table() {
        let cases: &[(u64, (u64, u64, u64))] = &[
            (268_435_456, (32_000_000, 8_000_000, 128_000_000)),
            (512_000_000, (64_000_000, 16_000_000, 256_000_000)),
            (8_000_000_000, (1_000_000_000, 248_000_000, 4_000_000_000)),
            (16_000_000_000, (2_000_000_000, 496_000_000, 8_000_000_000)),
            (32_000_000_000, (4_000_000_000, 1_000_000_000, 16_000_000_000)),
            (64_000_000_000, (8_000_000_000, 2_000_000_000, 32_000_000_000)),
            (128_000_000_000, (16_000_000_000, 2_000_000_000, 64_000_000_000)),
            (256_000_000_000, (16_000_000_000, 2_000_000_000, 64_000_000_000)),
            (2_000_000_000_000, (16_000_000_000, 2_000_000_000, 64_000_000_000)),
        ];
        for &(usable, expected) in cases {
            assert_eq!(triple(derive(usable, &HIGH_PERFORMANCE_FRACTIONS)), expected, "hp, usable={usable}");
        }
    }

    #[test]
    fn test_derivation_invariants_across_sweep() {
        // Sweep 256 MB .. 2.5 TB multiplicatively; check invariant, rounding,
        // clamps, and monotonicity for both fraction sets.
        for fractions in [&STANDARD_FRACTIONS, &HIGH_PERFORMANCE_FRACTIONS] {
            let mut prev: Option<(u64, u64, u64)> = None;
            let mut usable: u64 = 268_435_456;
            while usable < 2_500_000_000_000 {
                let (size, perfile, limit) = triple(derive(usable, fractions));

                // One coherent allocation: base + 8 concurrent files * perfile fits the limit.
                assert!(
                    size + 8 * perfile <= limit,
                    "invariant violated at usable={usable}: {size} + 8*{perfile} > {limit}"
                );

                // Clean display values.
                for v in [size, perfile, limit] {
                    assert_eq!(v % ROUND_TO, 0, "not a multiple of {ROUND_TO} at usable={usable}");
                }

                // Clamps respected.
                assert!((SIZE_FLOOR..=SIZE_CEILING).contains(&size));
                assert!((PERFILE_FLOOR..=PERFILE_CEILING).contains(&perfile));
                assert!((LIMIT_FLOOR..=LIMIT_CEILING).contains(&limit));

                // Monotonic in usable memory.
                if let Some((ps, pp, pl)) = prev {
                    assert!(size >= ps && perfile >= pp && limit >= pl, "not monotonic at usable={usable}");
                }
                prev = Some((size, perfile, limit));

                usable = usable * 5 / 4;
            }
        }
    }

    // --- cgroup parsing fixtures ---

    fn write_cgroup_file(root: &Path, rel: &str, file: &str, contents: &str) {
        let dir = root.join(rel);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(file), contents).unwrap();
    }

    #[test]
    fn test_cgroup_v2_nested_path() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "a/b", "memory.max", "1073741824\n");
        write_cgroup_file(root, "a", "memory.max", "max\n");
        assert_eq!(cgroup_effective_memory_limit("0::/a/b\n", root), Some(1_073_741_824));
    }

    #[test]
    fn test_cgroup_v2_min_of_ancestors() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "a", "memory.max", "536870912\n");
        write_cgroup_file(root, "a/b", "memory.max", "1073741824\n");
        assert_eq!(cgroup_effective_memory_limit("0::/a/b\n", root), Some(536_870_912));
    }

    #[test]
    fn test_cgroup_v2_container_root() {
        // In a Docker/k8s container with cgroup namespaces, the process sees "0::/".
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "", "memory.max", "2147483648\n");
        assert_eq!(cgroup_effective_memory_limit("0::/\n", root), Some(2_147_483_648));
    }

    #[test]
    fn test_cgroup_v2_unlimited() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "a", "memory.max", "max\n");
        write_cgroup_file(root, "", "memory.max", "max\n");
        assert_eq!(cgroup_effective_memory_limit("0::/a\n", root), None);
    }

    #[test]
    fn test_cgroup_v1_nested_slurm_style() {
        // Nested v1 path (e.g. a Slurm step): the limit is on the nested path while
        // the controller root reads PAGE_COUNTER_MAX ("unlimited").
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "memory/slurm/job123/step0", "memory.limit_in_bytes", "4294967296\n");
        write_cgroup_file(root, "memory", "memory.limit_in_bytes", "9223372036854771712\n");
        let contents = "12:memory:/slurm/job123/step0\n11:cpu,cpuacct:/\n";
        assert_eq!(cgroup_effective_memory_limit(contents, root), Some(4_294_967_296));
    }

    #[test]
    fn test_cgroup_v1_comounted_controllers() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "memory/x", "memory.limit_in_bytes", "1000000000\n");
        assert_eq!(cgroup_effective_memory_limit("5:memory,hugetlb:/x\n", root), Some(1_000_000_000));
    }

    #[test]
    fn test_cgroup_v1_unlimited_sentinel() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "memory", "memory.limit_in_bytes", "9223372036854771712\n");
        assert_eq!(cgroup_effective_memory_limit("12:memory:/\n", root), None);
    }

    #[test]
    fn test_cgroup_hybrid_takes_min() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "foo", "memory.max", "8000000000\n");
        write_cgroup_file(root, "memory/bar", "memory.limit_in_bytes", "4000000000\n");
        let contents = "0::/foo\n7:memory:/bar\n";
        assert_eq!(cgroup_effective_memory_limit(contents, root), Some(4_000_000_000));
    }

    #[test]
    fn test_cgroup_malformed_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        assert_eq!(cgroup_effective_memory_limit("garbage with no colons\n", root), None);
        assert_eq!(cgroup_effective_memory_limit("", root), None);
    }

    #[test]
    fn test_cgroup_missing_dirs_fall_back_to_root() {
        // A path from /proc/self/cgroup that is not visible under the mounted
        // hierarchy (mount/cgroup namespace mismatch): the walk skips missing
        // directories and still picks up a root-level limit.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_cgroup_file(root, "", "memory.max", "1000000000\n");
        assert_eq!(cgroup_effective_memory_limit("0::/nonexistent/deep/path\n", root), Some(1_000_000_000));
    }

    // --- probe and kill switch ---

    #[test]
    fn test_probe_reports_host_total() {
        let mem = usable_system_memory();
        assert!(mem.host_total.is_some_and(|t| t > 0), "host total should be detectable on test hosts");
    }

    #[test]
    #[serial_test::serial(config_env)]
    fn test_kill_switch_restores_static_defaults() {
        let _g = crate::utils::EnvVarGuard::set("HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS", "0");
        assert_eq!(default_download_buffer_sizes(), STATIC_DEFAULTS);
        assert_eq!(high_performance_download_buffer_sizes(), STATIC_HP_DEFAULTS);
    }

    #[test]
    #[serial_test::serial(config_env)]
    fn test_derived_defaults_match_probe() {
        let _g = crate::utils::EnvVarGuard::set("HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS", "1");
        let expected_std = match usable_system_memory().usable() {
            Some(u) => derive(u, &STANDARD_FRACTIONS),
            None => STATIC_DEFAULTS,
        };
        let expected_hp = match usable_system_memory().usable() {
            Some(u) => derive(u, &HIGH_PERFORMANCE_FRACTIONS),
            None => STATIC_HP_DEFAULTS,
        };
        assert_eq!(default_download_buffer_sizes(), expected_std);
        assert_eq!(high_performance_download_buffer_sizes(), expected_hp);
    }
}
