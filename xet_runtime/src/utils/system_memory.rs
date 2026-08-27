//! System-memory probing and memory-derived download buffer defaults.
//!
//! The download buffer configuration defaults (see `config::groups::reconstruction`) are
//! derived from the memory actually usable by this process: the minimum of the host's
//! physical RAM and the effective cgroup memory limit. Inside a container, the cgroup
//! limit is what matters; `System::total_memory` (and `psutil` in Python) report the
//! host total, which is how a 1 GiB pod would otherwise size a multi-GB buffer.
//!
//! The cgroup limit comes from sysinfo's `Process::cgroup_limits` (sysinfo >= 0.39),
//! which resolves the cgroup path listed in `/proc/self/cgroup` rather than reading the
//! controller root, and takes the tightest bound across ancestor cgroups. That covers
//! cgroup v2 and v1, nested paths (a Slurm step, a systemd slice), and namespaced
//! container roots.

use std::sync::LazyLock;

use tracing::info;

use crate::utils::ByteSize;
use crate::utils::configuration_utils::parse_bool_value;

/// Round derived values down to a multiple of this, for clean display values.
const ROUND_TO: u64 = 8_000_000;

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
/// These values also serve as the derivation ceilings below.
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

/// Floors are the values derived for 1 GiB of usable memory, the smallest environment
/// the derivation distinguishes; anything smaller receives the same values. At the
/// floors a single maximum-size term (one unpacked xorb block, <= 64 MiB) fits the
/// budget once one file download is active (size + perfile = 80 MB), and the floors
/// satisfy `size + 8 * perfile <= limit` just like the fractions do.
///
/// Ceilings are the historical high-performance values. All floors and ceilings are
/// multiples of `ROUND_TO`, so rounding a clamped value down never violates the clamp.
const SIZE_FLOOR: u64 = 64_000_000;
const SIZE_CEILING: u64 = STATIC_HP_DEFAULTS.size.as_u64();
const PERFILE_FLOOR: u64 = 16_000_000;
const PERFILE_CEILING: u64 = STATIC_HP_DEFAULTS.perfile.as_u64();
const LIMIT_FLOOR: u64 = 264_000_000;
const LIMIT_CEILING: u64 = STATIC_HP_DEFAULTS.limit.as_u64();

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

#[cfg(not(target_family = "wasm"))]
fn probe_usable_memory() -> UsableMemory {
    use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

    let mut system =
        System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()));
    let host_total = match system.total_memory() {
        0 => None,
        total => Some(total),
    };

    // `Process::cgroup_limits` resolves this process's cgroup path from /proc and takes
    // the tightest memory limit across ancestor cgroups (v1 and v2). It returns None on
    // non-Linux systems. When no limit binds, its total_memory equals the host total, so
    // filter that case out to keep `cgroup_limit = None` meaning "no limit set".
    let cgroup_limit = sysinfo::get_current_pid().ok().and_then(|pid| {
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, ProcessRefreshKind::nothing());
        let limits = system.process(pid)?.cgroup_limits()?;
        Some(limits.total_memory).filter(|&limit| host_total.is_none_or(|host| limit < host))
    });

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

/// The system memory reading, probed once and cached for the process lifetime.
///
/// Cached because the `config_group!` default expressions run on every config
/// construction, and so that every consumer derives from a single reading even if
/// cgroup limits are edited at runtime.
pub static USABLE_MEMORY: LazyLock<UsableMemory> = LazyLock::new(probe_usable_memory);

/// The standard and high-performance default sets, derived together from one
/// [`USABLE_MEMORY`] reading.
struct DerivedDefaults {
    standard: DownloadBufferDefaults,
    high_performance: DownloadBufferDefaults,
}

/// Computed once and logged on first use.
static DERIVED_DEFAULTS: LazyLock<DerivedDefaults> = LazyLock::new(|| {
    let memory = &*USABLE_MEMORY;
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
            DerivedDefaults {
                standard,
                high_performance,
            }
        },
        None => {
            info!("System memory could not be determined; using static download buffer defaults");
            DerivedDefaults {
                standard: STATIC_DEFAULTS,
                high_performance: STATIC_HP_DEFAULTS,
            }
        },
    }
});

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
/// disabled via `HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS=0`. A function rather than a
/// static because the kill switch is consulted on every call.
pub fn default_download_buffer_sizes() -> DownloadBufferDefaults {
    if memory_derived_defaults_enabled() {
        DERIVED_DEFAULTS.standard
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
        DERIVED_DEFAULTS.high_performance
    } else {
        STATIC_HP_DEFAULTS
    }
}

#[cfg(test)]
mod tests {
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
            (268_435_456, (64_000_000, 16_000_000, 264_000_000)), // 256 MiB: floors
            (512_000_000, (64_000_000, 16_000_000, 264_000_000)), // still floors
            (1_073_741_824, (64_000_000, 16_000_000, 264_000_000)), // 1 GiB container ~= floors
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
            (268_435_456, (64_000_000, 16_000_000, 264_000_000)), // floors
            (512_000_000, (64_000_000, 16_000_000, 264_000_000)), // limit floor binds (u/2 = 256 MB)
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

    // --- probe and kill switch ---

    #[test]
    fn test_probe_reports_host_total() {
        assert!(USABLE_MEMORY.host_total.is_some_and(|t| t > 0), "host total should be detectable on test hosts");
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
        let expected_std = match USABLE_MEMORY.usable() {
            Some(u) => derive(u, &STANDARD_FRACTIONS),
            None => STATIC_DEFAULTS,
        };
        let expected_hp = match USABLE_MEMORY.usable() {
            Some(u) => derive(u, &HIGH_PERFORMANCE_FRACTIONS),
            None => STATIC_HP_DEFAULTS,
        };
        assert_eq!(default_download_buffer_sizes(), expected_std);
        assert_eq!(high_performance_download_buffer_sizes(), expected_hp);
    }
}
