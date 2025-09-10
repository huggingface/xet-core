#[cfg(target_os = "macos")]
pub fn raise_nofile_soft_to_hard() {
    use tracing::info;

    unsafe {
        use libc;

        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            info!("Failed to get RLIMIT_NOFILE: {:?}", std::io::Error::last_os_error());
            return;
        }

        if lim.rlim_cur < lim.rlim_max {
            let new_lim = libc::rlimit {
                rlim_cur: lim.rlim_max,
                rlim_max: lim.rlim_max,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &new_lim) != 0 {
                info!(
                    "Failed to set RLIMIT_NOFILE soft limit from {} to {}: {:?}",
                    lim.rlim_cur,
                    lim.rlim_max,
                    std::io::Error::last_os_error()
                );
                return;
            }
            info!("Increased RLIMIT_NOFILE soft limit from {} to {}", lim.rlim_cur, lim.rlim_max);
        } else {
            info!("RLIMIT_NOFILE soft limit already at hard limit: {}", lim.rlim_cur);
        }
    }
}

#[cfg(not(target_os = "macos"))]
pub fn raise_nofile_soft_to_hard() {}
