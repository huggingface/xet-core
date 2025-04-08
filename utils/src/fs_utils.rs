#[cfg(target_os = "linux")]
pub use unix::*;

#[cfg(target_os = "linux")]
mod unix {
    use nix::libc;
    use nix::sys::statfs;
    use std::path::Path;

    fn determine_network_fs(path: impl AsRef<Path>) -> anyhow::Result<bool> {
        let fs_type = statfs::statfs(path.as_ref())?.filesystem_type();

        Ok(matches_network_fs(fs_type.0))
    }

    fn matches_network_fs(fs_type: libc::__fsword_t) -> bool {
        const LUSTRE_SUPER_MAGIC: libc::__fsword_t = 0x0BD00BD0;

        matches!(
            fs_type,
            libc::NFS_SUPER_MAGIC
                | libc::NFS_SUPER_MAGIC
                | libc::SMB_SUPER_MAGIC
                | libc::AFS_SUPER_MAGIC
                | libc::CODA_SUPER_MAGIC
                | LUSTRE_SUPER_MAGIC
        )
    }
}
