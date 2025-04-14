#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(target_os = "linux")]
mod linux {
    use std::path::Path;

    use nix::libc;
    use nix::sys::statfs;

    // code/s not in libc
    const LUSTRE_SUPER_MAGIC: libc::__fsword_t = 0x0BD00BD0;

    pub fn is_network_fs(path: impl AsRef<Path>) -> anyhow::Result<bool> {
        let fs_type = statfs::statfs(path.as_ref())?.filesystem_type();

        let ret = matches!(
            fs_type.0,
            libc::NFS_SUPER_MAGIC
                | libc::SMB_SUPER_MAGIC
                | libc::FUSE_SUPER_MAGIC
                | libc::AFS_SUPER_MAGIC
                | libc::CODA_SUPER_MAGIC
                | LUSTRE_SUPER_MAGIC
        );
        Ok(ret)
    }
}

#[cfg(not(target_os = "linux"))]
pub use not_linux::*;

#[cfg(not(target_os = "linux"))]
mod not_linux {
    pub fn is_network_fs(_path: impl AsRef<std::path::Path>) -> anyhow::Result<bool> {
        Ok(false)
    }
}
