use hub_client::{HubClient, HubRepositoryTrait};
use nfsserve::nfs::{fattr3, fileid3, filename3, nfspath3, nfsstat3, sattr3};
use nfsserve::vfs::{NFSFileSystem, ReadDirResult, VFSCapabilities};
use std::collections::{hash_map, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

enum Item {
    Directory(Arc<Directory>),
    RegularFile(Arc<RegularFile>),
    XetFile(Arc<XetFile>),
}

impl Item {
    fn fattr3(&self) -> &fattr3 {
        match self {
            Item::Directory(dir) => &dir.fattr3,
            Item::RegularFile(file) => &file.fattr3,
            Item::XetFile(file) => &file.fattr3,
        }
    }

    fn as_directory(&self) -> Option<&Directory> {
        match self {
            Item::Directory(dir) => Some(dir),
            _ => None,
        }
    }
}

struct RegularFile {
    fattr3: fattr3,
    path: String,
}

struct XetFile {
    fattr3: fattr3,
    path: String,
}

struct Directory {
    id: fileid3,
    fattr3: fattr3,
    children: Option<HashMap<fileid3, Item>>,
    path: String,
}

struct XetFS {
    everything: RwLock<HashMap<fileid3, Item>>,
    hub_client: HubClient,
}

impl XetFS {
    pub fn new(hub_client: HubClient) -> Self {
        Self {
            everything: Default::default(),
            hub_client,
        }
    }
}

impl NFSFileSystem for XetFS {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    fn root_dir(&self) -> fileid3 {
        0
    }

    async fn lookup(&self, _dirid: fileid3, _filename: &filename3) -> Result<fileid3, nfsstat3> {
        todo!()
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        match self.everything.read().await.get(&id) {
            Some(item) => Ok(item.fattr3().clone()),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn read(&self, id: fileid3, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        todo!()
    }

    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create_exclusive(&self, _dirid: fileid3, _filename: &filename3) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn mkdir(&self, _dirid: fileid3, _dirname: &filename3) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        todo!()
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}
