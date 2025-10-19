use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use cas_client::SequentialOutput;
use cas_types::FileRange;
use data::FileDownloader;
use hub_client::{HubClient, HubRepositoryTrait, TreeEntry};
use merklehash::MerkleHash;
use nfsserve::nfs::{fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use tokio::io::AsyncReadExt;
use tokio::sync::{OnceCell, RwLock};

#[derive(Clone)]
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

    fn fileid(&self) -> fileid3 {
        self.fattr3().fileid
    }

    fn path(&self) -> &str {
        match self {
            Item::Directory(dir) => dir.path.as_str(),
            Item::RegularFile(file) => file.path.as_str(),
            Item::XetFile(file) => file.path.as_str(),
        }
    }

    fn filename(&self) -> &[u8] {
        Path::new(self.path()).file_name().map(|s| s.as_encoded_bytes()).unwrap_or(b"/")
    }
}

struct RegularFile {
    fattr3: fattr3,
    path: String,
}

impl From<RegularFile> for Item {
    fn from(value: RegularFile) -> Self {
        Self::RegularFile(Arc::new(value))
    }
}

struct XetFile {
    fattr3: fattr3,
    path: String,
    hash: MerkleHash,
}

impl From<XetFile> for Item {
    fn from(value: XetFile) -> Self {
        Self::XetFile(Arc::new(value))
    }
}

struct Directory {
    fattr3: fattr3,
    children: OnceCell<Vec<Item>>,
    path: String,
}

impl From<Directory> for Item {
    fn from(value: Directory) -> Self {
        Self::Directory(Arc::new(value))
    }
}

pub struct XetFS {
    inner: Arc<XetFSInner>,
}

struct XetFSInner {
    everything: RwLock<HashMap<fileid3, Item>>,
    hub_client: Arc<HubClient>,
    next_id: AtomicU64,
    xet_downloader: FileDownloader,
}

const ROOT_DIR_ID: fileid3 = 0;

impl XetFS {
    pub fn new(hub_client: Arc<HubClient>, xet_downloader: FileDownloader) -> Self {
        Self {
            inner: Arc::new(XetFSInner::new(hub_client, xet_downloader)),
        }
    }
}

impl XetFSInner {
    pub fn new(hub_client: Arc<HubClient>, xet_downloader: FileDownloader) -> Self {
        let mut everything = HashMap::new();
        let root_attr = fattr3 {
            ftype: ftype3::NF3DIR,
            mode: 0o755,
            nlink: 1,
            uid: 0,
            gid: 0,
            size: 0,
            used: 0,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: ROOT_DIR_ID,
            atime: nfstime3::default(),
            mtime: nfstime3::default(),
            ctime: nfstime3::default(),
        };
        let root = Directory {
            fattr3: root_attr,
            children: OnceCell::new(),
            path: String::new(),
        };
        everything.insert(ROOT_DIR_ID, Item::Directory(Arc::new(root)));
        Self {
            everything: RwLock::new(everything),
            hub_client,
            xet_downloader,
            next_id: AtomicU64::new(ROOT_DIR_ID + 1),
        }
    }

    fn get_next_id(&self) -> fileid3 {
        self.next_id.fetch_add(1, Ordering::AcqRel)
    }

    // should be called in try_init for a children field
    async fn get_children_for_path(&self, path: &str) -> Result<Vec<Item>, nfsstat3> {
        let entries = self.hub_client.list_files(path).await.map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // Build the children map
        let mut children: Vec<Item> = Vec::with_capacity(entries.len());
        let mut everything_guard = self.everything.write().await;
        for entry in entries {
            let fileid = self.get_next_id();
            let item: Item = match entry {
                TreeEntry::File(file_entry) => {
                    let attr = get_fattr3(fileid, ftype3::NF3REG);
                    // Decide RegularFile vs XetFile based on xet_hash presence
                    if let Some(xet_hash) = file_entry.xet_hash {
                        XetFile {
                            fattr3: attr,
                            path: file_entry.path.clone(),
                            hash: xet_hash.into(),
                        }
                        .into()
                    } else {
                        RegularFile {
                            fattr3: attr,
                            path: file_entry.path.clone(),
                        }
                        .into()
                    }
                },
                TreeEntry::Directory(dirent) => {
                    let attr = get_fattr3(fileid, ftype3::NF3DIR);
                    Directory {
                        fattr3: attr,
                        children: OnceCell::new(),
                        path: dirent.path.clone(),
                    }
                    .into()
                },
            };
            children.push(item.clone());
            everything_guard.insert(fileid, item);
        }
        Ok(children)
    }

    async fn get_dir(&self, dirid: fileid3) -> Result<Arc<Directory>, nfsstat3> {
        // Fetch the directory item
        let maybe_item = { self.everything.read().await.get(&dirid).cloned() };
        match maybe_item {
            Some(Item::Directory(dir)) => Ok(dir),
            Some(_) => Err(nfsstat3::NFS3ERR_NOTDIR),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    async fn download_regular_file(
        &self,
        file: Arc<RegularFile>,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let file_len = file.fattr3.size;
        let past_the_end = offset + count as u64 > file_len;

        let data = self
            .hub_client
            .download_resolved_content(&file.path, Some(FileRange::new(offset, offset + count as u64)))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .to_vec();

        Ok((data, past_the_end))
    }

    async fn download_xet_file(
        &self,
        file: Arc<XetFile>,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let file_len = file.fattr3.size;
        let past_the_end = offset + count as u64 > file_len;

        let (w, s) = utils::pipe::pipe(10);
        let sequential_output: SequentialOutput = Box::new(w);

        let downloader = self.xet_downloader.clone();
        let hash = file.hash;
        let jh = tokio::spawn(async move {
            downloader
                .smudge_file_from_hash_sequential(
                    &hash,
                    file.path.clone().into(),
                    sequential_output,
                    Some(FileRange::new(offset, offset + count as u64)),
                    None,
                )
                .await
        });
        let mut res = Vec::with_capacity(1024.min(count as usize));
        s.reader().read_to_end(&mut res).await.map_err(|_| nfsstat3::NFS3ERR_IO)?;
        // this should be instantaneous
        jh.await.map_err(|_| nfsstat3::NFS3ERR_IO)?.map_err(|_| nfsstat3::NFS3ERR_IO)?;

        Ok((res, past_the_end))
    }
}

fn get_fattr3(fileid: fileid3, ftype: ftype3) -> fattr3 {
    fattr3 {
        ftype,
        mode: 0o755,
        nlink: 1,
        uid: 0,
        gid: 0,
        size: 0,
        used: 0,
        rdev: specdata3::default(),
        fsid: 0,
        fileid,
        atime: nfstime3::default(),
        mtime: nfstime3::default(),
        ctime: nfstime3::default(),
    }
}

#[async_trait::async_trait]
impl NFSFileSystem for XetFS {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    fn root_dir(&self) -> fileid3 {
        ROOT_DIR_ID
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let dir = self.inner.get_dir(dirid).await?;
        let children = dir
            .children
            .get_or_try_init(|| self.inner.get_children_for_path(dir.path.as_str()))
            .await?;
        for child in children {
            if child.filename() == filename.0 {
                return Ok(child.fileid());
            }
        }
        Err(nfsstat3::NFS3ERR_NOENT)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        match self.inner.everything.read().await.get(&id) {
            Some(item) => Ok(*item.fattr3()),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn read(&self, id: fileid3, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        println!("read: id: {:?}, offset: {:?}, count: {:?}", id, offset, count);
        let Some(item) = self.inner.everything.read().await.get(&id).cloned() else {
            return Err(nfsstat3::NFS3ERR_NOENT);
        };
        match item {
            Item::Directory(_) => Err(nfsstat3::NFS3ERR_ISDIR),
            Item::RegularFile(file) => self.inner.download_regular_file(file, offset, count).await,
            Item::XetFile(file) => self.inner.download_xet_file(file, offset, count).await,
        }
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
        println!("readdir: dirid: {:?}, start_after: {:?}, max_entries: {:?}", dirid, start_after, max_entries);
        // Fetch the directory item
        let maybe_item = { self.inner.everything.read().await.get(&dirid).cloned() };
        let dir_item = match maybe_item {
            Some(Item::Directory(dir)) => dir,
            Some(_) => return Err(nfsstat3::NFS3ERR_NOTDIR),
            None => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        // If children not cached, load from hub and populate
        let children = dir_item
            .children
            .get_or_try_init(|| self.inner.get_children_for_path(dir_item.path.as_str()))
            .await?;

        let mut entries = vec![];
        let mut skipped = 0;
        // since the children list is sorted, we should binary search over this list
        // to find how many to skip
        for child in children {
            if child.fileid() <= start_after {
                skipped += 1;
                continue;
            }
            entries.push(DirEntry {
                fileid: child.fileid(),
                name: child.filename().into(),
                attr: *child.fattr3(),
            });
            debug_assert!(entries.len() <= max_entries);
            if entries.len() == max_entries {
                break;
            }
        }
        let end = skipped + entries.len() == children.len();
        let result = ReadDirResult { entries, end };
        Ok(result)
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
