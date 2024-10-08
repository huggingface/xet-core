use std::fs;
use std::fs::{remove_file, DirEntry, File};
use std::io::ErrorKind;
use std::io::Write;
use std::ops::Range;
#[cfg(unix)]
use std::os::unix::fs::{FileExt, MetadataExt};
#[cfg(windows)]
use std::os::windows::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::str;

use byteorder::LittleEndian;
use tracing::{debug, info, warn};

use crate::disk::cache::EvictAction;
use crate::disk::size_bound::CacheValue;
use crate::metrics::{BLOCKS_STORED, BYTES_STORED, NAME_DISK_CACHE};
use crate::CacheError::{HeaderError, IOError};
use crate::{util, CacheError};

/// The DiskManager maintains the storage of blocks on disk, including how they're
/// laid out on disk, their format, and how to read/write/delete them.
///
/// # Disk Cache storage
/// Currently, we store each block as a file on disk under a singular root directory.
/// There are no plans currently to have support for multiple root directories or
/// any tiered storage (e.g. certain blocks are stored on NVMe vs SSD vs HDD).
///
/// The contents of the file are the block's contents (i.e. no added metadata).
///
/// The filename contains the following information separated by the `.` character:
/// - a base64 encoded identifier for the block (URL_SAFE config)
/// - the version of the block
///
/// TODO: Have a more robust format that isn't as easily hackable.
/// TODO: Implement checksums on block data.
#[derive(Debug)]
pub struct DiskManager {
    root_dir: PathBuf,
}

impl DiskManager {
    pub fn new(root_dir: PathBuf) -> Self {
        DiskManager { root_dir }
    }

    pub fn get_root_dir(&self) -> &PathBuf {
        &self.root_dir
    }

    /// Initializes management of the disk storage system. If things are ok, then
    /// an iterator of the cache elements found on disk are returned as an iterator.
    ///
    /// Initialization involves loading the root directory (or creating it if it
    /// doesn't exist) and reading through the root directory to get all of the
    /// cache values from it, returning those as an iterator for the caller to use.
    pub fn init(&self) -> Result<Box<dyn Iterator<Item = CacheValue>>, CacheError> {
        self.initialize_root_dir()?;

        let files = fs::read_dir(self.root_dir.as_path())?;
        Ok(Box::new(
            files
                .scan((), |_, f| f.ok())
                .filter_map(Self::try_load_entry)
                .inspect(|v| {
                    observe_data_added(v.size);
                }),
        ))
    }

    /// Tries to load the indicated directory entry as a cache file. If this entry cannot
    /// be loaded as a cache file, then we will log this and will remove the file.
    ///
    /// Note: we don't throw an error because this isn't a problem to surface further up.
    fn try_load_entry(entry: DirEntry) -> Option<CacheValue> {
        let path = entry.path();
        match to_cache_value(entry) {
            Ok(v) => Some(v),
            Err(e) => {
                info!("Entry in cache dir couldn't be read: {}", e);
                remove_file(path.clone())
                    .map_err(|e2| info!("Couldn't delete bad cache entry: {:?}, err: {}", path, e2))
                    .ok();
                None
            }
        }
    }

    pub async fn write(&self, item: &CacheValue, val: &[u8]) -> Result<(), CacheError> {
        let path = self.to_filepath(item);
        let size = val.len() as u64;

        let tempfile = tempfile::Builder::new().tempfile_in(&self.root_dir)?;
        let mut f = tempfile.as_file();
        let header = Header {
            block_size: item.block_size,
            block_idx: item.block_idx,
            key: item.key.clone(),
        };
        header.write_to(&mut f)?;
        f.write_all(val)
            .map_err(IOError)
            .map(|_| observe_data_added(size))?;

        let _ = tempfile.persist(path).map_err(|p| {
            warn!("Failed to persist {:?}", p.error);
            IOError(p.error)
        })?;

        Ok(())
    }

    #[cfg(unix)]
    fn read_impl(f: &mut File, buf: &mut [u8], start: u64) -> Result<(), CacheError> {
        f.read_exact_at(buf, start)?;
        Ok(())
    }

    #[cfg(windows)]
    fn read_impl(f: &mut File, buf: &mut Vec<u8>, start: u64) -> Result<(), CacheError> {
        let num_read = f.seek_read(buf, start)?;
        // replicate behavior of unix read_exact_at
        if num_read != buf.len() {
            return Err(IOError(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            )));
        }
        Ok(())
    }

    pub async fn read(&self, item: &CacheValue, range: Range<u64>) -> Result<Vec<u8>, CacheError> {
        let path = self.to_filepath(item);
        let mut f = File::open(path)?;
        let mut buf = vec![0u8; (range.end - range.start) as usize];
        let header = Header::read_from(&mut f)?;
        let start_off = header.get_header_len() + range.start;
        Self::read_impl(&mut f, &mut buf, start_off)?;
        Ok(buf)
    }

    pub fn remove(&self, item: &CacheValue) -> Result<(), CacheError> {
        let path = self.to_filepath(item);
        let size = item.size;
        remove_file(path)
            .map_err(IOError)
            .map(|_| observe_data_removed(size))
    }

    /// Checks that self.root_dir points to a directory on the local filesystem,
    /// creating it if it doesn't already exist.
    fn initialize_root_dir(&self) -> Result<(), CacheError> {
        let root = self.root_dir.clone();
        match root.metadata() {
            Ok(metadata) => {
                if !metadata.is_dir() || metadata.permissions().readonly() {
                    return Err(CacheError::CacheNotWritableDirectory);
                }
                metadata
            }
            Err(e) => {
                return match e.kind() {
                    ErrorKind::NotFound => {
                        info!("Cache dir doesn't exist. Creating...");
                        fs::create_dir_all(root).map_err(IOError)
                    }
                    _ => Err(IOError(e)),
                }
            }
        };
        Ok(())
    }

    fn to_filepath(&self, v: &CacheValue) -> PathBuf {
        let filename = to_filename(v.key.as_str());
        self.root_dir.join(Path::new(filename.as_str()))
    }
}

fn observe_data_added(size: u64) {
    BLOCKS_STORED.with_label_values(&[NAME_DISK_CACHE]).inc();
    BYTES_STORED
        .with_label_values(&[NAME_DISK_CACHE])
        .add(size as i64);
}

fn observe_data_removed(size: u64) {
    BLOCKS_STORED.with_label_values(&[NAME_DISK_CACHE]).dec();
    BYTES_STORED
        .with_label_values(&[NAME_DISK_CACHE])
        .sub(size as i64);
}

impl EvictAction for DiskManager {
    fn evict(&self, block: &CacheValue) -> Result<(), CacheError> {
        self.remove(block)
    }
}

fn to_filename(block_id: &str) -> String {
    base64::encode_config(block_id.as_bytes(), base64::URL_SAFE)
}

/// parses the filename into its "key" and "version" parts.
fn parse_filename(filename: &str) -> Option<String> {
    let key = base64::decode_config(filename, base64::URL_SAFE).ok()?;
    let block_id = str::from_utf8(key.as_slice()).ok()?;
    Some(block_id.to_string())
}

#[cfg(unix)]
fn metadata_size(metadata: &fs::Metadata) -> u64 {
    metadata.size()
}

#[cfg(windows)]
fn metadata_size(metadata: &fs::Metadata) -> u64 {
    metadata.file_size()
}

/// Pulls out all the information we need from the directory entry to
/// track it in the cache.
fn to_cache_value(entry: DirEntry) -> Result<CacheValue, String> {
    let filename = entry
        .file_name()
        .into_string()
        .map_err(|_| format!("({entry:?}) name isn't convertable to String"))?;
    let metadata = entry
        .metadata()
        .map_err(|e| format!("{filename} metadata couldn't be fetched: {e:?}"))?;

    if !metadata.is_file() {
        return Err(format!("{filename} is not a file"));
    }
    let key = parse_filename(filename.as_str())
        .ok_or_else(|| format!("{filename} doesn't follow naming convention"))?;

    let header = if let Some(h) = Header::attempt_from_key(&key) {
        #[cfg(debug_assertions)]
        {
            let alt_h =
                verify_header(entry).map_err(|e| format!("{filename} invalid header: {e:?}"))?;
            assert_eq!(alt_h, h);
        }
        h
    } else {
        debug!("Warning: Parsing header from filename for {filename} failed; loading from file.");
        verify_header(entry).map_err(|e| format!("{filename} invalid header: {e:?}"))?
    };

    let file_size = metadata_size(&metadata) - header.get_header_len();

    Ok(CacheValue {
        size: file_size,
        version: 1,
        block_size: header.block_size,
        block_idx: header.block_idx,
        key,
        insertion_time_ms: metadata
            .modified()
            .ok()
            .map_or(0, util::time_to_epoch_millis),
    })
}

fn verify_header(entry: DirEntry) -> Result<Header, CacheError> {
    let mut f = File::open(entry.path())?;
    Header::read_from(&mut f)
}

const HEADER_MAGIC: [u8; 8] = [88, 69, 84, 67, 65, 67, 72, 69]; // XETCACHE
const FORMAT_VERSION: u8 = 1u8;
const HEADER_FIXED_SIZE: u64 = 8 + 1 + 8 + 8 + 4;

/// The header for a cache file consists of the following pieces:
/// u64: HEADER_MAGIC
/// u8: FORMAT_VERSION
/// u64: block size in bytes
/// u64: block index
/// u32: length of key string
/// <len>: the key for the block
///
/// All numbers are encoded in LittleEndian encoding
#[derive(Debug, Default, PartialEq, Eq)]
struct Header {
    block_size: u64,
    block_idx: u64,
    key: String,
}

impl Header {
    /// Serialize the Header into a byte Vec:
    /// [HEADER_MAGIC, FORMAT_VERSION, BLOCK_SIZE, BLOCK_IDX, LEN(NAME), NAME]
    fn write_to<T: byteorder::WriteBytesExt>(&self, file: &mut T) -> Result<(), CacheError> {
        file.write_all(&HEADER_MAGIC)?;
        file.write_u8(FORMAT_VERSION)?;
        file.write_u64::<LittleEndian>(self.block_size)?;
        file.write_u64::<LittleEndian>(self.block_idx)?;

        let name_bytes = self.key.as_bytes();
        file.write_u32::<LittleEndian>(name_bytes.len() as u32)?;
        file.write_all(name_bytes)?;
        Ok(())
    }

    fn read_from<T: byteorder::ReadBytesExt>(file: &mut T) -> Result<Self, CacheError> {
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if magic != HEADER_MAGIC {
            return Err(HeaderError(format!(
                "magic number: {magic:?} doesn't match: {HEADER_MAGIC:?}"
            )));
        }
        let version = file.read_u8()?;
        match version {
            1 => Self::parse_header_v1(file),
            _ => Err(HeaderError(format!("version: {version} unsupported"))),
        }
    }

    fn parse_header_v1<T: byteorder::ReadBytesExt>(file: &mut T) -> Result<Self, CacheError> {
        let block_size = file.read_u64::<LittleEndian>()?;
        let block_idx = file.read_u64::<LittleEndian>()?;
        let name_len = file.read_u32::<LittleEndian>()? as usize;
        let mut name_arr = vec![0; name_len];
        file.read_exact(&mut name_arr)?;
        let key = String::from_utf8(name_arr)
            .map_err(|e| HeaderError(format!("failed to parse key as UTF8: {e:?}")))?;
        Ok(Self {
            block_size,
            block_idx,
            key,
        })
    }

    fn get_header_len(&self) -> u64 {
        return HEADER_FIXED_SIZE + self.key.as_bytes().len() as u64;
    }

    fn attempt_from_key(key: &str) -> Option<Self> {
        // This assumes the request_to_key function, which dictates the key value and the filename, uses
        // the following format:
        // format!(
        //    "{name}.{block_idx}.{block_size}",
        // )
        // See the request_to_key function in cache.rs

        let last_dot = key.rfind('.')?;
        let second_last_dot = key[..last_dot].rfind('.')?;

        let _name = &key[..second_last_dot];
        let block_idx = key[second_last_dot + 1..last_dot].parse::<u64>().ok()?;
        let block_size = key[last_dot + 1..].parse::<u64>().ok()?;

        Some(Self {
            key: key.to_owned(),
            block_idx,
            block_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::util::test_utils::CacheDirTest;

    use super::*;

    #[test]
    fn test_name_conversion() {
        let block_id = "file.0";
        let filename = to_filename(block_id);
        assert_eq!(filename, "ZmlsZS4w");
        let parsed_block_id = parse_filename(filename.as_str()).unwrap();
        assert_eq!(parsed_block_id, block_id.to_string());
    }

    #[test]
    fn test_name_conversion_invalid_format() {
        let filename_extra_components = "ZmlsZS4w.1";
        assert!(parse_filename(filename_extra_components).is_none());

        let filename_id_not_base64 = "a/b/c_txt";
        assert!(parse_filename(filename_id_not_base64).is_none());
    }

    #[tokio::test]
    async fn test_manager_write() {
        let dir = CacheDirTest::new("write");
        let m = DiskManager::new(dir.get_path().to_path_buf());
        let key = CacheValue::new(5, 0, "abc".to_string(), 1024, 0);
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        m.write(&key, data.as_slice()).await.unwrap();
        let entries = dir.get_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].file_name().into_string().unwrap().as_str(),
            "YWJj"
        );
    }

    #[tokio::test]
    async fn test_manager_write_read() {
        let dir = CacheDirTest::new("write_read");
        let m = DiskManager::new(dir.get_path().to_path_buf());
        let key = CacheValue::new(5, 0, "abc".to_string(), 1024, 0);
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        m.write(&key, data.as_slice()).await.unwrap();

        let data_read = m.read(&key, 0..5).await.unwrap();
        assert_eq!(data, data_read);
    }

    #[tokio::test]
    async fn test_manager_read_not_found() {
        let dir = CacheDirTest::new("read_not_found");
        let m = DiskManager::new(dir.get_path().to_path_buf());
        let key = CacheValue::new(5, 0, "abc".to_string(), 1024, 0);
        let res = m.read(&key, 0..5).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_manager_remove() {
        let dir = CacheDirTest::new("write_remove");
        let m = DiskManager::new(dir.get_path().to_path_buf());
        let key = CacheValue::new(5, 0, "abc".to_string(), 1024, 0);
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        m.write(&key, data.as_slice()).await.unwrap();

        m.remove(&key).unwrap();

        let entries = dir.get_entries();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_manager_load() {
        let dir = CacheDirTest::new("load");
        let m = DiskManager::new(dir.get_path().to_path_buf());
        let mut key = CacheValue::new(5, 0, "abc".to_string(), 1024, 0);
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        m.write(&key, data.as_slice()).await.unwrap();
        key.key = "bcd".to_string();
        m.write(&key, data.as_slice()).await.unwrap();

        // check that a new DiskManager will load in the 2 cached files.
        let m2 = DiskManager::new(dir.get_path().to_path_buf());
        let mut vals: Vec<CacheValue> = m2.init().unwrap().collect();
        assert_eq!(vals.len(), 2);
        vals.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(vals[0].key.as_str(), "abc");
        assert_eq!(vals[1].key.as_str(), "bcd");
    }

    #[test]
    fn test_header_serde() {
        let header = Header {
            block_size: 16 * 1024 * 1024,
            block_idx: 4,
            key: "prefix/abcdef20421.4.16000000".to_string(),
        };
        let mut buf = Vec::with_capacity(100);
        header.write_to(&mut buf).unwrap();
        println!("buf: {:?}", buf);

        let mut file = Cursor::new(buf);
        let header_deser = Header::read_from(&mut file).unwrap();
        assert_eq!(header.block_size, header_deser.block_size);
        assert_eq!(header.block_idx, header_deser.block_idx);
        assert_eq!(header.key, header_deser.key);
    }

    #[test]
    fn test_deserialize_invalid_magic_num() {
        let invalid_magic = vec![
            78, 65, 74, 64, 61, 63, 68, 65, 1, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 29,
            0, 0, 0, 112, 114, 101, 102, 105, 120, 47, 97, 98, 99, 100, 101, 102, 50, 48, 52, 50,
            49, 46, 52, 46, 49, 54, 48, 48, 48, 48, 48, 48,
        ];
        let mut file = Cursor::new(invalid_magic);
        let result = Header::read_from(&mut file);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_unsupported_version() {
        let invalid_version = vec![
            88, 69, 84, 67, 65, 67, 72, 69, 8, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 29,
            0, 0, 0, 112, 114, 101, 102, 105, 120, 47, 97, 98, 99, 100, 101, 102, 50, 48, 52, 50,
            49, 46, 52, 46, 49, 54, 48, 48, 48, 48, 48, 48,
        ];
        let mut file = Cursor::new(invalid_version);
        let result = Header::read_from(&mut file);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_eof() {
        let invalid_version = vec![
            88, 69, 84, 67, 65, 67, 72, 69, 1, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 29,
            0, 0, 0, 112, 114, 101, 102, 105, 120, 47, 97, 98, 99, 100,
        ];
        let mut file = Cursor::new(invalid_version);
        let result = Header::read_from(&mut file);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_delete_invalid_entries() {
        let dir = CacheDirTest::new("load_invalid");
        let m = DiskManager::new(dir.get_path().to_path_buf());
        let mut key = CacheValue::new(5, 0, "abc".to_string(), 1024, 0);
        let mut data: Vec<u8> = vec![1, 2, 3, 4, 5];
        m.write(&key, data.as_slice()).await.unwrap();
        key.key = "bcd".to_string();
        m.write(&key, data.as_slice()).await.unwrap();

        let invalid_file_path = dir.get_path().to_path_buf().join("YS4xLjEw.1");
        let mut invalid_file = File::create(invalid_file_path.clone()).unwrap();
        invalid_file.write_all(data.as_mut_slice()).unwrap();
        drop(invalid_file); // make sure file has been synced to disk

        // check that a new DiskManager will load in the 2 cached files.
        let m2 = DiskManager::new(dir.get_path().to_path_buf());
        let mut vals: Vec<CacheValue> = m2.init().unwrap().collect();
        assert_eq!(vals.len(), 2);
        vals.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(vals[0].key.as_str(), "abc");
        assert_eq!(vals[1].key.as_str(), "bcd");

        // check that the invalid file was deleted as part of the load.
        assert!(File::open(invalid_file_path).is_err())
    }
}
