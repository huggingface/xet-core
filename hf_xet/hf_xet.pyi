from typing import List, Optional, Tuple, Callable, Union

class PyXetUploadInfo:
    hash: str
    file_size: int
    
    def __init__(self, hash: str, file_size: int) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    
    # Backward compatibility property
    @property
    def filesize(self) -> int: ...

class PyXetDownloadInfo:
    destination_path: str
    hash: str
    file_size: int
    
    def __init__(self, destination_path: str, hash: str, file_size: int) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class PyPointerFile(PyXetDownloadInfo):
    def __init__(self, path: str, hash: str, filesize: int) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    
    @property
    def path(self) -> str: ...
    @path.setter
    def path(self, value: str) -> None: ...
    
    @property
    def filesize(self) -> int: ...

class PyItemProgressUpdate:
    item_name: str
    total_bytes: int
    bytes_completed: int
    bytes_completion_increment: int

class PyTotalProgressUpdate:
    total_bytes: int
    total_bytes_increment: int
    total_bytes_completed: int
    total_bytes_completion_increment: int
    total_bytes_completion_rate: Optional[float]
    total_transfer_bytes: int
    total_transfer_bytes_increment: int
    total_transfer_bytes_completed: int
    total_transfer_bytes_completion_increment: int
    total_transfer_bytes_completion_rate: Optional[float]

# Type aliases for progress updater callbacks
SimpleProgressUpdater = Callable[[int], None]
DetailedProgressUpdater = Callable[[PyTotalProgressUpdate, List[PyItemProgressUpdate]], None]
ProgressUpdater = Union[SimpleProgressUpdater, DetailedProgressUpdater]

def upload_files(
    file_paths: List[str],
    endpoint: Optional[str] = None,
    token_info: Optional[Tuple[str, int]] = None,
    token_refresher: Optional[Callable[[], Tuple[str, int]]] = None,
    progress_updater: Optional[ProgressUpdater] = None,
    _repo_type: Optional[str] = None,
) -> List[PyXetUploadInfo]: ...

def upload_bytes(
    file_contents: List[bytes],
    endpoint: Optional[str] = None,
    token_info: Optional[Tuple[str, int]] = None,
    token_refresher: Optional[Callable[[], Tuple[str, int]]] = None,
    progress_updater: Optional[ProgressUpdater] = None,
    _repo_type: Optional[str] = None,
) -> List[PyXetUploadInfo]: ...

def download_files(
    files: List[PyXetDownloadInfo],
    endpoint: Optional[str] = None,
    token_info: Optional[Tuple[str, int]] = None,
    token_refresher: Optional[Callable[[], Tuple[str, int]]] = None,
    progress_updater: Optional[List[ProgressUpdater]] = None,
) -> List[str]: ...
