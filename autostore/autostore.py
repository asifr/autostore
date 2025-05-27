"""
AutoStore - File Storage Made Simple

License: Apache License 2.0

Changes
-------
- 0.1.4 - parquet and csv are loaded as LazyFrames by default and sparse matrices are now saved as .sparse.npz
- 0.1.3
    - Refactored to use different storage backends including local file system and S3.
    - Included methods for file operations: upload, download, delete, copy, move, and list files.
    - Added support for directory-like structures in S3.
    - Implemented metadata retrieval for files.
    - Included utility functions for path parsing and glob pattern matching.
    - Calling store.keys() now only returns keys without extensions.
- 0.1.2 - config, setup_logging, and load_dotenv are now imported at the module top level
- 0.1.1 - Added config, setup_logging, and load_dotenv
- 0.1.0 - Initial release
"""

import os
import io
import re
import sys
import json
import codecs
import pickle
import shutil
import xxhash
import logging
import tempfile
import contextlib
import typing as t
from pathlib import Path
from urllib.parse import urlparse
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from dataclasses import dataclass, field

CONTENT_TYPES = {
    ".txt": "text/plain",
    ".html": "text/html",
    ".json": "application/json",
    ".csv": "text/csv",
    ".yaml": "application/x-yaml",
    ".yml": "application/x-yaml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".parquet": "application/octet-stream",
    ".pkl": "application/octet-stream",
    ".pt": "application/octet-stream",
    ".pth": "application/octet-stream",
    ".gif": "image/gif",
    ".pdf": "application/pdf",
    ".zip": "application/zip",
    ".npy": "application/octet-stream",
    ".npz": "application/octet-stream",
}

log = logging.getLogger(__name__)


def hash_obj(obj: str, seed: int = 123) -> str:
    """Generate a non-cryptographic hash from a string."""
    if isinstance(obj, (list, tuple)):
        obj = "_".join(map(str, obj))
    # Handle bytes and dicts
    if isinstance(obj, bytes):
        obj = obj.decode("utf-8", errors="ignore")
    if isinstance(obj, dict):
        obj = json.dumps(obj, sort_keys=True)
    if not isinstance(obj, str):
        logging.warning(f"Object {obj} cannot be serialized, using its ID for hashing.")
        obj = str(id(obj))
    return xxhash.xxh3_64_hexdigest(obj, seed=seed)


@dataclass
class FileMetadata:
    """Metadata information for a file."""

    size: int
    modified_time: datetime
    created_time: t.Optional[datetime] = None
    content_type: t.Optional[str] = None
    etag: t.Optional[str] = None
    extra: t.Dict[str, t.Any] = field(default_factory=dict)


@dataclass
class StorageConfig:
    """Base configuration for storage backends."""

    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    chunk_size: int = 8192
    enable_compression: bool = False
    cache_enabled: bool = False
    cache_dir: t.Optional[str] = None
    cache_expiry_hours: int = 24
    temp_dir: t.Optional[str] = None
    extra: t.Dict[str, t.Any] = field(default_factory=dict)


@dataclass
class CacheEntry:
    """Cache entry metadata."""

    file_path: Path
    created_time: datetime
    etag: t.Optional[str] = None
    size: int = 0


class StorageError(Exception):
    """Base exception for storage operations."""

    pass


class StorageFileNotFoundError(StorageError):
    """Raised when a file is not found."""

    pass


class StoragePermissionError(StorageError):
    """Raised when access is denied."""

    pass


class StorageConnectionError(StorageError):
    """Raised when connection to storage backend fails."""

    pass


class CacheManager:
    """Manages file caching with expiration."""

    def __init__(self, cache_dir: t.Optional[str] = None, expiry_hours: int = 24):
        if cache_dir is None:
            cache_dir = os.path.join(tempfile.gettempdir(), "autostore_cache")

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.expiry_hours = expiry_hours
        self._cache_index: t.Dict[str, CacheEntry] = {}
        self._temp_dir = None

    def get_temp_dir(self) -> Path:
        """Get temporary directory for intermediate files."""
        if self._temp_dir is None:
            self._temp_dir = Path(tempfile.mkdtemp(prefix="autostore_temp_"))
        return self._temp_dir

    def _get_cache_key(self, backend_uri: str, file_path: str) -> str:
        """Generate cache key from backend URI and file path."""
        key_string = f"{backend_uri}:{file_path}"
        return hash_obj(key_string)

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if cache entry is expired."""
        expiry_time = entry.created_time + timedelta(hours=self.expiry_hours)
        return datetime.now() > expiry_time

    def get_cached_file(self, backend_uri: str, file_path: str, etag: t.Optional[str] = None) -> t.Optional[Path]:
        """Get cached file path if valid cache exists."""
        cache_key = self._get_cache_key(backend_uri, file_path)

        if cache_key not in self._cache_index:
            return None

        entry = self._cache_index[cache_key]

        # Check if file still exists
        if not entry.file_path.exists():
            del self._cache_index[cache_key]
            return None

        # Check if expired
        if self._is_expired(entry):
            entry.file_path.unlink(missing_ok=True)
            del self._cache_index[cache_key]
            return None

        # Check etag if provided
        if etag and entry.etag and entry.etag != etag:
            entry.file_path.unlink(missing_ok=True)
            del self._cache_index[cache_key]
            return None

        return entry.file_path

    def cache_file(self, backend_uri: str, file_path: str, local_file_path: Path, etag: t.Optional[str] = None) -> Path:
        """Cache a file and return the cached path."""
        cache_key = self._get_cache_key(backend_uri, file_path)
        cached_file_path = self.cache_dir / f"{cache_key}_{local_file_path.name}"
        log.debug(f"Caching file {local_file_path} to {cached_file_path}")

        # Copy file to cache
        shutil.copy2(local_file_path, cached_file_path)

        # Update cache index
        entry = CacheEntry(
            file_path=cached_file_path, created_time=datetime.now(), etag=etag, size=cached_file_path.stat().st_size
        )
        self._cache_index[cache_key] = entry

        return cached_file_path

    def cleanup_temp(self):
        """Clean up temporary directory."""
        if self._temp_dir and self._temp_dir.exists():
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            self._temp_dir = None

    def cleanup_expired(self):
        """Clean up expired cache entries."""
        expired_keys = []
        for cache_key, entry in self._cache_index.items():
            if self._is_expired(entry):
                entry.file_path.unlink(missing_ok=True)
                expired_keys.append(cache_key)

        for key in expired_keys:
            del self._cache_index[key]


class StorageBackend(ABC):
    """
    Abstract base class for all storage backends.

    Now uses upload/download operations with temporary files instead of read/write bytes.
    """

    def __init__(self, uri: str, config: StorageConfig):
        """
        Initialize the storage backend.

        Args:
            uri: Storage URI (e.g., 'file:///path', 's3://bucket/prefix')
            config: Backend configuration
        """
        self.uri = uri
        self.config = config
        self._parsed_uri = urlparse(uri)
        self._temp_dir = None

        # Initialize cache manager if caching is enabled
        self.cache_manager: CacheManager = None
        if config.cache_enabled:
            self.cache_manager: CacheManager = CacheManager(
                cache_dir=config.cache_dir,
                expiry_hours=config.cache_expiry_hours,
            )

    def get_temp_dir(self) -> Path:
        """Get temporary directory for intermediate files."""
        if self._temp_dir is None:
            self._temp_dir = Path(tempfile.mkdtemp(prefix="autostore_temp_"))
        return self._temp_dir

    @property
    def scheme(self) -> str:
        """Return the URI scheme (e.g., 'file', 's3', 'gcs')."""
        return self._parsed_uri.scheme

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file or directory exists at the given path."""
        pass

    @abstractmethod
    def download(self, remote_path: str, local_path: Path) -> None:
        """Download a file from remote storage to local path."""
        pass

    @abstractmethod
    def upload(self, local_path: Path, remote_path: str) -> None:
        """Upload a local file to remote storage."""
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete a file."""
        pass

    @abstractmethod
    def list_files(self, pattern: str = "*", recursive: bool = True) -> t.Iterator[str]:
        """List files matching a pattern."""
        pass

    @abstractmethod
    def get_metadata(self, path: str) -> FileMetadata:
        """Get metadata for a file."""
        pass

    def download_with_cache(self, remote_path: str) -> Path:
        """Download file with caching support."""
        if not self.cache_manager:
            # No caching, use temp file
            temp_file = self.get_temp_dir() / f"temp_{Path(remote_path).name}"
            self.download(remote_path, temp_file)
            return temp_file

        # Check cache first
        try:
            metadata = self.get_metadata(remote_path)
            etag = metadata.etag
        except Exception:
            etag = None

        cached_file = self.cache_manager.get_cached_file(self.uri, remote_path, etag)
        if cached_file:
            log.debug(f"Cache hit for {remote_path}, using cached file: {cached_file}")
            return cached_file

        log.debug(f"Cache miss for {remote_path}, downloading...")

        # Download to temp, then cache
        temp_file = self.cache_manager.get_temp_dir() / f"download_{hash_obj(Path(remote_path).name)}"
        self.download(remote_path, temp_file)

        # Cache the file
        cached_file = self.cache_manager.cache_file(self.uri, remote_path, temp_file, etag)
        return cached_file

    def mkdir(self, path: str) -> None:
        """Create a directory and any necessary parent directories."""
        pass

    def copy(self, src_path: str, dst_path: str) -> None:
        """Copy a file within the same backend."""
        temp_file = self.download_with_cache(src_path)
        self.upload(temp_file, dst_path)

    def move(self, src_path: str, dst_path: str) -> None:
        """Move a file within the same backend."""
        self.copy(src_path, dst_path)
        self.delete(src_path)

    def get_size(self, path: str) -> int:
        """Get the size of a file in bytes."""
        return self.get_metadata(path).size

    def is_directory(self, path: str) -> bool:
        """Check if a path represents a directory."""
        try:
            next(self.list_files(f"{path.rstrip('/')}/*", recursive=False))
            return True
        except StopIteration:
            return False

    def cleanup(self) -> None:
        """Clean up resources used by the backend."""
        if self.cache_manager:
            self.cache_manager.cleanup_temp()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(uri='{self.uri}')"


@dataclass
class LocalFileConfig(StorageConfig):
    """Configuration for local file backend."""

    pass


class LocalFileBackend(StorageBackend):
    """Local filesystem storage backend."""

    def __init__(self, uri: str, config: LocalFileConfig):
        super().__init__(uri, config)

        # Parse the URI to get the actual path
        parsed = urlparse(uri)

        if parsed.scheme == "file":
            self.root_path = Path(parsed.path)
        elif parsed.scheme == "":
            self.root_path = Path(uri)
        else:
            raise ValueError(f"Unsupported scheme for LocalFileBackend: {parsed.scheme}")

        # Expand user home directory and resolve relative paths
        self.root_path = self.root_path.expanduser().resolve()

        # Canonicalize the root path to handle symlinks securely
        self.root_path = Path(os.path.realpath(self.root_path))

        # Create root directory if it doesn't exist
        try:
            self.root_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StoragePermissionError(f"Cannot create root directory {self.root_path}: {e}")

    def download_with_cache(self, remote_path: str) -> Path:
        """For local files, just return the original path - no copying needed."""
        return self._get_full_path(remote_path)

    def _get_full_path(self, path: str) -> Path:
        """Convert a relative path to an absolute path within the root directory."""
        path = path.replace("\\", "/").strip("/")
        full_path = (self.root_path / path).resolve()

        # Security check with canonical path resolution
        try:
            # Use realpath to handle symlinks more securely
            canonical_full_path = Path(os.path.realpath(full_path))
            canonical_root_path = Path(os.path.realpath(self.root_path))

            # Ensure the canonical resolved path is still within the canonical root directory
            canonical_full_path.relative_to(canonical_root_path)

            # Also verify the original resolved path is within the original root
            # This catches cases where symlinks might bypass the canonical check
            full_path.relative_to(self.root_path)

        except ValueError:
            raise StoragePermissionError(f"Path '{path}' resolves outside of root directory")

        return full_path

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists."""
        try:
            return self._get_full_path(path).exists()
        except (OSError, StoragePermissionError):
            return False

    def download(self, remote_path: str, local_path: Path) -> None:
        """Download (copy) file from storage to local path."""
        full_path = self._get_full_path(remote_path)

        try:
            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy file
            shutil.copy2(full_path, local_path)
        except FileNotFoundError:
            raise StorageFileNotFoundError(f"File not found: {remote_path}")
        except PermissionError as e:
            raise StoragePermissionError(f"Permission denied reading {remote_path}: {e}")
        except OSError as e:
            raise StorageError(f"Error downloading {remote_path}: {e}")

    def upload(self, local_path: Path, remote_path: str) -> None:
        """Upload (copy) local file to storage."""
        full_path = self._get_full_path(remote_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            shutil.copy2(local_path, full_path)
        except PermissionError as e:
            raise StoragePermissionError(f"Permission denied writing {remote_path}: {e}")
        except OSError as e:
            raise StorageError(f"Error uploading {remote_path}: {e}")

    def delete(self, path: str) -> None:
        """Delete a file."""
        full_path = self._get_full_path(path)

        try:
            if full_path.is_file():
                full_path.unlink()
            elif full_path.is_dir():
                shutil.rmtree(full_path)
            else:
                raise StorageFileNotFoundError(f"File not found: {path}")
        except FileNotFoundError:
            raise StorageFileNotFoundError(f"File not found: {path}")
        except PermissionError as e:
            raise StoragePermissionError(f"Permission denied deleting {path}: {e}")
        except OSError as e:
            raise StorageError(f"Error deleting {path}: {e}")

    def list_files(self, pattern: str = "*", recursive: bool = True) -> t.Iterator[str]:
        """List files matching a pattern."""
        try:
            if recursive:
                glob_pattern = "**/" + pattern if not pattern.startswith("**/") else pattern
                paths = self.root_path.rglob(glob_pattern)
            else:
                paths = self.root_path.glob(pattern)

            for full_path in paths:
                if full_path.is_file():
                    try:
                        rel_path = full_path.relative_to(self.root_path)
                        yield str(rel_path).replace("\\", "/")
                    except ValueError:
                        continue

        except OSError as e:
            raise StorageError(f"Error listing files with pattern '{pattern}': {e}")

    def get_metadata(self, path: str) -> FileMetadata:
        """Get file metadata."""
        full_path = self._get_full_path(path)

        try:
            stat = full_path.stat()
            modified_time = datetime.fromtimestamp(stat.st_mtime)
            created_time = datetime.fromtimestamp(stat.st_ctime)
            content_type = self._guess_content_type(full_path.suffix)

            return FileMetadata(
                size=stat.st_size,
                modified_time=modified_time,
                created_time=created_time,
                content_type=content_type,
                extra={
                    "mode": stat.st_mode,
                    "uid": stat.st_uid,
                    "gid": stat.st_gid,
                    "atime": stat.st_atime,
                },
            )

        except FileNotFoundError:
            raise StorageFileNotFoundError(f"File not found: {path}")
        except OSError as e:
            raise StorageError(f"Error getting metadata for {path}: {e}")

    def _guess_content_type(self, extension: str) -> t.Optional[str]:
        """Guess content type from file extension."""
        extension = extension.lower()
        return CONTENT_TYPES.get(extension)


class BackendRegistry:
    """Registry for managing storage backends."""

    def __init__(self):
        self._backends: t.Dict[str, t.Type[StorageBackend]] = {}
        self._register_default_backends()

    def _register_default_backends(self):
        """Register default backends."""
        self.register("file", LocalFileBackend)
        self.register("", LocalFileBackend)  # Empty scheme defaults to local

    def register(self, scheme: str, backend_class: t.Type[StorageBackend]) -> None:
        """Register a backend for a URI scheme."""
        self._backends[scheme.lower()] = backend_class

    def unregister(self, scheme: str) -> None:
        """Unregister a backend scheme."""
        self._backends.pop(scheme.lower(), None)

    def get_backend_class(self, scheme: str) -> t.Optional[t.Type[StorageBackend]]:
        """Get backend class for a scheme."""
        return self._backends.get(scheme.lower())

    def get_supported_schemes(self) -> t.List[str]:
        """Get list of supported URI schemes."""
        return list(self._backends.keys())


# Global backend registry
_backend_registry = BackendRegistry()


class DataHandler(ABC):
    """Abstract base class for all data handlers."""

    @abstractmethod
    def can_handle_extension(self, extension: str) -> bool:
        """Check if this handler can handle the given file extension."""
        pass

    @abstractmethod
    def can_handle_data(self, data: t.Any) -> bool:
        """Check if this handler can handle the given data instance for writing."""
        pass

    @abstractmethod
    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        """Read data from file."""
        pass

    @abstractmethod
    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        """Write data to file."""
        pass

    @property
    @abstractmethod
    def extensions(self) -> t.List[str]:
        """List of file extensions this handler supports."""
        pass

    @property
    @abstractmethod
    def priority(self) -> int:
        """Priority for type inference (higher = more preferred)."""
        pass


class ParquetHandler(DataHandler):
    """Handler for Parquet files using Polars."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".parquet"

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import polars as pl

            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        try:
            import polars as pl

            return pl.scan_parquet(file_path)
        except ImportError:
            raise ImportError("Polars is required to load .parquet files")

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        try:
            import polars as pl

            if isinstance(data, pl.LazyFrame):
                data = data.collect()
            if isinstance(data, pl.DataFrame):
                # Ensure parent directory exists
                file_path.parent.mkdir(parents=True, exist_ok=True)
                data.write_parquet(file_path)
            else:
                raise TypeError(f"Cannot save {type(data)} as parquet. Expected DataFrame or LazyFrame")
        except ImportError:
            raise ImportError("Polars is required to save .parquet files")

    @property
    def extensions(self) -> t.List[str]:
        return [".parquet"]

    @property
    def priority(self) -> int:
        return 10


class CSVHandler(DataHandler):
    """Handler for CSV files using Polars."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".csv"

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import polars as pl

            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        try:
            import polars as pl

            return pl.scan_csv(file_path, truncate_ragged_lines=True)
        except ImportError:
            raise ImportError("Polars is required to load .csv files")

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        try:
            import polars as pl

            if isinstance(data, pl.LazyFrame):
                data = data.collect()
            if isinstance(data, pl.DataFrame):
                file_path.parent.mkdir(parents=True, exist_ok=True)
                data.write_csv(file_path)
            else:
                raise TypeError(f"Cannot save {type(data)} as CSV. Expected DataFrame or LazyFrame")
        except ImportError:
            raise ImportError("Polars is required to save .csv files")

    @property
    def extensions(self) -> t.List[str]:
        return [".csv"]

    @property
    def priority(self) -> int:
        return 5


class JSONHandler(DataHandler):
    """Handler for JSON files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".json"

    def can_handle_data(self, data: t.Any) -> bool:
        return isinstance(data, (dict, list, int, float, bool, type(None), str))

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str)

    @property
    def extensions(self) -> t.List[str]:
        return [".json"]

    @property
    def priority(self) -> int:
        return 8


class JSONLHandler(DataHandler):
    """Handler for JSON Lines files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".jsonl"

    def can_handle_data(self, data: t.Any) -> bool:
        return isinstance(data, list) and len(data) > 0 and all(isinstance(item, dict) for item in data)

    def read_from_file(self, file_path: Path, file_extension: str) -> t.List[t.Any]:
        result = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    result.append(json.loads(line))
        return result

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        if not isinstance(data, list):
            raise TypeError(f"Cannot save {type(data)} as JSONL. Expected list")

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, default=str) + "\n")

    @property
    def extensions(self) -> t.List[str]:
        return [".jsonl"]

    @property
    def priority(self) -> int:
        return 6


class TorchHandler(DataHandler):
    """Handler for PyTorch model files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".pt", ".pth"]

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import torch

            return (
                isinstance(data, torch.Tensor)
                or hasattr(data, "state_dict")
                or (
                    hasattr(data, "__class__")
                    and hasattr(data.__class__, "__module__")
                    and "torch" in str(data.__class__.__module__)
                )
            )
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        try:
            import torch

            return torch.load(file_path, map_location="cpu")
        except ImportError:
            raise ImportError("PyTorch is required to load .pt/.pth files")

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        try:
            import torch

            file_path.parent.mkdir(parents=True, exist_ok=True)
            torch.save(data, file_path)
        except ImportError:
            raise ImportError("PyTorch is required to save .pt/.pth files")

    @property
    def extensions(self) -> t.List[str]:
        return [".pt", ".pth"]

    @property
    def priority(self) -> int:
        return 9


class TextHandler(DataHandler):
    """Handler for plain text files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".txt", ".html", ".md"]

    def can_handle_data(self, data: t.Any) -> bool:
        return isinstance(data, str)

    def read_from_file(self, file_path: Path, file_extension: str) -> str:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        if not isinstance(data, str):
            raise TypeError(f"Cannot save {type(data)} as text. Expected string")

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(data)

    @property
    def extensions(self) -> t.List[str]:
        return [".txt", ".html", ".md"]

    @property
    def priority(self) -> int:
        return 7


class YAMLHandler(DataHandler):
    """Handler for YAML files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".yaml", ".yml"]

    def can_handle_data(self, data: t.Any) -> bool:
        return isinstance(data, (dict, list, str, int, float, bool, type(None)))

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        try:
            import yaml

            with open(file_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except ImportError:
            raise ImportError("PyYAML is required to load YAML files")

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        try:
            import yaml

            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False)
        except ImportError:
            raise ImportError("PyYAML is required to save YAML files")

    @property
    def extensions(self) -> t.List[str]:
        return [".yaml", ".yml"]

    @property
    def priority(self) -> int:
        return 7


class ImageHandler(DataHandler):
    """Handler for PIL/Pillow Image objects."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".png", ".jpg", ".jpeg", ".bmp", ".tiff"]

    def can_handle_data(self, data: t.Any) -> bool:
        return (
            hasattr(data, "save")
            and hasattr(data, "mode")
            and hasattr(data, "size")
            and hasattr(data.__class__, "__module__")
            and "PIL" in str(data.__class__.__module__)
        )

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        try:
            from PIL import Image

            return Image.open(file_path)
        except ImportError:
            raise ImportError("Pillow is required to load image files")

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        # Determine format from extension
        format_map = {".png": "PNG", ".jpg": "JPEG", ".jpeg": "JPEG", ".bmp": "BMP", ".tiff": "TIFF"}
        format_name = format_map.get(file_extension.lower(), "PNG")
        data.save(file_path, format=format_name)

    @property
    def extensions(self) -> t.List[str]:
        return [".png", ".jpg", ".jpeg", ".bmp", ".tiff"]

    @property
    def priority(self) -> int:
        return 10


class SparseHandler(DataHandler):
    """Handler for SciPy sparse matrices."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".sparse.npz"

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            from scipy import sparse

            return sparse.issparse(data)
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        try:
            from scipy import sparse

            return sparse.load_npz(file_path)
        except ImportError:
            raise ImportError("SciPy is required to load .sparse files")

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        try:
            from scipy import sparse

            if not sparse.issparse(data):
                raise TypeError(f"Cannot save {type(data)} as .sparse. Expected scipy sparse matrix")

            file_path.parent.mkdir(parents=True, exist_ok=True)
            sparse.save_npz(file_path, data)
        except ImportError:
            raise ImportError("SciPy is required to save .sparse files")

    @property
    def extensions(self) -> t.List[str]:
        return [".sparse.npz"]

    @property
    def priority(self) -> int:
        return 9


class PydanticHandler(DataHandler):
    """Handler for Pydantic BaseModel instances."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".pydantic.json"

    def can_handle_data(self, data: t.Any) -> bool:
        return (
            hasattr(data, "model_dump")
            and hasattr(data, "model_validate")
            and hasattr(data.__class__, "__pydantic_core_schema__")
        )

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        # Note: This would need the original model class to reconstruct
        # For now, just return the JSON data
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data.model_dump(), f, indent=2, default=str)

    @property
    def extensions(self) -> t.List[str]:
        return [".pydantic.json"]

    @property
    def priority(self) -> int:
        return 12


class DataclassHandler(DataHandler):
    """Handler for Python dataclass instances."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".dataclass.json"

    def can_handle_data(self, data: t.Any) -> bool:
        return hasattr(data, "__dataclass_fields__")

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        # Similar limitation as Pydantic - would need original class
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        import dataclasses

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(dataclasses.asdict(data), f, indent=2, default=str)

    @property
    def extensions(self) -> t.List[str]:
        return [".dataclass.json"]

    @property
    def priority(self) -> int:
        return 11


class PickleHandler(DataHandler):
    """Handler for Pickle files - fallback for any Python object."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".pkl", ".pickle"]

    def can_handle_data(self, data: t.Any) -> bool:
        return True  # Pickle can handle any Python object

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        with open(file_path, "rb") as f:
            return pickle.load(f)

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            pickle.dump(data, f)

    @property
    def extensions(self) -> t.List[str]:
        return [".pkl", ".pickle"]

    @property
    def priority(self) -> int:
        return 1  # Lowest priority - fallback option


class NumpyHandler(DataHandler):
    """Handler for NumPy arrays."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".npy", ".npz"]

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import numpy as np

            return isinstance(data, np.ndarray)
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> t.Any:
        try:
            import numpy as np

            return np.load(file_path)
        except ImportError:
            raise ImportError("NumPy is required to load .npy/.npz files")

    def write_to_file(self, data: t.Any, file_path: Path, file_extension: str) -> None:
        try:
            import numpy as np

            file_path.parent.mkdir(parents=True, exist_ok=True)

            if file_extension.lower() == ".npy":
                if isinstance(data, np.ndarray):
                    np.save(file_path, data)
                else:
                    raise TypeError(f"Cannot save {type(data)} as .npy. Expected numpy array")
            elif file_extension.lower() == ".npz":
                if isinstance(data, dict):
                    np.savez(file_path, **data)
                elif isinstance(data, np.ndarray):
                    np.savez(file_path, data)
                else:
                    raise TypeError(f"Cannot save {type(data)} as .npz. Expected dict or numpy array")
        except ImportError:
            raise ImportError("NumPy is required to save .npy/.npz files")

    @property
    def extensions(self) -> t.List[str]:
        return [".npy", ".npz"]

    @property
    def priority(self) -> int:
        return 9


class HandlerRegistry:
    """Registry for managing data handlers."""

    def __init__(self):
        self._handlers: t.List[DataHandler] = []
        self._extension_map: t.Dict[str, t.List[DataHandler]] = {}
        self._register_default_handlers()

    def _register_default_handlers(self):
        """Register all default handlers."""
        default_handlers = [
            ParquetHandler(),
            CSVHandler(),
            JSONHandler(),
            JSONLHandler(),
            PydanticHandler(),
            DataclassHandler(),
            YAMLHandler(),
            ImageHandler(),
            TorchHandler(),
            NumpyHandler(),
            SparseHandler(),
            TextHandler(),
            PickleHandler(),  # Keep pickle as fallback
        ]

        for handler in default_handlers:
            self.register(handler)

    def register(self, handler: DataHandler) -> None:
        """Register a new handler."""
        self._handlers.append(handler)

        # Update extension mapping
        for ext in handler.extensions:
            ext_lower = ext.lower()
            if ext_lower not in self._extension_map:
                self._extension_map[ext_lower] = []
            self._extension_map[ext_lower].append(handler)
            # Sort by priority (higher priority first)
            self._extension_map[ext_lower].sort(key=lambda h: h.priority, reverse=True)

    def unregister(self, handler_class: t.Type[DataHandler]) -> None:
        """Unregister a handler by class type."""
        # Remove from main list
        self._handlers = [h for h in self._handlers if not isinstance(h, handler_class)]

        # Rebuild extension mapping
        self._extension_map.clear()
        for handler in self._handlers:
            for ext in handler.extensions:
                ext_lower = ext.lower()
                if ext_lower not in self._extension_map:
                    self._extension_map[ext_lower] = []
                self._extension_map[ext_lower].append(handler)
                self._extension_map[ext_lower].sort(key=lambda h: h.priority, reverse=True)

    def get_handler_for_extension(self, extension: str) -> t.Optional[DataHandler]:
        """Get the best handler for a given file extension."""
        ext_lower = extension.lower()
        handlers = self._extension_map.get(ext_lower, [])
        return handlers[0] if handlers else None

    def get_handler_for_data(self, data: t.Any) -> t.Optional[DataHandler]:
        """Get the best handler for a given data instance."""
        compatible_handlers = []
        for handler in self._handlers:
            if handler.can_handle_data(data):
                compatible_handlers.append(handler)

        # Sort by priority and return the best match
        compatible_handlers.sort(key=lambda h: h.priority, reverse=True)
        return compatible_handlers[0] if compatible_handlers else None

    def get_supported_extensions(self) -> t.List[str]:
        """Get all supported file extensions."""
        return list(self._extension_map.keys())


class AutoStore:
    """
    Read and write files like a dictionary with pluggable storage backends.

    Now uses upload/download operations with caching and dataclass configs.
    Optimized for local file access without unnecessary copying.
    """

    def __init__(
        self,
        storage_uri: t.Union[str, Path],
        config: t.Optional[StorageConfig] = None,
    ):
        """
        Initialize AutoStore with a storage backend.

        Args:
            storage_uri: Storage URI or path
            config: Backend configuration (dataclass)
        """
        # Handle Path objects
        if isinstance(storage_uri, Path):
            storage_uri = str(storage_uri)

        self.storage_uri = storage_uri

        # Parse URI to determine backend
        parsed_uri = urlparse(storage_uri)
        scheme = parsed_uri.scheme.lower() if parsed_uri.scheme else ""

        # Get backend class from registry
        backend_class = _backend_registry.get_backend_class(scheme)
        if not backend_class:
            supported = _backend_registry.get_supported_schemes()
            raise ValueError(f"Unsupported storage scheme: '{scheme}'. Supported schemes: {supported}")

        # Create default config if none provided
        if config is None:
            if scheme in ("", "file"):
                config = LocalFileConfig()
            else:
                config = StorageConfig()

        # Initialize backend
        try:
            self.backend = backend_class(storage_uri, config)
        except Exception as e:
            raise StorageError(f"Failed to initialize {scheme} backend: {e}") from e

        self.handler_registry = HandlerRegistry()

    @classmethod
    def register_backend(cls, scheme: str, backend_class: t.Type[StorageBackend]) -> None:
        """Register a new storage backend."""
        _backend_registry.register(scheme, backend_class)

    @classmethod
    def unregister_backend(cls, scheme: str) -> None:
        """Unregister a storage backend."""
        _backend_registry.unregister(scheme)

    @classmethod
    def get_supported_backends(cls) -> t.List[str]:
        """Get list of supported backend schemes."""
        return _backend_registry.get_supported_schemes()

    def register_handler(self, handler: DataHandler) -> None:
        """Register a custom data handler."""
        self.handler_registry.register(handler)

    def unregister_handler(self, handler_class: t.Type[DataHandler]) -> None:
        """Unregister a data handler by class type."""
        self.handler_registry.unregister(handler_class)

    def _infer_extension(self, data: t.Any) -> str:
        """Infer the appropriate file extension based on data instance."""
        handler = self.handler_registry.get_handler_for_data(data)
        if handler and handler.extensions:
            return handler.extensions[0]
        return ".pkl"  # Fallback to pickle

    def _find_file_key(self, key: str) -> str:
        """Find the actual file key for a given key."""
        # Normalize the key
        key = key.replace("\\", "/")

        # Try direct key first (with extension)
        if self.backend.exists(key):
            return key

        # If no extension provided, search for files with supported extensions
        if not Path(key).suffix:
            for ext in self.handler_registry.get_supported_extensions():
                test_key = key + ext
                if self.backend.exists(test_key):
                    return test_key

        # If still not found, raise an error instead of doing expensive iteration
        raise StorageFileNotFoundError(f"No file found for key: {key}")

    def find_file_fuzzy(self, key: str) -> t.Optional[str]:
        """
        Perform fuzzy search for a file key.

        This method performs an expensive iteration through all files and should
        be used sparingly. For performance-critical applications, use exact keys.

        Args:
            key: The key to search for (case-insensitive, stem matching)

        Returns:
            The actual file key if found, None otherwise
        """
        key_lower = key.lower()
        key_stem = Path(key).stem.lower()

        for file_key in self.backend.list_files():
            file_key_lower = file_key.lower()

            # Check exact match (case insensitive)
            if file_key_lower == key_lower:
                return file_key

            # Check stem match (filename without extension)
            if Path(file_key).stem.lower() == key_stem:
                return file_key

        return None

    def __getitem__(self, key: str) -> t.Any:
        """Load and return data for the given key."""
        try:
            file_key = self._find_file_key(key)

            # Get file extension to determine handler
            ext = Path(file_key).suffix.lower()
            handler = self.handler_registry.get_handler_for_extension(ext)

            if not handler:
                supported = ", ".join(self.handler_registry.get_supported_extensions())
                raise ValueError(f"Unsupported file type: {ext}. Supported types: {supported}")

            # Download file (with caching) - LocalFileBackend returns original path
            local_file_path = self.backend.download_with_cache(file_key)

            # Use handler to deserialize from file
            result = handler.read_from_file(local_file_path, ext)
            return result

        except Exception as e:
            if isinstance(e, (StorageFileNotFoundError, ValueError)):
                raise
            raise StorageError(f"Failed to load {key}: {str(e)}") from e

    def __setitem__(self, key: str, data: t.Any) -> None:
        """Save data to the given key."""
        # Normalize the key
        key = key.replace("\\", "/")

        # If no extension provided, infer it from the data type
        if not Path(key).suffix:
            extension = self._infer_extension(data)
            key = key + extension

        # Get the appropriate handler
        ext = Path(key).suffix.lower()
        handler = self.handler_registry.get_handler_for_extension(ext)

        if not handler:
            # Fallback to pickle for unknown extensions
            handler = self.handler_registry.get_handler_for_extension(".pkl")
            key = str(Path(key).with_suffix(".pkl"))
            ext = ".pkl"
            log.warning(f"No handler found for {ext}. Using fallback handler (pickle).")

        try:
            # Optimize for LocalFileBackend - write directly to final location
            if isinstance(self.backend, LocalFileBackend):
                final_path = self.backend._get_full_path(key)
                handler.write_to_file(data, final_path, ext)
            else:
                # For cloud storage - use temp file + upload
                temp_dir = (
                    self.backend.cache_manager.get_temp_dir()
                    if self.backend.cache_manager
                    else self.backend.get_temp_dir()
                )
                temp_file = temp_dir / f"upload_{Path(key).name}"

                # Use handler to serialize data to file
                handler.write_to_file(data, temp_file, ext)

                # Upload file to backend
                self.backend.upload(temp_file, key)

        except Exception as e:
            raise StorageError(f"Failed to save data to {key}: {str(e)}") from e

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the data store."""
        try:
            self._find_file_key(key)
            return True
        except StorageFileNotFoundError:
            return False

    def __delitem__(self, key: str) -> None:
        """Delete a file from the data store."""
        file_key = self._find_file_key(key)
        try:
            self.backend.delete(file_key)
        except Exception as e:
            raise StorageError(f"Failed to delete {key}: {str(e)}") from e

    def keys(self) -> t.Iterator[str]:
        """Iterate over all available keys."""
        seen = set()
        supported_extensions = self.handler_registry.get_supported_extensions()

        for file_path in self.backend.list_files():
            if Path(file_path).suffix.lower() in supported_extensions:
                file_path_no_ext = str(Path(file_path).with_suffix(""))

                for key in file_path_no_ext:
                    if key not in seen:
                        seen.add(key)
                        yield key

    def list_files(self, pattern: str = "*", recursive: bool = True) -> t.Iterator[str]:
        """List files matching a pattern."""
        supported_extensions = self.handler_registry.get_supported_extensions()

        for file_path in self.backend.list_files(pattern, recursive):
            if Path(file_path).suffix.lower() in supported_extensions:
                yield file_path

    def get_metadata(self, key: str) -> FileMetadata:
        """Get metadata for a file."""
        file_key = self._find_file_key(key)
        return self.backend.get_metadata(file_key)

    def copy(self, src_key: str, dst_key: str) -> None:
        """Copy a file within the storage backend."""
        src_file_key = self._find_file_key(src_key)

        # If dst_key has no extension, infer from source
        if not Path(dst_key).suffix:
            src_ext = Path(src_file_key).suffix
            dst_key = dst_key + src_ext

        try:
            self.backend.copy(src_file_key, dst_key)
        except Exception as e:
            raise StorageError(f"Failed to copy {src_key} to {dst_key}: {str(e)}") from e

    def move(self, src_key: str, dst_key: str) -> None:
        """Move a file within the storage backend."""
        src_file_key = self._find_file_key(src_key)

        # If dst_key has no extension, infer from source
        if not Path(dst_key).suffix:
            src_ext = Path(src_file_key).suffix
            dst_key = dst_key + src_ext

        try:
            self.backend.move(src_file_key, dst_key)
        except Exception as e:
            raise StorageError(f"Failed to move {src_key} to {dst_key}: {str(e)}") from e

    def exists(self, key: str) -> bool:
        """Check if a file exists."""
        return key in self

    def get_size(self, key: str) -> int:
        """Get the size of a file in bytes."""
        return self.get_metadata(key).size

    def cleanup_cache(self) -> None:
        """Clean up expired cache entries."""
        if self.backend.cache_manager:
            self.backend.cache_manager.cleanup_expired()

    def __len__(self) -> int:
        """Return the number of files in the data store."""
        return sum(1 for _ in self.list_files("*"))

    def __repr__(self) -> str:
        return f"AutoStore(storage_uri='{self.storage_uri}', backend={self.backend.__class__.__name__})"

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()

    def cleanup(self) -> None:
        """Clean up resources."""
        if hasattr(self, "backend"):
            self.backend.cleanup()


def config(key: str, cast: t.Callable = None, default: t.Any = None) -> t.Any:
    """Get a configuration value from environment variables."""
    if key in os.environ:
        value = os.environ[key]
        if cast is None or value is None:
            return value
        elif cast is bool and isinstance(value, str):
            mapping = {"true": True, "1": True, "false": False, "0": False}
            value = value.lower()
            if value not in mapping:
                raise ValueError(f"Config '{key}' has value '{value}'. Not a valid bool.")
            return mapping[value]
        try:
            return cast(value)
        except (TypeError, ValueError):
            raise ValueError(f"Config '{key}' has value '{value}'. Not a valid {cast.__name__}.")

    try:
        return cast(default)
    except (TypeError, ValueError):
        return default


def _walk_to_root(path: str) -> t.Iterator[str]:
    """Yield directories starting from the given directory up to the root"""
    if not os.path.exists(path):
        raise IOError("Starting path not found")

    if os.path.isfile(path):
        path = os.path.dirname(path)

    last_dir = None
    current_dir = os.path.abspath(path)
    while last_dir != current_dir:
        yield current_dir
        parent_dir = os.path.abspath(os.path.join(current_dir, os.path.pardir))
        last_dir, current_dir = current_dir, parent_dir


def find_dotenv(filename: str = ".env", raise_error_if_not_found: bool = False, usecwd: bool = False) -> str:
    """Search in increasingly higher folders for the given file."""

    def _is_interactive():
        """Decide whether this is running in a REPL or IPython notebook"""
        try:
            main = __import__("__main__", None, None, fromlist=["__file__"])
        except ModuleNotFoundError:
            return False
        return not hasattr(main, "__file__")

    if usecwd or _is_interactive() or getattr(sys, "frozen", False):
        path = os.getcwd()
    else:
        frame = sys._getframe()
        current_file = __file__

        while frame.f_back is not None and (
            frame.f_code.co_filename == current_file or not os.path.exists(frame.f_code.co_filename)
        ):
            frame = frame.f_back
        frame_filename = frame.f_code.co_filename
        path = os.path.dirname(os.path.abspath(frame_filename))

    for dirname in _walk_to_root(path):
        check_path = os.path.join(dirname, filename)
        if os.path.isfile(check_path):
            return check_path

    if raise_error_if_not_found:
        raise IOError("File not found")

    return ""


def load_dotenv(dotenv_path: str = None, override: bool = True, encoding: str = "utf-8") -> None:
    """Load environment variables from a .env file into os.environ."""
    if dotenv_path is None:
        dotenv_path = find_dotenv()

    envvars = {}

    _whitespace = re.compile(r"[^\S\r\n]*", flags=re.UNICODE)
    _export = re.compile(r"(?:export[^\S\r\n]+)?", flags=re.UNICODE)
    _single_quoted_key = re.compile(r"'([^']+)'", flags=re.UNICODE)
    _unquoted_key = re.compile(r"([^=\#\s]+)", flags=re.UNICODE)
    _single_quoted_value = re.compile(r"'((?:\\'|[^'])*)'", flags=re.UNICODE)
    _double_quoted_value = re.compile(r'"((?:\\"|[^"])*)"', flags=re.UNICODE)
    _unquoted_value = re.compile(r"([^\r\n]*)", flags=re.UNICODE)
    _double_quote_escapes = re.compile(r"\\[\\'\"abfnrtv]", flags=re.UNICODE)
    _single_quote_escapes = re.compile(r"\\[\\']", flags=re.UNICODE)

    @contextlib.contextmanager
    def _get_stream() -> t.Iterator[t.IO[str]]:
        if dotenv_path and os.path.isfile(dotenv_path):
            with open(dotenv_path, encoding=encoding) as stream:
                yield stream
        else:
            yield io.StringIO("")

    _double_quote_map = {
        r"\\": "\\",
        r"\'": "'",
        r"\"": '"',
        r"\a": "\a",
        r"\b": "\b",
        r"\f": "\f",
        r"\n": "\n",
        r"\r": "\r",
        r"\t": "\t",
        r"\v": "\v",
    }

    def _double_quote_escape(m: t.Match[str]) -> str:
        return _double_quote_map[m.group()]

    nexport = len("export ")
    with _get_stream() as stream:
        for line in stream:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[nexport:]
            key, value = line.split("=", 1)
            key = key.strip()
            key = _single_quoted_key.sub(r"\1", key)
            key = _unquoted_key.sub(r"\1", key)
            key = _whitespace.sub("", key)
            key = _export.sub("", key)
            key = key.strip()
            if not key:
                continue

            if _single_quoted_value.match(value):
                value = _single_quoted_value.sub(r"\1", value)
                value = _single_quote_escapes.sub(r"\1", value)
            elif _double_quoted_value.match(value):
                value = _double_quoted_value.sub(r"\1", value)
                value = codecs.decode(value.encode("utf-8"), "unicode_escape")
            elif _unquoted_value.match(value):
                value = _unquoted_value.sub(r"\1", value)
                value = _double_quote_escapes.sub(_double_quote_escape, value)
                value = _whitespace.sub("", value)
            else:
                raise ValueError(f"Line {line} does not match format KEY=VALUE")
            envvars[key] = value

    for k, v in envvars.items():
        if k in os.environ and not override:
            continue
        if v is not None:
            os.environ[k] = v


def setup_logging(level: int = None, file: str = None, disable_stdout: bool = False):
    """Setup logging."""
    if level is None:
        level = logging.INFO
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    if file is None and disable_stdout:
        return
    handlers = []
    if not disable_stdout:
        handlers.append(logging.StreamHandler())
    if file is not None:
        os.makedirs(os.path.dirname(file), exist_ok=True)
        handlers.append(logging.FileHandler(file))
    logging.basicConfig(
        level=level,
        format="%(asctime)s|%(levelname)s|%(name)s|%(message)s",
        handlers=handlers,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
