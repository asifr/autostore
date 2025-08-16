"""
AutoStore - File Storage Made Simple

License: Apache License 2.0

Changes
-------
- 0.1.7 - Cache expiry can be set to 0 to never expire cache entries.
- 0.1.6 - Added Options and a new backend registry for auto-discovery of storage backends.
- 0.1.5 - added StorePath to use the Autostore instance in path-like operations
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
import pickle
import shutil
import hashlib
import logging
import tempfile
import importlib
import contextlib
import typing as t
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from dataclasses import dataclass, field, fields, asdict

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
        log.warning(f"Object {obj} cannot be serialized, using its ID for hashing.")
        obj = str(id(obj))
    return hashlib.md5(f"{seed}:{obj}".encode("utf-8")).hexdigest()


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
class Options:
    """Base options class for all storage backends."""

    cache_enabled: bool = False
    cache_dir: t.Optional[str] = None
    cache_expiry_hours: int = 24  # Set to 0 to never expire cache entries
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    temp_dir: t.Optional[str] = None
    backend_class: t.Optional[t.Type['StorageBackend']] = None


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


class BackendNotAvailableError(StorageError):
    """Raised when a backend is not available due to missing dependencies."""

    pass


class UnsupportedSchemeError(StorageError):
    """Raised when a URI scheme is not supported."""

    pass


class BackendConfigurationError(StorageError):
    """Raised when backend configuration is invalid."""

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
        if self.expiry_hours == 0:
            return False  # Never expire when expiry_hours is 0
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

    # Backend classes should declare their options class
    options_class = Options

    def __init__(self, uri: str, options: Options):
        """
        Initialize the storage backend.

        Args:
            uri: Storage URI (e.g., 'file:///path', 's3://bucket/prefix')
            options: Backend options
        """
        self.uri = uri
        self.options = options
        self._parsed_uri = urlparse(uri)
        self._temp_dir = None

        # Initialize cache manager if caching is enabled
        self.cache_manager = None
        if options.cache_enabled:
            self.cache_manager = CacheManager(
                cache_dir=options.cache_dir,
                expiry_hours=options.cache_expiry_hours,
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
        # For LocalFileBackend, we can check directly
        if hasattr(self, "_get_full_path"):
            try:
                full_path = self._get_full_path(path)
                return full_path.is_dir()
            except Exception:
                return False

        # Fallback for other backends - check if we can list files in it
        try:
            next(self.list_files(f"{path.rstrip('/')}/*", recursive=False))
            return True
        except StopIteration:
            return False

    def is_dataset(self, path: str) -> bool:
        """Check if path represents a dataset (directory with multiple files)."""
        if not self.is_directory(path):
            return False

        # Count files in the directory
        file_count = 0
        for _ in self.list_files(f"{path.rstrip('/')}/*", recursive=False):
            file_count += 1
            if file_count > 1:
                return True

        return False

    def download_dataset(
        self, remote_dataset_path: str, local_dataset_path: Path, file_pattern: str = "*"
    ) -> t.List[Path]:
        """Download entire dataset preserving directory structure."""
        downloaded_files = []

        # List all files in the dataset
        pattern = (
            f"{remote_dataset_path.rstrip('/')}/**/{file_pattern}"
            if file_pattern != "*"
            else f"{remote_dataset_path.rstrip('/')}/*"
        )
        files = list(self.list_files(pattern, recursive=True))

        # Download each file, preserving directory structure
        for file_path in files:
            # Calculate relative path from dataset root
            if remote_dataset_path.rstrip("/"):
                rel_path = Path(file_path).relative_to(remote_dataset_path.rstrip("/"))
            else:
                rel_path = Path(file_path)
            local_file_path = local_dataset_path / rel_path

            # Ensure parent directory exists
            local_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Download file
            self.download(file_path, local_file_path)
            downloaded_files.append(local_file_path)

        return downloaded_files

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
class LocalFileOptions(Options):
    """Options for local file backend."""

    pass


# Keep legacy name for backward compatibility
LocalFileConfig = LocalFileOptions


class LocalFileBackend(StorageBackend):
    """Local filesystem storage backend."""

    # Declare the options class for this backend
    options_class = LocalFileOptions

    def __init__(self, uri: str, options: Options):
        super().__init__(uri, options)

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


class OptionsRegistry:
    """Registry for mapping URI schemes to Options instances."""
    
    def __init__(self):
        self._scheme_options: t.Dict[str, Options] = {}
    
    def register_options(self, options: Options) -> None:
        """Register options for its supported scheme."""
        # Check if options has a scheme attribute
        if hasattr(options, 'scheme') and options.scheme:
            self._scheme_options[options.scheme.lower()] = options
    
    def get_options_for_scheme(self, scheme: str) -> t.Optional[Options]:
        """Get options instance for a URI scheme."""
        return self._scheme_options.get(scheme.lower())
    
    def list_registered_schemes(self) -> t.List[str]:
        """List all registered schemes."""
        return list(self._scheme_options.keys())


class BackendRegistry:
    """Registry for managing storage backends through options only."""

    def get_backend_class_for_options(self, options: Options) -> t.Optional[t.Type[StorageBackend]]:
        """Get backend class based on options backend_class attribute."""
        return options.backend_class


# Global backend registry
_backend_registry = BackendRegistry()


class DataHandler(ABC):
    """Abstract base class for all data handlers with dataset support."""

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

    def can_handle_dataset(self, path: Path) -> bool:
        """Check if this handler can handle datasets in the given path."""
        # Default implementation: check if any files match our extensions
        for ext in self.extensions:
            if any(path.glob(f"*{ext}")) or any(path.rglob(f"*{ext}")):
                return True
        return False

    def read_dataset(self, dataset_path: Path, file_pattern: str = "*") -> t.Any:
        """Read data from a dataset (multiple files). Override for custom behavior."""
        # Default implementation: find all matching files and read the first one
        # Subclasses should override for proper dataset handling
        for ext in self.extensions:
            pattern = f"*{ext}" if file_pattern == "*" else file_pattern
            files = list(dataset_path.glob(pattern)) + list(dataset_path.rglob(pattern))
            if files:
                return self.read_from_file(files[0], ext)
        raise ValueError(f"No compatible files found in dataset: {dataset_path}")

    def write_dataset(self, data: t.Any, dataset_path: Path, partition_strategy: str = "auto") -> None:
        """Write data as a dataset (potentially multiple files). Override for custom behavior."""
        # Default implementation: write as single file
        dataset_path.mkdir(parents=True, exist_ok=True)
        ext = self.extensions[0] if self.extensions else ".dat"
        file_path = dataset_path / f"data{ext}"
        self.write_to_file(data, file_path, ext)


class ParquetHandler(DataHandler):
    """Handler for Parquet files using Polars with dataset support."""

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

    def can_handle_dataset(self, path: Path) -> bool:
        """Check if path contains parquet files."""
        return any(path.glob("*.parquet")) or any(path.rglob("*.parquet"))

    def read_dataset(self, dataset_path: Path, file_pattern: str = "**/*.parquet") -> t.Any:
        """Read parquet dataset as LazyFrame."""
        try:
            import polars as pl

            return pl.scan_parquet(str(dataset_path / file_pattern))
        except ImportError:
            raise ImportError("Polars is required to load .parquet datasets")

    def write_dataset(self, data: t.Any, dataset_path: Path, partition_strategy: str = "auto") -> None:
        """Write LazyFrame as partitioned parquet dataset."""
        try:
            import polars as pl

            if isinstance(data, pl.LazyFrame):
                # For datasets, keep as LazyFrame and use sink_parquet
                dataset_path.mkdir(parents=True, exist_ok=True)

                if partition_strategy == "auto":
                    # Simple heuristic: if data looks large, try to partition
                    try:
                        # Check if we can determine row count efficiently
                        data.sink_parquet(str(dataset_path / "data.parquet"))
                    except Exception:
                        # Fallback to collecting and writing
                        df = data.collect()
                        df.write_parquet(dataset_path / "data.parquet")
                else:
                    # Single file for other strategies
                    data.sink_parquet(str(dataset_path / "data.parquet"))
            else:
                # Fallback to single file write
                self.write_to_file(data, dataset_path / "data.parquet", ".parquet")
        except ImportError:
            raise ImportError("Polars is required to save .parquet datasets")

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


class StorePath:
    """
    Path-like object for AutoStore that supports the / operator using pathlib.Path.
    """

    def __init__(self, store: "AutoStore", path: t.Union[str, Path] = ""):
        self.store = store
        # Use pathlib.Path for proper path handling, normalize separators
        if isinstance(path, str):
            # Replace backslashes with forward slashes and strip leading/trailing slashes
            path = path.replace("\\", "/").strip("/")
        self._path = Path(path) if path else Path()

    @property
    def path(self) -> str:
        """Return the path as a string with forward slashes for backend compatibility."""
        # Convert to POSIX-style path for backend compatibility (works for S3, local, etc.)
        return str(self._path.as_posix()) if str(self._path) != "." else ""

    def __truediv__(self, other: t.Union[str, Path]) -> "StorePath":
        """Support store / "path" / "file.ext" syntax using pathlib.Path."""
        new_path = self._path / other
        return StorePath(self.store, new_path)

    def __getitem__(self, key: str) -> t.Any:
        """Load data from this path + key."""
        full_key = self._get_full_key(key)
        return self.store[full_key]

    def __setitem__(self, key: str, value: t.Any) -> None:
        """Save data to this path + key."""
        full_key = self._get_full_key(key)
        self.store[full_key] = value

    def __contains__(self, key: str) -> bool:
        """Check if key exists at this path."""
        full_key = self._get_full_key(key)
        return full_key in self.store

    def __delitem__(self, key: str) -> None:
        """Delete key at this path."""
        full_key = self._get_full_key(key)
        del self.store[full_key]

    def _get_full_key(self, key: str) -> str:
        """Get the full key by combining path and key."""
        if not key:
            return self.path

        if self.path:
            # Use pathlib to join paths properly
            full_path = self._path / key
            return full_path.as_posix()
        else:
            return key

    def exists(self, key: str = "") -> bool:
        """Check if path or path + key exists."""
        full_key = self._get_full_key(key)
        return self.store.exists(full_key) if full_key else True

    def list_files(self, pattern: str = "*", recursive: bool = True) -> t.Iterator[str]:
        """List files at this path matching pattern."""
        if self.path:
            # Use pathlib to construct the pattern
            full_pattern = (self._path / pattern).as_posix()
        else:
            full_pattern = pattern

        for file_path in self.store.list_files(full_pattern, recursive):
            # Return relative paths from this StorePath
            if self.path:
                file_posix_path = Path(file_path).as_posix()
                base_posix_path = self._path.as_posix()
                if file_posix_path.startswith(base_posix_path + "/"):
                    yield file_posix_path[len(base_posix_path) + 1 :]
            else:
                yield file_path

    def get_metadata(self, key: str = "") -> FileMetadata:
        """Get metadata for this path or path + key."""
        full_key = self._get_full_key(key)
        return self.store.get_metadata(full_key)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"StorePath('{self.path}')"


class AutoStore:
    """
    Read and write files like a dictionary with automatic backend detection,
    dataset support, and cross-backend access capabilities.
    """

    @classmethod
    def _get_backend_module_for_options(cls, options: Options) -> t.Optional[str]:
        """Get the backend module path for an options instance."""
        # Map options classes to their backend modules
        options_to_module = {
            'S3Options': 'autostore.s3',
            # Future: add other backend options here
            # 'GCSOptions': 'autostore.gcs',
            # 'AzureOptions': 'autostore.azure',
        }
        
        options_class_name = options.__class__.__name__
        return options_to_module.get(options_class_name)


    def __init__(self, storage_uri: t.Union[str, Path], options: t.Union[Options, t.List[Options], None] = None, **kwargs):
        """
        Initialize AutoStore with automatic backend detection.

        Args:
            storage_uri: Storage URI (s3://bucket/path, conductor://bucket/path, etc.)
            options: Backend-specific options instance, or list of options for multiple schemes
            **kwargs: Backend-specific options as keyword arguments
        """
        # Handle Path objects
        if isinstance(storage_uri, Path):
            storage_uri = str(storage_uri)

        self.storage_uri = storage_uri

        # Initialize options registry for cross-backend access
        self._options_registry = OptionsRegistry()
        
        # Handle multiple options
        if isinstance(options, list):
            # Register each options instance for its schemes
            for opt in options:
                self._options_registry.register_options(opt)
            # Use first suitable option for primary backend or create default
            primary_options = self._get_primary_options(storage_uri, options, **kwargs)
        else:
            # Single options instance or None
            if options:
                self._options_registry.register_options(options)
            primary_options = options

        # Parse URI to determine backend
        parsed_uri = urlparse(storage_uri)
        scheme = parsed_uri.scheme.lower() if parsed_uri.scheme else ""

        # Auto-detect and load backend from options only
        try:
            backend_class = None
            
            # Determine backend from options type - this is the only way now
            if primary_options:
                backend_class = _backend_registry.get_backend_class_for_options(primary_options)
            
            # Special case: local file backend for empty scheme (built-in)
            if not backend_class and scheme in ("", "file"):
                backend_class = LocalFileBackend
            
            if not backend_class:
                available_options = [opt.__class__.__name__ for opt in options if hasattr(opt, 'backend_class')]
                raise UnsupportedSchemeError(
                    f"No backend available for scheme '{scheme}'. "
                    f"Available options classes: {available_options}. "
                    f"Make sure to provide options with backend_class set."
                )

            # Create or merge options
            resolved_options = self._create_backend_options(backend_class, primary_options, **kwargs)

            # Initialize primary backend
            self.primary_backend = backend_class(storage_uri, resolved_options)

        except BackendNotAvailableError:
            raise
        except Exception as e:
            raise StorageError(f"Failed to initialize {scheme} backend: {e}") from e

        # Cache for additional backends (cross-backend access)
        self._secondary_backends: t.Dict[str, StorageBackend] = {}

        # Global options that apply to all backends
        self._global_options = resolved_options

        self.handler_registry = HandlerRegistry()

    def _get_primary_options(self, storage_uri: str, options_list: t.List[Options], **kwargs) -> t.Optional[Options]:
        """Get appropriate options for the primary backend URI."""
        parsed_uri = urlparse(storage_uri)
        scheme = parsed_uri.scheme.lower() if parsed_uri.scheme else ""
        
        # Find options that match the URI scheme
        for opt in options_list:
            if hasattr(opt, 'scheme') and opt.scheme.lower() == scheme:
                return opt
        
        # If no matching scheme found, return None (will create default)
        return None

    def _create_backend_options(
        self, backend_class: t.Type[StorageBackend], options: t.Optional[Options], **kwargs
    ) -> Options:
        """Create appropriate options for backend."""
        # Get expected options class from backend
        options_class = getattr(backend_class, "options_class", Options)

        if options is None:
            # Create options from kwargs
            try:
                return options_class(**kwargs)
            except TypeError as e:
                valid_fields = [f.name for f in fields(options_class)]
                invalid_kwargs = [k for k in kwargs.keys() if k not in valid_fields]
                if invalid_kwargs:
                    raise BackendConfigurationError(
                        f"Invalid options for {backend_class.__name__}: {invalid_kwargs}. Valid options: {valid_fields}"
                    ) from e
                raise
        elif kwargs:
            # Merge kwargs into existing options
            options_dict = asdict(options)
            options_dict.update(kwargs)
            try:
                return options_class(**options_dict)
            except TypeError as e:
                valid_fields = [f.name for f in fields(options_class)]
                invalid_kwargs = [k for k in kwargs.keys() if k not in valid_fields]
                if invalid_kwargs:
                    raise BackendConfigurationError(
                        f"Invalid options for {backend_class.__name__}: {invalid_kwargs}. Valid options: {valid_fields}"
                    ) from e
                raise
        else:
            return options


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
        else:
            # If the extension is unsupported, check if it was stored as pickle
            ext = Path(key).suffix.lower()
            if not self.handler_registry.get_handler_for_extension(ext):
                # Try with .pkl extension (fallback handler)
                pkl_key = str(Path(key).with_suffix(".pkl"))
                if self.backend.exists(pkl_key):
                    return pkl_key

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
        """Load data - supports cross-backend access via full URIs."""
        # Check if key contains a URI scheme
        parsed_key = urlparse(key)

        if parsed_key.scheme:
            # Cross-backend access - key is a full URI
            return self._load_from_uri(key)
        else:
            # Standard access - use primary backend
            return self._load_from_primary(key)

    def _load_from_uri(self, uri: str) -> t.Any:
        """Load data from any backend using full URI."""
        backend = self._get_backend_for_uri(uri)

        # Extract the path component for the backend
        parsed_uri = urlparse(uri)
        # Remove the scheme and netloc to get the relative path
        relative_path = parsed_uri.path.lstrip("/")
        
        # Parse query parameters for format specification
        format_override = None
        if parsed_uri.query:
            from urllib.parse import parse_qs
            query_params = parse_qs(parsed_uri.query)
            if 'format' in query_params:
                format_override = query_params['format'][0]

        # Use the backend's normal loading process
        # Special handling for parquet: check if single file or dataset
        if format_override == "parquet":
            # First check if it's definitely a dataset (multiple files)
            if backend.is_dataset(relative_path):
                return self._load_dataset_from_backend(relative_path, backend, format_override)
            else:
                # Try as single file first (more common for files without extensions)
                return self._load_file_from_backend(relative_path, backend, format_override)
        else:
            # Normal logic for other formats
            if backend.is_directory(relative_path):
                return self._load_dataset_from_backend(relative_path, backend, format_override)
            else:
                return self._load_file_from_backend(relative_path, backend, format_override)

    def _load_from_primary(self, key: str) -> t.Any:
        """Load data from primary backend."""
        try:
            # Find the actual storage path
            resolved_path = self._resolve_path(key)

            if self.primary_backend.is_directory(resolved_path):
                # Handle as dataset
                return self._load_dataset_from_backend(resolved_path, self.primary_backend)
            else:
                # Handle as single file
                return self._load_file_from_backend(resolved_path, self.primary_backend)

        except Exception as e:
            if isinstance(e, (StorageFileNotFoundError, ValueError)):
                raise
            raise StorageError(f"Failed to load {key}: {str(e)}") from e

    def _resolve_path(self, key: str) -> str:
        """Resolve key to actual storage path (file or directory)."""
        # Try exact key first
        if self.primary_backend.exists(key):
            return key

        # Try with supported extensions for files
        for ext in self.handler_registry.get_supported_extensions():
            test_key = key + ext
            if self.primary_backend.exists(test_key):
                return test_key

        # Try as directory path
        if self.primary_backend.is_directory(key):
            return key

        raise StorageFileNotFoundError(f"No file or dataset found for key: {key}")

    def _load_file_from_backend(self, file_path: str, backend: StorageBackend, format_override: t.Optional[str] = None) -> t.Any:
        """Load a single file from a backend."""
        # Get file extension to determine handler
        ext = Path(file_path).suffix.lower()
        
        # Use format override if provided and no extension
        if format_override and not ext:
            ext = f".{format_override.lstrip('.')}"
        
        handler = self.handler_registry.get_handler_for_extension(ext)

        if not handler:
            supported = ", ".join(self.handler_registry.get_supported_extensions())
            if format_override:
                raise ValueError(f"Unsupported format: {format_override}. Supported types: {supported}")
            else:
                raise ValueError(f"Unsupported file type: {ext}. Supported types: {supported}")

        # Download file (with caching)
        local_file_path = backend.download_with_cache(file_path)

        # Use handler to deserialize from file
        return handler.read_from_file(local_file_path, ext)

    def _load_dataset_from_backend(self, dataset_path: str, backend: StorageBackend, format_override: t.Optional[str] = None) -> t.Any:
        """Load data from a dataset."""
        # Download/cache entire dataset locally
        local_dataset_path = self._cache_dataset(dataset_path, backend, format_override)

        # Find appropriate handler for dataset
        handler = self._find_dataset_handler(local_dataset_path, format_override)
        if not handler:
            format_hint = f" (format: {format_override})" if format_override else ""
            raise ValueError(f"No handler found for dataset at: {dataset_path}{format_hint}")

        # Load using handler
        return handler.read_dataset(local_dataset_path)

    def _cache_dataset(self, dataset_path: str, backend: StorageBackend, format_override: t.Optional[str] = None) -> Path:
        """Cache entire dataset locally, preserving structure."""
        cache_manager = backend.cache_manager
        if not cache_manager:
            # No caching - use temp directory
            temp_dir = backend.get_temp_dir() / f"dataset_{hash_obj(dataset_path)}"
            temp_dir.mkdir(exist_ok=True)
            
            # Use appropriate file pattern for download
            file_pattern = "*"
            if format_override == "parquet":
                file_pattern = "*.parquet"
            
            backend.download_dataset(dataset_path, temp_dir, file_pattern)
            return temp_dir

        # For now, use temp directory (could be enhanced with proper dataset caching)
        temp_dir = cache_manager.get_temp_dir() / f"dataset_{hash_obj(dataset_path)}"
        temp_dir.mkdir(exist_ok=True)
        
        # Use appropriate file pattern for download
        file_pattern = "*"
        if format_override == "parquet":
            file_pattern = "*.parquet"
            
        backend.download_dataset(dataset_path, temp_dir, file_pattern)
        return temp_dir

    def _find_dataset_handler(self, dataset_path: Path, format_override: t.Optional[str] = None) -> t.Optional[DataHandler]:
        """Find handler that can process the dataset."""
        # If format override is provided, try to find handler for that format first
        if format_override:
            ext = f".{format_override.lstrip('.')}"
            handler = self.handler_registry.get_handler_for_extension(ext)
            if handler and hasattr(handler, "can_handle_dataset"):
                return handler
        
        # Default behavior: find handler that can handle the dataset
        for handler in self.handler_registry._handlers:
            if hasattr(handler, "can_handle_dataset") and handler.can_handle_dataset(dataset_path):
                return handler
        return None

    def __setitem__(self, key: str, data: t.Any) -> None:
        """Save data - supports cross-backend access via full URIs."""
        parsed_key = urlparse(key)

        if parsed_key.scheme:
            # Cross-backend access - key is a full URI
            self._save_to_uri(key, data)
        else:
            # Standard access - use primary backend
            self._save_to_primary(key, data)

    def _save_to_uri(self, uri: str, data: t.Any) -> None:
        """Save data to any backend using full URI."""
        backend = self._get_backend_for_uri(uri)

        # Extract the path component for the backend
        parsed_uri = urlparse(uri)
        relative_path = parsed_uri.path.lstrip("/")

        # Use the backend's normal saving process
        self._save_to_backend(relative_path, data, backend)

    def _save_to_primary(self, key: str, data: t.Any) -> None:
        """Save data to primary backend."""
        self._save_to_backend(key, data, self.primary_backend)

    def _save_to_backend(self, key: str, data: t.Any, backend: StorageBackend) -> None:
        """Save data to a specific backend."""
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
            if isinstance(backend, LocalFileBackend):
                final_path = backend._get_full_path(key)
                handler.write_to_file(data, final_path, ext)
            else:
                # For cloud storage - use temp file + upload
                temp_dir = backend.cache_manager.get_temp_dir() if backend.cache_manager else backend.get_temp_dir()
                temp_file = temp_dir / f"upload_{Path(key).name}"

                # Use handler to serialize data to file
                handler.write_to_file(data, temp_file, ext)

                # Upload file to backend
                backend.upload(temp_file, key)

        except Exception as e:
            raise StorageError(f"Failed to save data to {key}: {str(e)}") from e

    def _get_backend_for_uri(self, uri: str) -> StorageBackend:
        """Get or create backend for the given URI."""
        parsed_uri = urlparse(uri)
        scheme = parsed_uri.scheme.lower()

        # For bucket-based backends (like S3), create backend per bucket, not per file
        # Check if we already have a backend for this scheme + netloc
        backend_key = f"{scheme}://{parsed_uri.netloc}"

        if backend_key in self._secondary_backends:
            return self._secondary_backends[backend_key]

        # Create new backend for this URI
        # Find backend through registered options for this scheme
        registered_options = self._options_registry.get_options_for_scheme(scheme)
        if registered_options and registered_options.backend_class:
            backend_class = registered_options.backend_class
            backend_options = registered_options
            # For bucket-based backends, use only the bucket part of the URI
            backend_uri = f"{scheme}://{parsed_uri.netloc}/"
        # Special case: local file backend for empty scheme (built-in)
        elif scheme in ("", "file"):
            backend_class = LocalFileBackend
            backend_options = self._create_cross_backend_options(backend_class, uri)
            backend_uri = uri
        else:
            raise UnsupportedSchemeError(
                f"No backend available for scheme '{scheme}'. "
                f"Register options for this scheme using AutoStore with appropriate options."
            )

        # Initialize and cache the backend
        backend = backend_class(backend_uri, backend_options)
        self._secondary_backends[backend_key] = backend

        return backend

    def _create_cross_backend_options(self, backend_class: t.Type[StorageBackend], uri: str) -> Options:
        """Create options for cross-backend access."""
        parsed_uri = urlparse(uri)
        scheme = parsed_uri.scheme.lower()
        
        # First, check if we have registered options for this scheme
        registered_options = self._options_registry.get_options_for_scheme(scheme)
        if registered_options:
            # Use the registered options for this scheme
            return registered_options
        
        # Fallback to creating options from global options
        options_class = getattr(backend_class, "options_class", Options)

        # Start with global options as base
        base_options = asdict(self._global_options)

        # Add any URI-specific parameters (e.g., from query string)
        if parsed_uri.query:
            query_params = parse_qs(parsed_uri.query)
            # Convert query params to options
            for key, values in query_params.items():
                if values:  # Take first value if multiple
                    base_options[key] = values[0]

        # Create options instance, filtering out invalid fields
        try:
            valid_fields = {f.name for f in fields(options_class)}
            filtered_options = {k: v for k, v in base_options.items() if k in valid_fields}
            return options_class(**filtered_options)
        except Exception:
            # Fallback to base Options if backend-specific options fail
            return Options(**{k: v for k, v in base_options.items() if k in {f.name for f in fields(Options)}})

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the data store."""
        parsed_key = urlparse(key)

        if parsed_key.scheme:
            # Cross-backend access
            backend = self._get_backend_for_uri(key)
            relative_path = parsed_key.path.lstrip("/")
            return backend.exists(relative_path)
        else:
            # Primary backend access
            try:
                self._resolve_path(key)
                return True
            except StorageFileNotFoundError:
                return False

    def __delitem__(self, key: str) -> None:
        """Delete a file from the data store."""
        parsed_key = urlparse(key)

        if parsed_key.scheme:
            # Cross-backend access
            backend = self._get_backend_for_uri(key)
            relative_path = parsed_key.path.lstrip("/")
            try:
                backend.delete(relative_path)
            except Exception as e:
                raise StorageError(f"Failed to delete {key}: {str(e)}") from e
        else:
            # Primary backend access
            file_key = self._resolve_path(key)
            try:
                self.primary_backend.delete(file_key)
            except Exception as e:
                raise StorageError(f"Failed to delete {key}: {str(e)}") from e

    def keys(self) -> t.Iterator[str]:
        """Iterate over all available keys."""
        seen = set()
        supported_extensions = self.handler_registry.get_supported_extensions()

        for file_path in self.primary_backend.list_files():
            if Path(file_path).suffix.lower() in supported_extensions:
                file_path_no_ext = str(Path(file_path).with_suffix(""))

                if file_path_no_ext not in seen:
                    seen.add(file_path_no_ext)
                    yield file_path_no_ext

    def list_files(self, pattern: str = "*", recursive: bool = True) -> t.Iterator[str]:
        """List files matching a pattern."""
        supported_extensions = self.handler_registry.get_supported_extensions()

        for file_path in self.primary_backend.list_files(pattern, recursive):
            if Path(file_path).suffix.lower() in supported_extensions:
                yield file_path

    def get_metadata(self, key: str) -> FileMetadata:
        """Get metadata for a file."""
        parsed_key = urlparse(key)

        if parsed_key.scheme:
            # Cross-backend access
            backend = self._get_backend_for_uri(key)
            relative_path = parsed_key.path.lstrip("/")
            return backend.get_metadata(relative_path)
        else:
            # Primary backend access
            file_key = self._resolve_path(key)
            return self.primary_backend.get_metadata(file_key)

    def copy(self, src_key: str, dst_key: str) -> None:
        """Copy a file within the storage backend."""
        # For now, only support copying within primary backend
        src_file_key = self._resolve_path(src_key)

        # If dst_key has no extension, infer from source
        if not Path(dst_key).suffix:
            src_ext = Path(src_file_key).suffix
            dst_key = dst_key + src_ext

        try:
            self.primary_backend.copy(src_file_key, dst_key)
        except Exception as e:
            raise StorageError(f"Failed to copy {src_key} to {dst_key}: {str(e)}") from e

    def move(self, src_key: str, dst_key: str) -> None:
        """Move a file within the storage backend."""
        # For now, only support moving within primary backend
        src_file_key = self._resolve_path(src_key)

        # If dst_key has no extension, infer from source
        if not Path(dst_key).suffix:
            src_ext = Path(src_file_key).suffix
            dst_key = dst_key + src_ext

        try:
            self.primary_backend.move(src_file_key, dst_key)
        except Exception as e:
            raise StorageError(f"Failed to move {src_key} to {dst_key}: {str(e)}") from e

    def exists(self, key: str) -> bool:
        """Check if a file exists."""
        return key in self

    def get_size(self, key: str) -> int:
        """Get the size of a file in bytes."""
        return self.get_metadata(key).size

    def read(self, key: str, format: t.Optional[str] = None) -> t.Any:
        """Read data from storage with optional format specification.
        
        Args:
            key: The storage key/path to read from
            format: Optional format override (e.g., 'parquet', 'csv', 'json')
                   Useful when the file has no extension or you want to override detection
        
        Returns:
            The loaded data in appropriate format
            
        Example:
            # Read with auto-detection
            data = store.read("my_data")
            
            # Read with format override
            df = store.read("s3a://bucket/dataset", format="parquet")
        """
        if format:
            # Use format parameter by appending as query string
            if '?' in key:
                key += f"&format={format}"
            else:
                key += f"?format={format}"
        
        return self[key]
    
    def write(self, key: str, data: t.Any, format: t.Optional[str] = None) -> None:
        """Write data to storage with optional format specification.
        
        Args:
            key: The storage key/path to write to
            data: The data to save
            format: Optional format override (e.g., 'parquet', 'csv', 'json')
                   If not provided, format is inferred from data type
        
        Example:
            # Write with auto-detection
            store.write("my_data", df)
            
            # Write with format override
            store.write("my_data", df, format="parquet")
        """
        if format:
            # Ensure the key has the correct extension
            key_path = Path(key)
            if not key_path.suffix:
                key += f".{format.lstrip('.')}"
        
        self[key] = data

    def cleanup_cache(self) -> None:
        """Clean up expired cache entries."""
        if self.primary_backend.cache_manager:
            self.primary_backend.cache_manager.cleanup_expired()

    def __len__(self) -> int:
        """Return the number of files in the data store."""
        return sum(1 for _ in self.list_files("*"))

    def __repr__(self) -> str:
        return f"AutoStore(storage_uri='{self.storage_uri}', backend={self.primary_backend.__class__.__name__})"

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()

    def cleanup(self) -> None:
        """Clean up all backends and their resources."""
        # Clean up primary backend
        if hasattr(self, "primary_backend"):
            self.primary_backend.cleanup()

        # Clean up secondary backends
        for backend in self._secondary_backends.values():
            backend.cleanup()

        self._secondary_backends.clear()

    def invalidate_backend_cache(self, scheme: str) -> None:
        """Remove cached backend for a scheme to force recreation."""
        # Remove all backends matching the scheme
        to_remove = [key for key in self._secondary_backends.keys() if key.startswith(f"{scheme}://")]
        for key in to_remove:
            self._secondary_backends[key].cleanup()
            del self._secondary_backends[key]

    def list_active_backends(self) -> t.List[str]:
        """List all currently active backends."""
        backends = [f"primary: {self.primary_backend.uri}"]
        backends.extend(f"secondary: {uri}" for uri in self._secondary_backends.keys())
        return backends

    def invalidate_cache(self, key: str) -> None:
        """Invalidate cache for a specific key or URI."""
        parsed_key = urlparse(key)

        if parsed_key.scheme:
            # Cross-backend cache invalidation
            backend = self._get_backend_for_uri(key)
            if backend.cache_manager:
                # This would need to be implemented in the cache manager
                pass
        else:
            # Primary backend cache invalidation
            if self.primary_backend.cache_manager:
                # This would need to be implemented in the cache manager
                pass

    def __truediv__(self, path: str) -> StorePath:
        """Support store / "path" syntax to create a StorePath."""
        return StorePath(self, path)


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
                value = _single_quote_escapes.sub(lambda m: m.group()[1:], value)
            elif _double_quoted_value.match(value):
                value = _double_quoted_value.sub(r"\1", value)
                value = _double_quote_escapes.sub(_double_quote_escape, value)
            elif _unquoted_value.match(value):
                value = _unquoted_value.sub(r"\1", value)
                value = _double_quote_escapes.sub(_double_quote_escape, value)
                value = value.strip()
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
