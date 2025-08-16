import logging
import tempfile
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)


class StorageBackend(ABC):
    """
    Abstract base class for all storage backends.

    Uses upload/download operations with temporary files and centralized caching.
    """

    def __init__(self, uri: str, options, cache_service=None):
        """
        Initialize the storage backend.

        Args:
            uri: Storage URI (e.g., 'file:///path', 's3://bucket/prefix')
            options: Backend options
            cache_service: Optional CacheService instance
        """
        self.uri = uri
        self.options = options
        self._parsed_uri = urlparse(uri)
        self._temp_dir = None
        self.cache_service = cache_service

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
    def list_files(self, pattern: str = "*", recursive: bool = True) -> Iterator[str]:
        """List files matching a pattern."""
        pass

    @abstractmethod
    def get_metadata(self, path: str):
        """Get metadata for a file."""
        pass

    def download_with_cache(self, remote_path: str, ignore_cache: bool = False) -> Path:
        """
        Download file with cache control (remote backends only).

        Args:
            remote_path: Path to remote file
            ignore_cache: If True, bypass cache

        Returns:
            Path to local file (cached or temporary)
        """
        # For local backends, return direct path without caching
        if self.scheme in ("", "file"):
            temp_file = self.get_temp_dir() / f"temp_{Path(remote_path).name}"
            self.download(remote_path, temp_file)
            return temp_file

        # Use cache service if available
        if self.cache_service and not ignore_cache:
            cached_file = self.cache_service.get_cached_file(self.uri, remote_path, ignore_cache)
            if cached_file:
                log.debug(f"Cache hit for {remote_path}: {cached_file}")
                return cached_file

        log.debug(f"Cache miss for {remote_path}, downloading...")

        # Download to temp file
        temp_file = self.get_temp_dir() / f"download_{Path(remote_path).name}"
        self.download(remote_path, temp_file)

        # Cache the file if cache service available
        if self.cache_service and not ignore_cache:
            try:
                metadata = self.get_metadata(remote_path)
                metadata_dict = {
                    "etag": getattr(metadata, "etag", None),
                    "modified_time": getattr(metadata, "modified_time", None),
                    "size": getattr(metadata, "size", None),
                }
                cached_file = self.cache_service.cache_file(self.uri, remote_path, temp_file, metadata_dict)
                return cached_file
            except Exception as e:
                log.debug(f"Failed to cache file {remote_path}: {e}")

        return temp_file

    def download_dataset_with_cache(
        self, remote_dataset_path: str, ignore_cache: bool = False, file_pattern: str = "*"
    ) -> Path:
        """
        Download dataset with cache control.

        Args:
            remote_dataset_path: Path to remote dataset
            ignore_cache: If True, bypass cache
            file_pattern: Pattern to match files in dataset

        Returns:
            Path to local dataset directory (cached or temporary)
        """
        # For local backends, download to temp without caching
        if self.scheme in ("", "file"):
            local_dataset_path = self.get_temp_dir() / f"dataset_{Path(remote_dataset_path).name}"
            self.download_dataset(remote_dataset_path, local_dataset_path, file_pattern)
            return local_dataset_path

        # Use cache service if available
        if self.cache_service and not ignore_cache:
            cached_dataset = self.cache_service.get_cached_dataset(self.uri, remote_dataset_path, ignore_cache)
            if cached_dataset:
                log.debug(f"Dataset cache hit for {remote_dataset_path}: {cached_dataset}")
                return cached_dataset

        log.debug(f"Dataset cache miss for {remote_dataset_path}, downloading...")

        # Download to temp directory
        local_dataset_path = self.get_temp_dir() / f"dataset_{Path(remote_dataset_path).name}"
        self.download_dataset(remote_dataset_path, local_dataset_path, file_pattern)

        # Cache the dataset if cache service available
        if self.cache_service and not ignore_cache:
            try:
                metadata_dict = {"dataset_path": remote_dataset_path, "file_pattern": file_pattern}
                cached_dataset = self.cache_service.cache_dataset(
                    self.uri, remote_dataset_path, local_dataset_path, metadata_dict
                )
                return cached_dataset
            except Exception as e:
                log.debug(f"Failed to cache dataset {remote_dataset_path}: {e}")

        return local_dataset_path

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

    def download_dataset(self, remote_dataset_path: str, local_dataset_path: Path, file_pattern: str = "*") -> list:
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
        if self._temp_dir and self._temp_dir.exists():
            import shutil

            shutil.rmtree(self._temp_dir, ignore_errors=True)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(uri='{self.uri}')"
