import os
import shutil
import logging
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime
from dataclasses import dataclass
from typing import Iterator, Optional
from autostore.backends.base import StorageBackend
from autostore.types import FileMetadata, StorageError, StorageFileNotFoundError, StoragePermissionError, Options

log = logging.getLogger(__name__)

# Content type mapping
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


@dataclass
class LocalFileOptions(Options):
    """Options for local file backend."""

    pass


class LocalFileBackend(StorageBackend):
    """Local filesystem storage backend."""

    def __init__(self, uri: str, options: Options, cache_service=None):
        super().__init__(uri, options, cache_service)

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

    def download_with_cache(self, remote_path: str, ignore_cache: bool = False) -> Path:
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

    def list_files(self, pattern: str = "*", recursive: bool = True) -> Iterator[str]:
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

    def _guess_content_type(self, extension: str) -> Optional[str]:
        """Guess content type from file extension."""
        extension = extension.lower()
        return CONTENT_TYPES.get(extension)
