import re
import json
import shutil
import logging
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

log = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Metadata for cached files and datasets."""

    created_time: datetime
    source_uri: str
    cache_type: str  # "single_file" or "dataset"
    etag: Optional[str] = None
    file_count: Optional[int] = None
    total_size: Optional[int] = None
    expires_at: Optional[datetime] = None
    files: Optional[list] = None


class CacheService:
    """Centralized caching service with structure-preserving paths."""

    def __init__(self, cache_dir: Optional[str] = None, expiry_hours: int = 24):
        """
        Initialize cache service.

        Args:
            cache_dir: Cache directory path. Defaults to system temp.
            expiry_hours: Cache expiry in hours. 0 = never expire.
        """
        self.cache_dir = Path(cache_dir or tempfile.gettempdir()) / "autostore_cache"
        self.expiry_hours = expiry_hours
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cached_file(self, backend_uri: str, file_path: str, ignore_cache: bool = False) -> Optional[Path]:
        """
        Get cached file with cache control.

        Args:
            backend_uri: Backend URI (e.g., s3://bucket/)
            file_path: File path within backend
            ignore_cache: If True, skip cache lookup

        Returns:
            Path to cached file if exists and valid, None otherwise
        """
        if ignore_cache:
            return None

        # Skip caching for local backends
        if self._is_local_backend(backend_uri):
            return None

        cache_file_path = self._get_cache_path(backend_uri, file_path)
        meta_file = cache_file_path.parent / f"{cache_file_path.name}.cache_meta"

        # Check if cache exists
        if not cache_file_path.exists() or not meta_file.exists():
            return None

        # Check expiration using local metadata only
        try:
            with open(meta_file, "r") as f:
                metadata = json.load(f)

            if self._is_expired_locally(metadata):
                return None

            return cache_file_path
        except Exception as e:
            log.debug(f"Cache metadata error for {file_path}: {e}")
            return None

    def cache_file(self, backend_uri: str, file_path: str, local_file_path: Path, metadata: dict) -> Path:
        """
        Cache file preserving folder structure.

        Args:
            backend_uri: Backend URI
            file_path: File path within backend
            local_file_path: Local file to cache
            metadata: File metadata dict

        Returns:
            Path to cached file
        """
        cache_file_path = self._get_cache_path(backend_uri, file_path)
        cache_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file to cache
        shutil.copy2(local_file_path, cache_file_path)

        # Create metadata
        cache_entry = CacheEntry(
            created_time=datetime.now(),
            source_uri=f"{backend_uri.rstrip('/')}/{file_path}",
            cache_type="single_file",
            etag=metadata.get("etag"),
            total_size=cache_file_path.stat().st_size,
            expires_at=self._calculate_expiry(),
            files=[
                {
                    "path": file_path,
                    "size": cache_file_path.stat().st_size,
                    "etag": metadata.get("etag"),
                    "modified_time": metadata.get("modified_time", datetime.now()).isoformat(),
                }
            ],
        )

        # Write metadata
        meta_file = cache_file_path.parent / f"{cache_file_path.name}.cache_meta"
        with open(meta_file, "w") as f:
            json.dump(asdict(cache_entry), f, indent=2, default=str)

        return cache_file_path

    def get_cached_dataset(self, backend_uri: str, dataset_path: str, ignore_cache: bool = False) -> Optional[Path]:
        """
        Get cached dataset with cache control.

        Args:
            backend_uri: Backend URI
            dataset_path: Dataset path within backend
            ignore_cache: If True, skip cache lookup

        Returns:
            Path to cached dataset directory if exists and valid, None otherwise
        """
        if ignore_cache:
            return None

        # Skip caching for local backends
        if self._is_local_backend(backend_uri):
            return None

        cache_dataset_path = self._get_cache_path(backend_uri, dataset_path)
        meta_file = cache_dataset_path / ".cache_meta"

        # Check if cache exists
        if not cache_dataset_path.exists() or not meta_file.exists():
            return None

        # Check expiration using local metadata only
        try:
            with open(meta_file, "r") as f:
                metadata = json.load(f)

            if self._is_expired_locally(metadata):
                return None

            return cache_dataset_path
        except Exception as e:
            log.debug(f"Cache metadata error for dataset {dataset_path}: {e}")
            return None

    def cache_dataset(self, backend_uri: str, dataset_path: str, local_dataset_path: Path, metadata: dict) -> Path:
        """
        Cache dataset preserving folder structure.

        Args:
            backend_uri: Backend URI
            dataset_path: Dataset path within backend
            local_dataset_path: Local dataset directory to cache
            metadata: Dataset metadata dict

        Returns:
            Path to cached dataset directory
        """
        cache_dataset_path = self._get_cache_path(backend_uri, dataset_path)
        cache_dataset_path.mkdir(parents=True, exist_ok=True)

        # Copy dataset to cache
        import shutil

        for item in local_dataset_path.rglob("*"):
            if item.is_file():
                rel_path = item.relative_to(local_dataset_path)
                dest_path = cache_dataset_path / rel_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, dest_path)

        # Create metadata
        files_info = []
        total_size = 0
        file_count = 0

        for item in cache_dataset_path.rglob("*"):
            if item.is_file() and item.name != ".cache_meta":
                rel_path = item.relative_to(cache_dataset_path)
                file_size = item.stat().st_size
                total_size += file_size
                file_count += 1
                files_info.append(
                    {
                        "path": str(rel_path),
                        "size": file_size,
                        "etag": None,  # Could be enhanced with individual file ETags
                        "modified_time": datetime.fromtimestamp(item.stat().st_mtime).isoformat(),
                    }
                )

        cache_entry = CacheEntry(
            created_time=datetime.now(),
            source_uri=f"{backend_uri.rstrip('/')}/{dataset_path}",
            cache_type="dataset",
            file_count=file_count,
            total_size=total_size,
            expires_at=self._calculate_expiry(),
            files=files_info,
        )

        # Write metadata
        meta_file = cache_dataset_path / ".cache_meta"
        with open(meta_file, "w") as f:
            json.dump(asdict(cache_entry), f, indent=2, default=str)

        return cache_dataset_path

    def invalidate_cache(self, backend_uri: str, path: str) -> None:
        """Remove specific cached item."""
        cache_path = self._get_cache_path(backend_uri, path)

        if cache_path.is_file():
            # Single file
            cache_path.unlink(missing_ok=True)
            meta_file = cache_path.parent / f"{cache_path.name}.cache_meta"
            meta_file.unlink(missing_ok=True)
        elif cache_path.is_dir():
            # Dataset
            import shutil

            shutil.rmtree(cache_path, ignore_errors=True)

    def cleanup_expired(self) -> None:
        """Remove expired cache entries."""
        if self.expiry_hours == 0:
            return  # Never expire

        for meta_file in self.cache_dir.rglob("*.cache_meta"):
            try:
                with open(meta_file, "r") as f:
                    metadata = json.load(f)

                if self._is_expired_locally(metadata):
                    # Remove the cached file/dataset
                    if metadata.get("cache_type") == "dataset":
                        cache_path = meta_file.parent
                        import shutil

                        shutil.rmtree(cache_path, ignore_errors=True)
                    else:
                        cache_file = meta_file.with_suffix("")
                        cache_file.unlink(missing_ok=True)
                        meta_file.unlink(missing_ok=True)
            except Exception as e:
                log.debug(f"Error cleaning up cache entry {meta_file}: {e}")

    def _get_cache_path(self, backend_uri: str, file_path: str) -> Path:
        """Generate cache path preserving folder structure with security."""
        scheme, bucket = self._parse_backend_uri(backend_uri)
        safe_file_path = self._sanitize_path(file_path)

        return self.cache_dir / "backends" / scheme / bucket / safe_file_path

    def _parse_backend_uri(self, backend_uri: str) -> Tuple[str, str]:
        """Parse backend URI into scheme and bucket/storage identifier with safety."""
        parsed = urlparse(backend_uri)
        scheme = self._slugify(parsed.scheme or "local")

        # For bucket-based storage (S3, etc.)
        if parsed.netloc:
            bucket = self._slugify(parsed.netloc)
            return scheme, bucket
        else:
            # Fallback for schemes without clear bucket
            return scheme, "storage"

    def _slugify(self, text: str) -> str:
        """Convert string to safe filesystem name."""
        # Convert to lowercase and replace unsafe characters
        safe_text = re.sub(r"[^a-zA-Z0-9\-_]", "_", text.lower())

        # Remove multiple consecutive underscores
        safe_text = re.sub(r"_+", "_", safe_text)

        # Remove leading/trailing underscores
        safe_text = safe_text.strip("_")

        # Ensure non-empty result
        if not safe_text:
            safe_text = "unknown"

        # Limit length to prevent filesystem issues
        if len(safe_text) > 50:
            safe_text = safe_text[:50].rstrip("_")

        return safe_text

    def _sanitize_path(self, file_path: str) -> str:
        """Sanitize file path to prevent directory traversal and illegal characters."""
        # Normalize path separators
        normalized = file_path.replace("\\", "/")

        # Remove any attempts at directory traversal
        normalized = normalized.replace("..", "")

        # Split into parts and sanitize each part
        parts = [part for part in normalized.split("/") if part]
        safe_parts = []

        for part in parts:
            # Skip empty parts and current directory references
            if not part or part == ".":
                continue

            # Slugify each path component for safety
            safe_part = self._slugify(part)
            if safe_part:  # Only add non-empty parts
                safe_parts.append(safe_part)

        # Reconstruct path
        return "/".join(safe_parts) if safe_parts else "unknown"

    def _is_local_backend(self, backend_uri: str) -> bool:
        """Check if backend is local (no caching needed)."""
        parsed = urlparse(backend_uri)
        return parsed.scheme in ("", "file") or backend_uri.startswith("./")

    def _calculate_expiry(self) -> Optional[datetime]:
        """Calculate expiration time based on expiry_hours setting."""
        if self.expiry_hours == 0:
            return None  # Never expire
        return datetime.now() + timedelta(hours=self.expiry_hours)

    def _is_expired_locally(self, metadata: dict) -> bool:
        """Check if cache entry is expired using local metadata only."""
        if self.expiry_hours == 0:
            return False  # Never expire

        expires_at_str = metadata.get("expires_at")
        if not expires_at_str:
            return False  # No expiration set

        try:
            expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
            return datetime.now() > expires_at
        except (ValueError, TypeError):
            return True  # Invalid expiration, consider expired
