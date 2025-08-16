from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class FileMetadata:
    """Metadata information for a file."""
    size: int
    modified_time: datetime
    created_time: Optional[datetime] = None
    content_type: Optional[str] = None
    etag: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Options:
    """Base options class for all storage backends."""
    cache_enabled: bool = False
    cache_dir: Optional[str] = None
    cache_expiry_hours: int = 24  # Set to 0 to never expire cache entries
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    
    @property
    def backend_class(self):
        """Return the backend class to use for this options instance."""
        from autostore.backends.local import LocalFileBackend
        return LocalFileBackend


# Exception classes
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
    """Raised when connection to storage fails."""
    pass


class BackendNotAvailableError(StorageError):
    """Raised when a required backend is not available."""
    pass


class UnsupportedSchemeError(StorageError):
    """Raised when URI scheme is not supported."""
    pass


class BackendConfigurationError(StorageError):
    """Raised when backend configuration is invalid."""
    pass


class InvalidParameterError(StorageError):
    """Raised when URI parameters are invalid."""
    pass


class FormatNotSupportedError(StorageError):
    """Raised when format override is not supported."""
    pass


class CacheError(StorageError):
    """Raised when cache operations fail."""
    pass