import json
import logging
import tempfile
from pathlib import Path
from autostore.cache import CacheService
from urllib.parse import urlparse, parse_qs
from autostore.backends import get_backend_class
from typing import Any, Optional, Tuple, Union, List
from autostore.handlers import create_default_registry
from autostore.types import Options, FormatNotSupportedError


log = logging.getLogger(__name__)


class AutoStore:
    """
    Simplified AutoStore with automatic backend detection and query parameter support.
    """

    def __init__(self, storage_uri: str, options: Union[Options, List[Options], None] = None):
        """
        Initialize AutoStore with automatic backend detection.

        Args:
            storage_uri: Storage URI (s3://bucket/path, ./local/path, etc.)
            options: Backend-specific options. Can be:
                    - None: Creates default options based on URI scheme
                    - Single Options object: Used for primary backend
                    - List of Options: Registers multiple options for cross-backend access
        """
        self.storage_uri = storage_uri
        self._parse_storage_uri()

        # Handle multiple options or single options
        if isinstance(options, list):
            # Multiple options - register all and find primary
            self._options_registry = self._create_options_registry(options)
            primary_options = self._get_primary_options_from_list(options)
        else:
            # Single options or None
            self._options_registry = {}
            primary_options = options

        # Create cache service if caching enabled
        self.cache_service = None
        cache_options = primary_options or self._get_cache_options_from_registry()
        if cache_options and cache_options.cache_enabled:
            self.cache_service = CacheService(
                cache_dir=cache_options.cache_dir, expiry_hours=cache_options.cache_expiry_hours
            )

        # Determine backend class and options
        backend_class = get_backend_class(self.scheme, primary_options)

        # Use provided options or create default
        if primary_options is None:
            primary_options = self._create_default_options(backend_class)

        # Initialize primary backend
        self.primary_backend = backend_class(storage_uri, primary_options, self.cache_service)
        self.options = primary_options

        # Initialize handler registry
        self.handler_registry = create_default_registry()

        # Cache for cross-backend access
        self._secondary_backends = {}

    def _parse_storage_uri(self):
        """Parse storage URI to extract scheme and components."""
        parsed = urlparse(self.storage_uri)
        self.scheme = parsed.scheme.lower() if parsed.scheme else ""
        self.netloc = parsed.netloc
        self.path = parsed.path

    def _create_default_options(self, backend_class) -> Options:
        """Create default options for backend."""
        if hasattr(backend_class, "__name__") and "S3" in backend_class.__name__:
            # Import S3Options here to avoid circular imports
            from autostore.backends.s3 import S3Options

            return S3Options()
        else:
            return Options()

    def _create_options_registry(self, options_list: List[Options]) -> dict:
        """Create a registry mapping schemes to options."""
        registry = {}
        for opt in options_list:
            if hasattr(opt, "scheme"):
                registry[opt.scheme] = opt
            else:
                # Fallback for options without scheme
                registry["default"] = opt
        return registry

    def _get_primary_options_from_list(self, options_list: List[Options]) -> Optional[Options]:
        """Get appropriate options for the primary backend URI."""
        # First try to find options matching the primary URI scheme
        for opt in options_list:
            if hasattr(opt, "scheme") and opt.scheme == self.scheme:
                return opt

        # For local schemes ("" or "file"), don't use S3 options
        if self.scheme in ("", "file"):
            # Look for non-S3 options or return None to use defaults
            for opt in options_list:
                if not hasattr(opt, "scheme") or opt.scheme in ("", "file"):
                    return opt
            # If all options are S3-specific, return None to use default local options
            return None

        # For non-local schemes, use the first options as fallback
        return options_list[0] if options_list else None

    def _get_cache_options_from_registry(self) -> Optional[Options]:
        """Get cache options from registry if no primary options."""
        if not self._options_registry:
            return None

        # Return any options with caching enabled
        for opt in self._options_registry.values():
            if opt.cache_enabled:
                return opt

        # Return first options as fallback
        return next(iter(self._options_registry.values()), None)

    def _parse_uri_parameters(self, key: str) -> Tuple[str, dict]:
        """Parse URI and extract query parameters."""
        parsed = urlparse(key)
        query_params = parse_qs(parsed.query) if parsed.query else {}

        # Extract cache control
        ignore_cache = "ignore_cache" in query_params

        # Extract format override
        format_override = None
        if "format" in query_params:
            format_override = query_params["format"][0]

        # Reconstruct clean URI without query parameters
        clean_uri = f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.scheme else parsed.path

        return clean_uri, {"ignore_cache": ignore_cache, "format": format_override}

    def __getitem__(self, key: str) -> Any:
        """Load data with query parameter support for format and cache control."""
        # Parse URI and extract parameters
        clean_uri, params = self._parse_uri_parameters(key)

        # Extract cache control and format
        ignore_cache = params.get("ignore_cache", False)
        format_override = params.get("format")

        # Route to appropriate backend
        parsed_clean = urlparse(clean_uri)
        if parsed_clean.scheme:
            return self._load_from_uri(clean_uri, format_override, ignore_cache)
        else:
            return self._load_from_primary(clean_uri, format_override, ignore_cache)

    def __setitem__(self, key: str, data: Any) -> None:
        """Save data with automatic format detection."""
        # Parse URI and extract parameters (ignore cache control for writes)
        clean_uri, params = self._parse_uri_parameters(key)
        format_override = params.get("format")

        # Route to appropriate backend
        parsed_clean = urlparse(clean_uri)
        if parsed_clean.scheme:
            self._save_to_uri(clean_uri, data, format_override)
        else:
            self._save_to_primary(clean_uri, data, format_override)

    def __contains__(self, key: str) -> bool:
        """Check if key exists."""
        clean_uri, _ = self._parse_uri_parameters(key)

        parsed_clean = urlparse(clean_uri)
        if parsed_clean.scheme:
            backend = self._get_backend_for_uri(clean_uri)
            relative_path = parsed_clean.path.lstrip("/")
            return backend.exists(relative_path)
        else:
            return self.primary_backend.exists(clean_uri)

    def __delitem__(self, key: str) -> None:
        """Delete key."""
        clean_uri, _ = self._parse_uri_parameters(key)

        parsed_clean = urlparse(clean_uri)
        if parsed_clean.scheme:
            backend = self._get_backend_for_uri(clean_uri)
            relative_path = parsed_clean.path.lstrip("/")
            backend.delete(relative_path)
        else:
            self.primary_backend.delete(clean_uri)

    def read(self, key: str, format: Optional[str] = None, ignore_cache: bool = False) -> Any:
        """
        Read data from storage with optional format specification and cache control.

        Args:
            key: The storage key/path to read from
            format: Optional format override (e.g., 'parquet', 'csv', 'json')
            ignore_cache: If True, forces fresh download from source, bypassing cache

        Returns:
            The loaded data in appropriate format
        """
        parsed_key = urlparse(key)
        if parsed_key.scheme:
            return self._load_from_uri(key, format, ignore_cache)
        else:
            return self._load_from_primary(key, format, ignore_cache)

    def write(self, key: str, data: Any, format: Optional[str] = None) -> None:
        """Write data to storage with optional format specification."""
        parsed_key = urlparse(key)
        if parsed_key.scheme:
            self._save_to_uri(key, data, format)
        else:
            self._save_to_primary(key, data, format)

    def _load_from_primary(self, key: str, format_override: Optional[str] = None, ignore_cache: bool = False) -> Any:
        """Load data from primary backend."""
        # Try to determine if it's a dataset, fallback to file if check fails
        try:
            if self.primary_backend.is_dataset(key):
                return self._load_dataset_from_backend(key, self.primary_backend, format_override, ignore_cache)
        except Exception:
            # If dataset check fails (e.g., connection issues), treat as single file
            pass

        return self._load_file_from_backend(key, self.primary_backend, format_override, ignore_cache)

    def _load_from_uri(self, uri: str, format_override: Optional[str] = None, ignore_cache: bool = False) -> Any:
        """Load data from any backend using full URI."""
        backend = self._get_backend_for_uri(uri)

        # Let the backend handle the URI parsing - each backend knows how to extract its own paths
        # For cross-backend access, just pass the relative path after the netloc
        parsed_uri = urlparse(uri)
        relative_path = parsed_uri.path.lstrip("/")

        # Try to determine if it's a dataset, fallback to file if check fails
        try:
            if backend.is_dataset(relative_path):
                return self._load_dataset_from_backend(relative_path, backend, format_override, ignore_cache)
        except Exception:
            # If dataset check fails (e.g., connection issues), treat as single file
            pass

        return self._load_file_from_backend(relative_path, backend, format_override, ignore_cache)

    def _load_file_from_backend(
        self, file_path: str, backend, format_override: Optional[str] = None, ignore_cache: bool = False
    ) -> Any:
        """Load single file from backend with cache control."""
        # Download file with cache control
        local_file_path = backend.download_with_cache(file_path, ignore_cache)

        # Get appropriate handler
        handler = self.handler_registry.get_handler_for_file(file_path, format_override)
        if not handler:
            raise FormatNotSupportedError(f"No handler found for file: {file_path}")

        # Load data
        file_extension = Path(file_path).suffix if not format_override else f".{format_override.lstrip('.')}"
        return handler.read_from_file(local_file_path, file_extension)

    def _load_dataset_from_backend(
        self, dataset_path: str, backend, format_override: Optional[str] = None, ignore_cache: bool = False
    ) -> Any:
        """Load dataset from backend with cache control."""
        # Download dataset with cache control
        local_dataset_path = backend.download_dataset_with_cache(dataset_path, ignore_cache)

        # Get appropriate handler
        handler = self.handler_registry.get_handler_for_file(dataset_path, format_override)
        if not handler:
            raise FormatNotSupportedError(f"No handler found for dataset: {dataset_path}")

        # Load dataset
        return handler.read_dataset(local_dataset_path)

    def _save_to_primary(self, key: str, data: Any, format_override: Optional[str] = None) -> None:
        """Save data to primary backend."""
        self._save_file_to_backend(key, data, self.primary_backend, format_override)

    def _save_to_uri(self, uri: str, data: Any, format_override: Optional[str] = None) -> None:
        """Save data to any backend using full URI."""
        backend = self._get_backend_for_uri(uri)

        # Let the backend handle the URI parsing - each backend knows how to extract its own paths
        # For cross-backend access, just pass the relative path after the netloc
        parsed_uri = urlparse(uri)
        relative_path = parsed_uri.path.lstrip("/")

        self._save_file_to_backend(relative_path, data, backend, format_override)

    def _save_file_to_backend(self, file_path: str, data: Any, backend, format_override: Optional[str] = None) -> None:
        """Save data to backend."""
        # Get appropriate handler
        if format_override:
            ext = f".{format_override.lstrip('.')}"
            handler = self.handler_registry.get_handler_for_extension(ext)
        else:
            handler = self.handler_registry.get_handler_for_data(data)

        if not handler:
            raise FormatNotSupportedError(f"No handler found for data type: {type(data)}")

        # Create temp file
        temp_dir = Path(tempfile.mkdtemp(prefix="autostore_upload_"))
        file_extension = Path(file_path).suffix if not format_override else f".{format_override.lstrip('.')}"
        temp_file = temp_dir / f"upload{file_extension}"

        try:
            # Write data to temp file
            handler.write_to_file(data, temp_file, file_extension)

            # Upload to backend
            backend.upload(temp_file, file_path)
        finally:
            # Cleanup temp file
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def _get_backend_for_uri(self, uri: str):
        """Get backend for URI, creating if needed."""
        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()

        # Check if we already have this backend
        backend_key = f"{scheme}://{parsed.netloc}"
        if backend_key in self._secondary_backends:
            return self._secondary_backends[backend_key]

        # Look for options in registry first
        options = None
        if scheme in self._options_registry:
            options = self._options_registry[scheme]
        elif "default" in self._options_registry:
            options = self._options_registry["default"]

        # Create default options if none found in registry
        if options is None:
            backend_class = get_backend_class(scheme)
            options = self._create_default_options(backend_class)
            # Set the scheme for the options
            if hasattr(options, "scheme"):
                options.scheme = scheme

        # Clone options to avoid modifying the registry
        if hasattr(options, "__dict__"):
            # Simple clone for dataclass-style options
            import copy

            options = copy.deepcopy(options)

        # Add cache service if available
        if self.cache_service:
            options.cache_enabled = True
            if not hasattr(options, "cache_dir") or not options.cache_dir:
                options.cache_dir = self.cache_service.cache_dir
            if not hasattr(options, "cache_expiry_hours"):
                options.cache_expiry_hours = self.cache_service.expiry_hours

        # Ensure scheme matches for S3Options
        if hasattr(options, "scheme"):
            options.scheme = scheme

        backend_class = get_backend_class(scheme, options)
        # For cross-backend access, create backend with just scheme://netloc (no path)
        # This ensures each backend handles individual paths without a fixed prefix
        backend_uri = f"{scheme}://{parsed.netloc}"
        backend = backend_class(backend_uri, options, self.cache_service)
        self._secondary_backends[backend_key] = backend

        return backend

    def invalidate_cache(self, key: str) -> None:
        """Remove specific cached item."""
        if not self.cache_service:
            return

        clean_uri, _ = self._parse_uri_parameters(key)
        parsed_clean = urlparse(clean_uri)

        if parsed_clean.scheme:
            backend_uri = f"{parsed_clean.scheme}://{parsed_clean.netloc}"
            relative_path = parsed_clean.path.lstrip("/")
            self.cache_service.invalidate_cache(backend_uri, relative_path)
        else:
            self.cache_service.invalidate_cache(self.storage_uri, clean_uri)

    def cleanup_expired_cache(self) -> None:
        """Remove expired cache entries."""
        if self.cache_service:
            self.cache_service.cleanup_expired()

    def list_files(self, pattern: str = "*", recursive: bool = True):
        """List files in primary backend."""
        return list(self.primary_backend.list_files(pattern, recursive))

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        return key in self

    def keys(self):
        """List all keys in primary backend."""
        return self.list_files()

    def cleanup(self):
        """Clean up resources."""
        self.primary_backend.cleanup()
        for backend in self._secondary_backends.values():
            backend.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _ = exc_type, exc_val, exc_tb  # Standard context manager signature
        self.cleanup()


def hash_obj(obj: str, seed: int = 123) -> str:
    """Generate a non-cryptographic hash from a string."""
    import hashlib

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
