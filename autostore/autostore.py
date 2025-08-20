import json
import zipfile
import logging
import tempfile
from pathlib import Path
from autostore.cache import CacheService
from urllib.parse import urlparse, parse_qs
from autostore.backends import get_backend_class
from autostore.handlers import create_default_registry
from autostore.types import Options, FormatNotSupportedError
from typing import Any, Optional, Tuple, Union, List, Iterator


log = logging.getLogger(__name__)


class AutoStore:
    """
    Simplified AutoStore with automatic backend detection and query parameter support.
    """

    def __init__(self, storage_uri: str, options: Union[Options, List[Options], None] = None):
        """
        Initialize AutoStore with automatic backend detection.

        Args:
            storage_uri: Storage URI (s3://bucket/path, ./local/path, etc.) is the primary identifier for the store.
            options: Backend-specific options. Can be None which creates default options based on URI scheme,
                single options object used for primary backend, or a list of options that registers multiple options
                for cross-backend access
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


# AutoPath does NOT implement storage operations directly. Instead it does the following:
# 1. Path Management: Handles path parsing, URI detection, and path operations (joining, parent/child relationships)
# 2. Backend Selection: Routes operations to the appropriate backend based on the URI scheme
# 3. Path-like Interface: Provides the familiar pathlib.Path-style API
# 4. Path Translation: Converts AutoPath operations into backend method calls
# Backend's Role (Actual Work):
# The backends (local.py, s3.py) handle all the real storage operations:
# LocalFileBackend (local.py):
# - exists() → Path.exists()
# - upload() → shutil.copy2()
# - download() → shutil.copy2()
# - delete() → Path.unlink() or shutil.rmtree()
# - list_files() → Path.glob() or Path.rglob()
# S3Backend (s3.py):
# - exists() → client.head_object() or client.list_objects_v2()
# - upload() → client.upload_file()
# - download() → client.download_file()
# - delete() → client.delete_object()
# - list_files() → client.get_paginator("list_objects_v2")
# Here's what actually happens when we call: data_path.read_text()
# 1. AutoPath determines which backend to use (self._get_backend())
# 2. AutoPath converts the path to backend format (self._get_relative_path())
# 3. AutoPath calls backend.download_with_cache(rel_path)
# 4. Backend does the actual work (S3 download or local file access)
# 5. AutoPath reads from the local file and returns the content
# AutoPath has ZERO knowledge of AWS APIs, boto3, file system calls, etc. All the storage-specific logic lives in the backends. AutoPath just:
# - Figures out which backend to use
# - Translates AutoPath operations → backend method calls
# - Provides a consistent interface regardless of backend
# This is a clean separation of concerns: AutoPath handles the "what" (path operations) while backends handle the "how" (actual storage implementation).


class AutoPath:
    """
    PathLike access to local and S3 storage.

    A Path-like interface that provides unified access to both local filesystem
    and S3 storage using AutoStore functionality. Supports all common Path operations
    with automatic backend detection and handling.

    Examples:
        ```python
        # Create a store for local and S3 storage
        store = AutoStore(
            str(DATA_DIR),
            options=[
                S3Options(
                    scheme="s3a",
                    profile_name=AWS_PROFILE,
                    cache_enabled=True,
                    cache_expiry_hours=0,  # Never expire
                    cache_dir=str(CACHE_DIR),
                ),
            ],
        )

        remote = AutoPath("s3a://bucket/prefix", store=store)
        v1 = remote / "v1"
        config_path = v1 / "config.txt"
        config_path.exists()  # True if the file exists
        config_path.read_text()  # Read the file content as text
        config_path.read_bytes()  # Read the file content as bytes
        config_path.write_text("New content")  # Write new content to the file
        config_path.write_bytes(b"New binary content")  # Write new binary content to the file
        config_path.delete()  # Delete the file
        list(config_path.parent.iterdir())  # List all files in parent directory
        config_path.exists()  # Check if the path exists
        config_path.is_dir()  # Check if the path is a directory
        config_path.is_file()  # Check if the path is a file
        config_path.mkdir()  # Create the directory
        config_path.rmdir()  # Remove the directory
        v1.glob("*.txt")  # List all .txt files in the S3 bucket/prefix
        config_path.copy_to("s3a://another-bucket/new-config.txt")  # Copy the file or directory to another location
        config_path.move_to("s3a://another-bucket/new-config.txt")  # Move the file or directory to another location
        config_path.upload_from("./local/file.txt")  # Upload a local file to this path
        config_path.download_to("./local/file.txt")  # Download this file to a local path

        # File-like operations (works with numpy, etc.)
        matrix_path = remote / "cooccurrence_matrix.npz"
        np.savez_compressed(matrix_path, data=matrix.data, indices=matrix.indices)  # Works directly!

        # Or with context manager for explicit control
        with matrix_path as f:
            np.savez_compressed(f, data=matrix.data, indices=matrix.indices)
        ```
    """

    def __init__(self, path: Union[str, "AutoPath", Path], store: Optional["AutoStore"] = None):
        """
        Initialize AutoPath.

        Args:
            path: Path string, AutoPath instance, or pathlib.Path
            store: AutoStore instance for storage operations
        """
        if isinstance(path, AutoPath):
            self._path_str = path._path_str
            self._store = store or path._store
        elif isinstance(path, Path):
            self._path_str = str(path)
            self._store = store
        else:
            self._path_str = str(path)
            self._store = store

        # Parse the path to determine if it's a URI
        self._parsed = urlparse(self._path_str)
        self._is_uri = bool(self._parsed.scheme)

        # Create default store if none provided
        if self._store is None:
            if self._is_uri:
                # For URIs, create a store with the base URI
                base_uri = f"{self._parsed.scheme}://{self._parsed.netloc}"
                self._store = AutoStore(base_uri)
            else:
                # For local paths, create a store with the parent directory
                parent_path = (
                    Path(self._path_str).parent if Path(self._path_str).parent != Path(self._path_str) else Path(".")
                )
                self._store = AutoStore(str(parent_path))

        # File-like object state for buffered writing
        self._write_buffer = bytearray()
        self._is_open = False

    @property
    def path_str(self) -> str:
        """Get the string representation of the path."""
        return self._path_str

    @property
    def store(self) -> "AutoStore":
        """Get the AutoStore instance."""
        return self._store

    def __fspath__(self) -> str:
        """Return string representation for os.PathLike compatibility."""
        return self._path_str

    def __str__(self) -> str:
        """String representation of the path."""
        return self._path_str

    def __repr__(self) -> str:
        """Detailed representation of the AutoPath."""
        return f"AutoPath('{self._path_str}')"

    def __truediv__(self, other: Union[str, "AutoPath", Path]) -> "AutoPath":
        """Join paths using the / operator."""
        if isinstance(other, (AutoPath, Path)):
            other = str(other)

        if self._is_uri:
            # For URIs, manually construct the path to avoid urljoin issues
            base_path = self._path_str.rstrip("/")
            other_path = other.lstrip("/")
            new_path = f"{base_path}/{other_path}"
        else:
            # For local paths, use regular path joining
            new_path = str(Path(self._path_str) / other)

        return AutoPath(new_path, self._store)

    def __eq__(self, other) -> bool:
        """Check equality with another AutoPath."""
        if not isinstance(other, AutoPath):
            return False
        return self._path_str == other._path_str

    def __hash__(self) -> int:
        """Hash for use in sets and dictionaries."""
        return hash(self._path_str)

    @property
    def name(self) -> str:
        """The final component of the path."""
        if self._is_uri:
            return Path(self._parsed.path).name
        return Path(self._path_str).name

    @property
    def stem(self) -> str:
        """The final component without its suffix."""
        if self._is_uri:
            return Path(self._parsed.path).stem
        return Path(self._path_str).stem

    @property
    def suffix(self) -> str:
        """The file extension of the final component."""
        if self._is_uri:
            return Path(self._parsed.path).suffix
        return Path(self._path_str).suffix

    @property
    def suffixes(self) -> List[str]:
        """A list of the path's suffixes."""
        if self._is_uri:
            return Path(self._parsed.path).suffixes
        return Path(self._path_str).suffixes

    @property
    def parent(self) -> "AutoPath":
        """The parent directory."""
        if self._is_uri:
            parsed_parent = Path(self._parsed.path).parent
            if str(parsed_parent) == ".":
                # At root level
                parent_path = f"{self._parsed.scheme}://{self._parsed.netloc}"
            else:
                parent_path = f"{self._parsed.scheme}://{self._parsed.netloc}{parsed_parent}"
        else:
            parent_path = str(Path(self._path_str).parent)

        return AutoPath(parent_path, self._store)

    @property
    def parents(self) -> List["AutoPath"]:
        """An immutable sequence providing access to the logical ancestors of the path."""
        parents = []
        current = self.parent
        while str(current) != str(current.parent):  # Avoid infinite loop at root
            parents.append(current)
            current = current.parent
        return parents

    def exists(self) -> bool:
        """Check if the path exists."""
        try:
            backend = self._get_backend()
            rel_path = self._get_relative_path()
            return backend.exists(rel_path)
        except Exception:
            return False

    def is_file(self) -> bool:
        """Check if the path is a file."""
        if not self.exists():
            return False
        try:
            # Use the backend's is_directory method if available
            backend = self._get_backend()
            rel_path = self._get_relative_path()
            return not backend.is_directory(rel_path)
        except Exception:
            return False

    def is_dir(self) -> bool:
        """Check if the path is a directory."""
        try:
            backend = self._get_backend()
            rel_path = self._get_relative_path()

            # For local backends, use direct directory check
            if hasattr(backend, "_get_full_path"):
                # This is LocalFileBackend
                full_path = backend._get_full_path(rel_path)
                return full_path.exists() and full_path.is_dir()
            else:
                # For S3-like backends, check if there are any files with this prefix
                # This is more efficient than exists() + is_directory()
                prefix_path = rel_path.rstrip("/") + "/" if rel_path else ""
                try:
                    # Try to find at least one file with this prefix
                    next(backend.list_files(f"{prefix_path}*", recursive=False))
                    return True
                except StopIteration:
                    return False
        except Exception:
            return False

    def read_text(self, encoding: str = "utf-8") -> str:
        """Read file content as text."""
        try:
            # Use the backend directly to avoid AutoStore's format detection
            backend = self._get_backend()
            rel_path = self._get_relative_path()

            # Download to a temporary location and read
            local_path = backend.download_with_cache(rel_path)
            return local_path.read_text(encoding=encoding)
        except Exception:
            raise FileNotFoundError(f"File not found: {self._path_str}")

    def read_bytes(self) -> bytes:
        """Read file content as bytes."""
        try:
            # Use the backend directly to avoid AutoStore's format detection
            backend = self._get_backend()
            rel_path = self._get_relative_path()

            # Download to a temporary location and read
            local_path = backend.download_with_cache(rel_path)
            return local_path.read_bytes()
        except Exception:
            raise FileNotFoundError(f"File not found: {self._path_str}")

    def write_text(self, data: str, encoding: str = "utf-8") -> None:
        """Write text content to file."""
        # Use backend directly to avoid AutoStore's format detection
        import tempfile

        backend = self._get_backend()
        rel_path = self._get_relative_path()

        # Write to a temporary file and upload
        with tempfile.NamedTemporaryFile(mode="w", encoding=encoding, delete=False) as temp_file:
            temp_file.write(data)
            temp_path = Path(temp_file.name)

        try:
            backend.upload(temp_path, rel_path)
        finally:
            temp_path.unlink(missing_ok=True)

    def write_bytes(self, data: bytes) -> None:
        """Write bytes content to file."""
        # Use backend directly to avoid AutoStore's format detection
        import tempfile

        backend = self._get_backend()
        rel_path = self._get_relative_path()

        # Write to a temporary file and upload
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
            temp_file.write(data)
            temp_path = Path(temp_file.name)

        try:
            backend.upload(temp_path, rel_path)
        finally:
            temp_path.unlink(missing_ok=True)

    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        """Create directory."""
        backend = self._get_backend()
        rel_path = self._get_relative_path()

        if not exist_ok and self.exists():
            raise FileExistsError(f"Directory already exists: {self._path_str}")

        if parents:
            # Create parent directories if needed
            for parent in reversed(self.parents):
                if not parent.exists():
                    parent.mkdir(exist_ok=True)

        # For local backend, create directory directly
        if hasattr(backend, "_get_full_path"):
            # This is LocalFileBackend - create real directory
            full_path = backend._get_full_path(rel_path)
            full_path.mkdir(parents=parents, exist_ok=exist_ok)
        # For remote backends (like S3), directories are implicit
        # They don't need to exist until files are placed in them

    def rmdir(self) -> None:
        """Remove empty directory."""
        backend = self._get_backend()
        rel_path = self._get_relative_path()

        if not self.is_dir():
            raise NotADirectoryError(f"Not a directory: {self._path_str}")

        # Check if directory is empty
        try:
            next(self.iterdir())
            raise OSError(f"Directory not empty: {self._path_str}")
        except StopIteration:
            pass  # Directory is empty

        # For local backend, remove directory directly
        if hasattr(backend, "_get_full_path"):
            # This is LocalFileBackend - remove real directory
            full_path = backend._get_full_path(rel_path)
            full_path.rmdir()
        # For remote backends (like S3), empty directories don't exist
        # so there's nothing to remove

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove file."""
        try:
            del self._store[self._get_relative_path()]
        except KeyError:
            if not missing_ok:
                raise FileNotFoundError(f"File not found: {self._path_str}")

    def iterdir(self) -> Iterator["AutoPath"]:
        """Iterate over directory contents."""
        backend = self._get_backend()
        rel_path = self._get_relative_path()

        # For local backend, use direct directory listing
        if hasattr(backend, "_get_full_path"):
            # This is LocalFileBackend
            try:
                full_path = backend._get_full_path(rel_path)
                if not full_path.is_dir():
                    raise NotADirectoryError(f"Not a directory: {self._path_str}")

                for item in full_path.iterdir():
                    # Convert back to AutoPath
                    rel_item_path = str(item.relative_to(backend.root_path))
                    if self._is_uri:
                        item_path = f"{self._parsed.scheme}://{self._parsed.netloc}/{rel_item_path}"
                    else:
                        item_path = str(item)
                    yield AutoPath(item_path, self._store)
            except Exception:
                return
        else:
            # For S3-like backends, list files directly without checking is_dir() first
            # This avoids the need for a separate exists check
            prefix_path = rel_path.rstrip("/") + "/" if rel_path else ""
            pattern = f"{prefix_path.rstrip('/')}/*"

            found_any = False
            try:
                for file_path in backend.list_files(pattern, recursive=False):
                    found_any = True
                    # Convert backend relative path back to full path
                    if self._is_uri:
                        # Ensure proper path construction without double slashes
                        clean_file_path = file_path.lstrip("/")
                        full_path = f"{self._parsed.scheme}://{self._parsed.netloc}/{clean_file_path}"
                    else:
                        full_path = str(Path(self._store.storage_uri) / file_path)
                    yield AutoPath(full_path, self._store)
            except Exception:
                pass

            # If we didn't find any files, check if this path represents a directory
            if not found_any and not self.is_dir():
                raise NotADirectoryError(f"Not a directory: {self._path_str}")

    def glob(self, pattern: str) -> Iterator["AutoPath"]:
        """Glob for files matching pattern."""
        base_path = self._get_relative_path()
        full_pattern = f"{base_path.rstrip('/')}/{pattern}" if base_path else pattern

        try:
            backend = self._get_backend()
            for file_path in backend.list_files(full_pattern, recursive=True):
                # Convert backend relative path back to full path
                if self._is_uri:
                    full_path = f"{self._parsed.scheme}://{self._parsed.netloc}/{file_path}"
                else:
                    full_path = str(Path(self._store.storage_uri) / file_path)
                yield AutoPath(full_path, self._store)
        except Exception:
            return

    def copy_to(self, destination: Union[str, "AutoPath", Path]) -> "AutoPath":
        """Copy file or directory to destination."""
        dest = AutoPath(destination) if not isinstance(destination, AutoPath) else destination

        if self.is_file():
            # Copy single file
            data = self.read_bytes()
            dest.write_bytes(data)
        elif self.is_dir():
            # Copy directory recursively
            dest.mkdir(parents=True, exist_ok=True)
            for item in self.iterdir():
                relative_path = item._get_relative_path().replace(self._get_relative_path().rstrip("/") + "/", "", 1)
                item.copy_to(dest / relative_path)
        else:
            raise FileNotFoundError(f"Source not found: {self._path_str}")

        return dest

    def move_to(self, destination: Union[str, "AutoPath", Path]) -> "AutoPath":
        """Move file or directory to destination."""
        dest = self.copy_to(destination)

        if self.is_file():
            self.unlink()
        elif self.is_dir():
            # Remove all files in directory
            for item in self.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    item.rmdir()
            self.rmdir()

        return dest

    def upload_from(self, source: Union[str, Path, "AutoPath"]) -> None:
        """Upload from local file/directory to this path."""
        if isinstance(source, str):
            source = Path(source)
        elif isinstance(source, AutoPath):
            # If source is also a AutoPath, use copy_to instead
            source.copy_to(self)
            return

        if source.is_file():
            # Upload single file
            data = source.read_bytes()
            self.write_bytes(data)
        elif source.is_dir():
            # Upload directory recursively
            self.mkdir(parents=True, exist_ok=True)
            for item in source.rglob("*"):
                if item.is_file():
                    relative_path = item.relative_to(source)
                    dest_path = self / str(relative_path)
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    dest_path.write_bytes(item.read_bytes())
        else:
            raise FileNotFoundError(f"Source not found: {source}")

    def download_to(self, destination: Union[str, Path]) -> Path:
        """Download this file/directory to local destination."""
        if isinstance(destination, str):
            destination = Path(destination)

        if self.is_file():
            # Download single file
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(self.read_bytes())
        elif self.is_dir():
            # Download directory recursively
            destination.mkdir(parents=True, exist_ok=True)
            for item in self.iterdir():
                relative_path = item._get_relative_path().replace(self._get_relative_path().rstrip("/") + "/", "", 1)
                dest_path = destination / relative_path
                item.download_to(dest_path)
        else:
            raise FileNotFoundError(f"Source not found: {self._path_str}")

        return destination

    def stat(self):
        """Get file statistics (using backend metadata)."""
        try:
            backend = self._get_backend()
            rel_path = self._get_relative_path()
            return backend.get_metadata(rel_path)
        except Exception as e:
            raise OSError(f"Cannot stat {self._path_str}: {e}")

    def _get_backend(self):
        """Get the appropriate backend for this path."""
        if self._is_uri:
            return self._store._get_backend_for_uri(self._path_str)
        else:
            return self._store.primary_backend

    def _get_relative_path(self) -> str:
        """Get the relative path for backend operations."""
        if self._is_uri:
            # For URIs, extract the path component
            parsed = urlparse(self._path_str)
            return parsed.path.lstrip("/")
        else:
            # For local paths, make relative to store root
            try:
                store_root = Path(self._store.storage_uri)
                file_path = Path(self._path_str)
                return str(file_path.relative_to(store_root))
            except ValueError:
                # If path is not relative to store root, use as-is
                return self._path_str

    # Additional pathlib.Path compatibility methods
    def with_name(self, name: str) -> "AutoPath":
        """Return a new path with the name changed."""
        return self.parent / name

    def with_suffix(self, suffix: str) -> "AutoPath":
        """Return a new path with the suffix changed."""
        return self.parent / (self.stem + suffix)

    def with_stem(self, stem: str) -> "AutoPath":
        """Return a new path with the stem changed."""
        return self.parent / (stem + self.suffix)

    def as_posix(self) -> str:
        """Return string representation with forward slashes."""
        return self._path_str.replace("\\", "/")

    def as_uri(self) -> str:
        """Return as URI string."""
        if self._is_uri:
            return self._path_str
        else:
            return Path(self._path_str).as_uri()

    def is_absolute(self) -> bool:
        """Check if path is absolute."""
        if self._is_uri:
            return True
        return Path(self._path_str).is_absolute()

    def joinpath(self, *args) -> "AutoPath":
        """Join path components."""
        result = self
        for arg in args:
            result = result / arg
        return result

    def match(self, pattern: str) -> bool:
        """Test whether the path matches a glob pattern."""
        if self._is_uri:
            return Path(self._parsed.path).match(pattern)
        return Path(self._path_str).match(pattern)

    def relative_to(self, other: Union[str, "AutoPath", Path]) -> str:
        """Return the relative path from other."""
        if isinstance(other, AutoPath):
            other_path = other._path_str
        else:
            other_path = str(other)

        if self._is_uri and AutoPath(other_path)._is_uri:
            # Both are URIs - compare path components
            self_parsed = urlparse(self._path_str)
            other_parsed = urlparse(other_path)
            if self_parsed.netloc != other_parsed.netloc or self_parsed.scheme != other_parsed.scheme:
                raise ValueError(f"'{self._path_str}' is not relative to '{other_path}'")
            return str(Path(self_parsed.path).relative_to(other_parsed.path))
        elif not self._is_uri and not AutoPath(other_path)._is_uri:
            # Both are local paths
            return str(Path(self._path_str).relative_to(other_path))
        else:
            raise ValueError("Cannot compute relative path between different path types")

    def resolve(self) -> "AutoPath":
        """Make the path absolute, resolving symlinks."""
        if self._is_uri:
            return self  # URIs are already resolved
        resolved_path = Path(self._path_str).resolve()
        return AutoPath(str(resolved_path), self._store)

    # Deletion methods
    def delete(self) -> None:
        """Delete file or directory (alias for unlink/rmdir)."""
        if self.is_file():
            self.unlink()
        elif self.is_dir():
            # Remove directory and all contents
            for item in self.iterdir():
                item.delete()
            self.rmdir()
        else:
            raise FileNotFoundError(f"Path not found: {self._path_str}")

    def load(self, format: Optional[str] = None, ignore_cache: bool = False) -> Any:
        """
        Load data using AutoStore's handler system for format detection and conversion.

        This method leverages AutoStore's handlers to automatically detect and parse
        different data formats like parquet, CSV, JSON, etc., returning the appropriate
        data structure (e.g., pandas DataFrame for parquet/CSV).

        Args:
            format: Optional format override (e.g., 'parquet', 'csv', 'json')
            ignore_cache: If True, forces fresh download from source, bypassing cache

        Returns:
            The loaded data in appropriate format based on file type and handlers

        Examples:
            ```python
            # Load parquet file as DataFrame
            df = data_path.load()
            # Force JSON parsing
            json_data = data_path.load(format="json")
            # Bypass cache
            fresh_data = data_path.load(ignore_cache=True)
            ```
        """
        # Use full path for URIs to ensure proper backend routing, relative path for local files
        if self._is_uri:
            return self._store.read(self._path_str, format=format, ignore_cache=ignore_cache)
        else:
            rel_path = self._get_relative_path()
            return self._store.read(rel_path, format=format, ignore_cache=ignore_cache)

    def save(self, data: Any, format: Optional[str] = None) -> None:
        """
        Save data using AutoStore's handler system for format detection and conversion.

        This method leverages AutoStore's handlers to automatically detect the appropriate
        format based on the data type and file extension, then saves the data accordingly.

        Args:
            data: The data to save (DataFrame, dict, list, etc.)
            format: Optional format override (e.g., 'parquet', 'csv', 'json')

        Examples:
            ```python
            # Save DataFrame as parquet (auto-detected from extension)
            data_path.save(df)
            # Force CSV format
            data_path.save(df, format="csv")
            # Save dict as JSON
            data_path.save({"key": "value"}, format="json")
            ```
        """
        rel_path = self._get_relative_path()
        self._store.write(rel_path, data, format=format)

    # File-like object methods for compatibility with functions like np.savez_compressed
    def write(self, data: bytes) -> int:
        """
        Write bytes to the file buffer.

        This method allows AutoPath to work as a file-like object with functions
        like numpy's savez_compressed that expect a file with a write method.

        Args:
            data: Bytes to write to the buffer

        Returns:
            Number of bytes written

        Example:
            # Works with numpy savez_compressed
            np.savez_compressed(matrix_path, data=matrix.data, indices=matrix.indices)
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("write() argument must be bytes or bytearray")

        self._is_open = True
        self._write_buffer.extend(data)
        return len(data)

    def flush(self) -> None:
        """Flush the write buffer (no-op for compatibility)."""
        pass

    def close(self) -> None:
        """
        Close the file-like object and save buffered data to storage.

        This uploads the buffered data to the backend storage and clears the buffer.
        """
        if self._is_open and self._write_buffer:
            self.write_bytes(bytes(self._write_buffer))
            self._write_buffer.clear()
        self._is_open = False

    def readable(self) -> bool:
        """Return whether the file supports reading."""
        return True

    def writable(self) -> bool:
        """Return whether the file supports writing."""
        return True

    def seekable(self) -> bool:
        """Return whether the file supports seeking."""
        return False

    def __enter__(self):
        """Context manager entry."""
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - automatically close and save."""
        self.close()

    def savez(self, *args, compress: bool = True, **kwargs) -> None:
        """
        Save multiple data objects to a zip archive, similar to numpy's savez_compressed.

        This method creates a zip file containing multiple data objects, each saved in their
        appropriate format based on the data type and AutoStore's handler system.

        Args:
            *args: Positional arguments - data objects to save (will be named 'data_0', 'data_1', etc.)
            compress: Whether to use compression (default True)
            **kwargs: Keyword arguments - data objects with custom names

        Examples:
            # Save multiple arrays like numpy's savez_compressed
            matrix_path.savez(matrix.data, indices=matrix.indices, shape=matrix.shape)

            # Save mixed data types
            results_path.savez(
                dataframe,
                model_params={"lr": 0.01, "epochs": 100},
                metrics=[0.95, 0.87, 0.92],
                compress=True
            )

            # Save with custom names
            analysis_path.savez(
                raw_data=df,
                processed_data=processed_df,
                summary_stats=stats_dict
            )
        """
        # Ensure the path ends with .zip
        if not self._path_str.endswith(".zip"):
            save_path = self._path_str + ".zip"
        else:
            save_path = self._path_str

        # Create name dictionary similar to numpy's approach
        namedict = kwargs.copy()
        for i, val in enumerate(args):
            key = f"data_{i}"
            if key in namedict:
                raise ValueError(f"Cannot use un-named variables and keyword {key}")
            namedict[key] = val

        if not namedict:
            raise ValueError("At least one data object must be provided")

        # Determine compression
        compression = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED

        # Create temporary directory for staging files
        with tempfile.TemporaryDirectory(prefix="autostore_zip_") as temp_dir:
            temp_path = Path(temp_dir)
            zip_path = temp_path / "archive.zip"

            # Create zip file
            with zipfile.ZipFile(zip_path, "w", compression=compression, allowZip64=True) as zipf:
                for key, data in namedict.items():
                    # Determine the best format for this data type
                    handler = self._store.handler_registry.get_handler_for_data(data)
                    if not handler:
                        # Fallback to pickle for unknown data types
                        file_extension = ".pkl"
                        handler = self._store.handler_registry.get_handler_for_extension(".pkl")
                    else:
                        # Use the first extension supported by the handler
                        file_extension = handler.extensions[0]

                    # Create temporary file for this data object
                    temp_file_path = temp_path / f"{key}{file_extension}"

                    # Write data using the appropriate handler
                    handler.write_to_file(data, temp_file_path, file_extension)

                    # Add to zip with the original key name + extension
                    zipf.write(temp_file_path, f"{key}{file_extension}")

            # Upload the zip file using the backend's upload method
            backend = self._get_backend()
            if save_path != self._path_str:
                # Need to create new AutoPath for the .zip extension
                rel_path = AutoPath(save_path, self._store)._get_relative_path()
            else:
                rel_path = self._get_relative_path()
            backend.upload(zip_path, rel_path)

    def loadz(self) -> dict:
        """
        Load data from a zip archive created by savez.

        Returns a dictionary where keys are the original names (without extensions)
        and values are the loaded data objects in their original formats.

        Returns:
            dict: Dictionary mapping names to loaded data objects

        Examples:
            # Load zip file
            data = matrix_path.loadz()
            matrix_data = data['data_0']
            indices = data['indices']
            shape = data['shape']

            # Access with known names
            results = results_path.loadz()
            df = results['raw_data']
            params = results['model_params']
        """
        # Ensure we're reading a zip file
        if not self._path_str.endswith(".zip"):
            # Create AutoPath for the .zip version
            zip_autopath = AutoPath(self._path_str + ".zip", self._store)
        else:
            zip_autopath = self

        if not zip_autopath.exists():
            raise FileNotFoundError(f"Zip file not found: {zip_autopath._path_str}")

        # Download zip file to temporary location using backend
        backend = zip_autopath._get_backend()
        rel_path = zip_autopath._get_relative_path()

        # Extract and load data
        result = {}
        with tempfile.TemporaryDirectory(prefix="autostore_unzip_") as temp_dir:
            temp_path = Path(temp_dir)
            zip_file_path = temp_path / "archive.zip"

            # Download the zip file using backend's download method
            downloaded_path = backend.download_with_cache(rel_path)
            # Copy to our temp location for processing
            import shutil

            shutil.copy2(downloaded_path, zip_file_path)

            with zipfile.ZipFile(zip_file_path, "r") as zipf:
                for file_info in zipf.infolist():
                    # Extract file
                    extracted_path = temp_path / file_info.filename
                    with zipf.open(file_info) as source, open(extracted_path, "wb") as target:
                        target.write(source.read())

                    # Determine the key name (remove extension)
                    file_path = Path(file_info.filename)
                    key = file_path.stem
                    file_extension = file_path.suffix

                    # Get appropriate handler and load data
                    handler = self._store.handler_registry.get_handler_for_extension(file_extension)
                    if handler:
                        result[key] = handler.read_from_file(extracted_path, file_extension)
                    else:
                        # Fallback: try to read as bytes
                        result[key] = extracted_path.read_bytes()

        return result
