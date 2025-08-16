from autostore.backends.base import StorageBackend
from autostore.backends.local import LocalFileBackend
from autostore.backends.s3 import S3Backend, S3Options

# Built-in scheme mapping
BUILTIN_BACKENDS = {
    "": LocalFileBackend,
    "file": LocalFileBackend,
    "s3": S3Backend,
}


def get_backend_class(scheme: str, options=None):
    """
    Get backend class for scheme.

    Args:
        scheme: URI scheme (e.g., 's3', 'file', '')
        options: Options object to determine backend type

    Returns:
        Backend class to use
    """
    # If options provided, use the backend_class property
    if options and hasattr(options, "backend_class"):
        return options.backend_class

    # Fallback to built-in scheme mapping for schemes without explicit options
    return BUILTIN_BACKENDS.get(scheme, LocalFileBackend)
