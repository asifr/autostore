import re
import boto3
import logging
import warnings
from pathlib import Path
from dataclasses import dataclass
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from typing import Optional, Iterator, Tuple
from autostore.backends.base import StorageBackend
from autostore.types import (
    Options,
    FileMetadata,
    StorageError,
    StorageFileNotFoundError,
    StorageConnectionError,
)

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

log = logging.getLogger(__name__)

# Suppress AWS SDK logging
warnings.filterwarnings("ignore")
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("s3fs").setLevel(logging.WARNING)
logging.getLogger("fsspec").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


@dataclass
class S3Options(Options):
    """S3 backend options."""

    scheme: str = "s3"  # Default scheme, can be customized

    # Authentication
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None

    # Configuration
    region_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    use_ssl: bool = True
    verify: Optional[bool] = None

    # Performance
    multipart_threshold: int = 64 * 1024 * 1024  # 64MB
    multipart_chunksize: int = 16 * 1024 * 1024  # 16MB
    max_concurrency: int = 10

    @property
    def backend_class(self):
        """Return the S3Backend class for this options instance."""
        return S3Backend


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """Return the bucket and key from an S3-style path."""
    # Use urllib.parse to properly handle any URI scheme
    from urllib.parse import urlparse

    parsed = urlparse(s3_path)
    if parsed.scheme:
        # For URIs with schemes, use netloc as bucket and path as key
        bucket = parsed.netloc
        path = parsed.path.lstrip("/").rstrip("/")
    else:
        # For paths without schemes, use the original logic
        path = s3_path.lstrip("/").rstrip("/")

    if parsed.scheme:
        # For URIs with schemes, we already extracted bucket and path above
        return bucket, path
    else:
        # For paths without schemes, use the original parsing logic
        if "/" not in path:
            return path, ""
        else:
            # Handle ARN formats and standard bucket/key format
            bucket_format_list = [
                re.compile(r"^(?P<bucket>arn:(aws).*:s3:[a-z\-0-9]*:[0-9]{12}:accesspoint[:/][^/]+)/?" r"(?P<key>.*)$"),
                re.compile(
                    r"^(?P<bucket>arn:(aws).*:s3-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
                    r"[a-zA-Z0-9\-]{1,63}[/:](bucket|accesspoint)[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
                ),
            ]

            for bucket_format in bucket_format_list:
                match = bucket_format.match(path)
                if match:
                    return match.group("bucket"), match.group("key")

            # Standard bucket/key format
            s3_components = path.split("/", 1)
            bucket = s3_components[0]
            key = s3_components[1] if len(s3_components) > 1 else ""

            # Handle version ID
            key, _, version_id = key.partition("?versionId=")
            return bucket, key


def glob_translate(pattern: str) -> str:
    """Translate a glob pattern to a regular expression."""
    # Simple glob to regex translation
    pattern = pattern.replace("\\", "/")  # Normalize separators

    # Escape special regex characters except * and ?
    special_chars = ".^$+{}[]|()"
    for char in special_chars:
        pattern = pattern.replace(char, "\\" + char)

    # Convert glob wildcards to regex
    pattern = pattern.replace("*", ".*")
    pattern = pattern.replace("?", ".")

    return f"^{pattern}$"


class S3Backend(StorageBackend):
    """S3 and S3-compatible storage backend."""

    def __init__(self, uri: str, options: S3Options, cache_service=None):
        super().__init__(uri, options, cache_service)

        # Parse S3 URI
        self.bucket, self.prefix = parse_s3_path(uri)

        # Set up boto3 session and client
        self._setup_client()

    def _setup_client(self):
        """Set up boto3 session and S3 client."""
        session_kwargs = {}

        if self.options.profile_name:
            session_kwargs["profile_name"] = self.options.profile_name

        self.session = boto3.Session(**session_kwargs)

        # Client configuration
        client_kwargs = {
            "service_name": "s3",
            "config": Config(
                max_pool_connections=self.options.max_concurrency,
                retries={"max_attempts": self.options.max_retries if hasattr(self.options, "max_retries") else 3},
            ),
        }

        if self.options.region_name:
            client_kwargs["region_name"] = self.options.region_name

        if self.options.endpoint_url:
            client_kwargs["endpoint_url"] = self.options.endpoint_url

        if self.options.aws_access_key_id:
            client_kwargs["aws_access_key_id"] = self.options.aws_access_key_id

        if self.options.aws_secret_access_key:
            client_kwargs["aws_secret_access_key"] = self.options.aws_secret_access_key

        if self.options.aws_session_token:
            client_kwargs["aws_session_token"] = self.options.aws_session_token

        if self.options.verify is not None:
            client_kwargs["verify"] = self.options.verify

        if not self.options.use_ssl:
            client_kwargs["use_ssl"] = False

        try:
            self.client = self.session.client(**client_kwargs)
        except Exception as e:
            raise StorageConnectionError(f"Failed to connect to S3: {e}")

        # Set up transfer config
        self.transfer_config = TransferConfig(
            multipart_threshold=self.options.multipart_threshold,
            multipart_chunksize=self.options.multipart_chunksize,
            max_concurrency=self.options.max_concurrency,
            max_io_queue=100,
            io_chunksize=262144,  # 256KB
            use_threads=True,
        )

    def _get_full_key(self, path: str) -> str:
        """Get full S3 key by combining prefix and path."""
        path = path.lstrip("/")
        if self.prefix:
            return f"{self.prefix.rstrip('/')}/{path}"
        return path

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists."""
        full_key = self._get_full_key(path)

        try:
            # Try to get object metadata
            self.client.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except self.client.exceptions.NoSuchKey:
            # Check if it's a directory by listing objects with the prefix
            try:
                response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=full_key.rstrip("/") + "/", MaxKeys=1)
                return response.get("KeyCount", 0) > 0
            except Exception:
                return False
        except Exception as e:
            log.debug(f"Error checking existence of {path}: {e}")
            return False

    def download(self, remote_path: str, local_path: Path) -> None:
        """Download a file from S3 to local path."""
        full_key = self._get_full_key(remote_path)

        try:
            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Download file
            self.client.download_file(self.bucket, full_key, str(local_path), Config=self.transfer_config)

        except self.client.exceptions.NoSuchKey:
            raise StorageFileNotFoundError(f"File not found: {remote_path}")
        except self.client.exceptions.NoSuchBucket:
            raise StorageFileNotFoundError(f"Bucket not found: {self.bucket}")
        except Exception as e:
            # Check if it's a 404 error (file not found)
            if hasattr(e, "response") and e.response.get("Error", {}).get("Code") == "404":
                raise StorageFileNotFoundError(f"File not found: {remote_path}")
            elif hasattr(e, "response") and "Not Found" in str(e):
                raise StorageFileNotFoundError(f"File not found: {remote_path}")
            else:
                raise StorageError(f"Error downloading {remote_path}: {e}")

    def upload(self, local_path: Path, remote_path: str) -> None:
        """Upload a local file to S3."""
        full_key = self._get_full_key(remote_path)

        try:
            # Determine content type
            content_type = self._guess_content_type(local_path.suffix)
            extra_args = {}
            if content_type:
                extra_args["ContentType"] = content_type

            # Upload file
            self.client.upload_file(
                str(local_path), self.bucket, full_key, ExtraArgs=extra_args, Config=self.transfer_config
            )

        except Exception as e:
            raise StorageError(f"Error uploading {remote_path}: {e}")

    def delete(self, path: str) -> None:
        """Delete a file from S3."""
        full_key = self._get_full_key(path)

        try:
            self.client.delete_object(Bucket=self.bucket, Key=full_key)
        except Exception as e:
            raise StorageError(f"Error deleting {path}: {e}")

    def list_files(self, pattern: str = "*", recursive: bool = True) -> Iterator[str]:
        """List files matching a pattern."""
        try:
            # Convert glob pattern to prefix for S3 optimization
            if pattern.startswith("**/"):
                pattern = pattern[3:]

            # Determine prefix from pattern
            prefix_parts = []
            pattern_parts = pattern.split("/")

            for part in pattern_parts:
                if "*" in part or "?" in part:
                    break
                prefix_parts.append(part)

            prefix = "/".join(prefix_parts) if prefix_parts else ""
            if prefix and not pattern.endswith("/"):
                prefix += "/"

            full_prefix = self._get_full_key(prefix) if prefix else self.prefix or ""

            # List objects
            paginator = self.client.get_paginator("list_objects_v2")
            page_kwargs = {"Bucket": self.bucket, "Prefix": full_prefix}
            if not recursive:
                page_kwargs["Delimiter"] = "/"
            page_iterator = paginator.paginate(**page_kwargs)

            # Convert pattern to regex
            regex_pattern = re.compile(glob_translate(pattern))

            for page in page_iterator:
                for obj in page.get("Contents", []):
                    key = obj["Key"]

                    # Remove prefix to get relative path
                    if self.prefix and key.startswith(self.prefix):
                        rel_path = key[len(self.prefix) :].lstrip("/")
                    else:
                        rel_path = key

                    # Check pattern match
                    if regex_pattern.match(rel_path):
                        yield rel_path

        except Exception as e:
            raise StorageError(f"Error listing files with pattern '{pattern}': {e}")

    def get_metadata(self, path: str) -> FileMetadata:
        """Get metadata for a file."""
        full_key = self._get_full_key(path)

        try:
            response = self.client.head_object(Bucket=self.bucket, Key=full_key)

            size = response["ContentLength"]
            modified_time = response["LastModified"]
            content_type = response.get("ContentType")
            etag = response.get("ETag", "").strip('"')

            return FileMetadata(
                size=size,
                modified_time=modified_time,
                content_type=content_type,
                etag=etag,
                extra={
                    "bucket": self.bucket,
                    "key": full_key,
                    "storage_class": response.get("StorageClass"),
                    "metadata": response.get("Metadata", {}),
                },
            )

        except self.client.exceptions.NoSuchKey:
            raise StorageFileNotFoundError(f"File not found: {path}")
        except Exception as e:
            raise StorageError(f"Error getting metadata for {path}: {e}")

    def _guess_content_type(self, extension: str) -> Optional[str]:
        """Guess content type from file extension."""
        extension = extension.lower()
        return CONTENT_TYPES.get(extension)
