"""
AutoStore S3 Storage Backend

License: Apache License 2.0

Changes
-------
- 0.1.6 - Added S3Options for configuration management and parquet dataset support
- 0.1.3 - Initial implementation
    - Implement S3 storage backend with basic operations
    - Added S3StorageConfig for configuration management.
    - Implemented S3Backend class for handling S3 interactions.
    - Integrated error handling for common S3 exceptions.
    - Added support for multipart uploads and downloads.
"""

import re
import boto3
import fnmatch
import logging
import warnings
import typing as t
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from botocore.config import Config
from urllib.parse import urlparse, parse_qs
from boto3.s3.transfer import S3Transfer, TransferConfig
from autostore.autostore import (
    StorageBackend,
    Options,
    FileMetadata,
    StorageError,
    StorageFileNotFoundError,
    StoragePermissionError,
    StorageConnectionError,
    CONTENT_TYPES,
)


@dataclass
class S3Options(Options):
    """Options for S3 backend."""
    
    # Instance-level scheme specification
    scheme: str = "s3"  # Default to s3, but can be overridden per instance

    # Authentication
    aws_access_key_id: t.Optional[str] = None
    aws_secret_access_key: t.Optional[str] = None
    aws_session_token: t.Optional[str] = None
    profile_name: t.Optional[str] = None

    # Configuration
    region_name: t.Optional[str] = None
    endpoint_url: t.Optional[str] = None
    use_ssl: bool = True
    verify: t.Optional[bool] = None

    # Performance
    multipart_threshold: int = 64 * 1024 * 1024  # 64MB
    multipart_chunksize: int = 16 * 1024 * 1024  # 16MB
    max_concurrency: int = 10

    # Dataset Support
    enable_dataset_detection: bool = True
    dataset_cache_strategy: str = "preserve_structure"  # "preserve_structure" or "flatten"
    
    def __post_init__(self):
        # Set the backend class after initialization
        if self.backend_class is None:
            # Import here to avoid circular imports
            self.backend_class = S3Backend


warnings.filterwarnings("ignore")
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("s3fs").setLevel(logging.WARNING)
logging.getLogger("fsspec").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_s3_path(s3_path: str) -> t.Tuple[str, str]:
    """Return the bucket and key from an S3 path."""
    # Remove protocol prefix
    path = s3_path
    for protocol in ("s3://", "s3a://", "s3::", "s3a::"):
        if path.startswith(protocol):
            path = path[len(protocol) :]
            break

    path = path.lstrip("/").rstrip("/")

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


# S3Options is now defined in autostore.py, but keep this for backward compatibility
@dataclass
class S3StorageConfig(S3Options):
    """Legacy S3 configuration for backward compatibility."""

    pass


class S3Backend(StorageBackend):
    """
    S3 storage backend for AWS S3 and S3-compatible services with dataset support.

    Supports URI formats:
    - s3://bucket/prefix/
    - s3://bucket/prefix/?region=us-west-2
    - s3://bucket/prefix/?endpoint_url=https://s3.amazonaws.com

    Authentication methods:
    1. Explicit credentials in options
    2. AWS profile name in options
    3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    4. IAM roles (when running on EC2)
    5. AWS credentials file (~/.aws/credentials)
    """

    # Declare the options class for this backend
    options_class = S3Options

    def __init__(self, uri: str, options: S3Options):
        super().__init__(uri, options)

        # Parse S3 URI - accept any scheme since this backend can handle S3-compatible services
        parsed = urlparse(uri)
        if not parsed.scheme:
            raise ValueError("URI must include a scheme (e.g., s3://, conductor://)")

        # Extract bucket and prefix from URI
        self.bucket, self.prefix = parse_s3_path(uri)
        if not self.bucket:
            raise ValueError("S3 URI must include a bucket name")

        # Ensure prefix ends with / if not empty (for consistent key handling)
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

        # Parse query parameters from URI
        query_params = parse_qs(parsed.query) if parsed.query else {}

        # Update options with URI query parameters
        if "region" in query_params:
            self.options.region_name = query_params["region"][0]
        if "endpoint_url" in query_params:
            self.options.endpoint_url = query_params["endpoint_url"][0]

        # Initialize S3 client
        self._client = None
        self._transfer = None

    @property
    def client(self):
        """Lazy initialization of S3 client."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    @property
    def transfer(self):
        """Lazy initialization of S3 transfer manager."""
        if self._transfer is None:
            transfer_config = TransferConfig(
                multipart_threshold=self.options.multipart_threshold,
                multipart_chunksize=self.options.multipart_chunksize,
                max_concurrency=self.options.max_concurrency,
                use_threads=True,
            )
            self._transfer = S3Transfer(client=self.client, config=transfer_config)
        return self._transfer

    def _create_client(self):
        """Create boto3 S3 client with configuration."""
        try:
            # Create boto3 config
            boto_config = Config(read_timeout=self.options.timeout, retries={"max_attempts": self.options.max_retries})

            # Create session
            if self.options.profile_name:
                session = boto3.Session(profile_name=self.options.profile_name)
            else:
                session = boto3.Session()

            # Client parameters
            client_kwargs = {
                "service_name": "s3",
                "config": boto_config,
                "use_ssl": self.options.use_ssl,
            }

            # Add credentials if provided
            if self.options.aws_access_key_id:
                client_kwargs["aws_access_key_id"] = self.options.aws_access_key_id
            if self.options.aws_secret_access_key:
                client_kwargs["aws_secret_access_key"] = self.options.aws_secret_access_key
            if self.options.aws_session_token:
                client_kwargs["aws_session_token"] = self.options.aws_session_token
            if self.options.region_name:
                client_kwargs["region_name"] = self.options.region_name
            if self.options.endpoint_url:
                client_kwargs["endpoint_url"] = self.options.endpoint_url
            if self.options.verify is not None:
                client_kwargs["verify"] = self.options.verify

            return session.client(**client_kwargs)

        except Exception as e:
            raise StorageConnectionError(f"Failed to create S3 client: {e}") from e

    def _get_full_key(self, path: str) -> str:
        """Convert relative path to full S3 key."""
        path = path.replace("\\", "/").strip("/")
        if self.prefix:
            return f"{self.prefix}{path}"
        return path

    def _strip_prefix(self, key: str) -> str:
        """Remove prefix from S3 key to get relative path."""
        if self.prefix and key.startswith(self.prefix):
            return key[len(self.prefix) :]
        return key

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists in S3."""
        try:
            full_key = self._get_full_key(path)

            # Try head_object first (for files)
            try:
                self.client.head_object(Bucket=self.bucket, Key=full_key)
                return True
            except self.client.exceptions.NoSuchKey:
                pass
            except Exception:
                # Other errors might indicate permission issues
                pass

            # Try list_objects_v2 for directory-like paths
            try:
                prefix = full_key if full_key.endswith("/") else full_key + "/"
                response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
                return "Contents" in response or "CommonPrefixes" in response
            except Exception:
                return False

        except Exception:
            return False

    def download(self, remote_path: str, local_path: Path) -> None:
        """Download file from S3 to local path."""
        full_key = self._get_full_key(remote_path)

        try:
            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Get object metadata first to optimize transfer
            try:
                head_response = self.client.head_object(Bucket=self.bucket, Key=full_key)
                content_length = head_response.get("ContentLength", 0)
            except self.client.exceptions.NoSuchKey:
                raise StorageFileNotFoundError(f"File not found: {remote_path}")

            # Use transfer manager for large files, direct download for small files
            if content_length > self.options.multipart_threshold:
                self.transfer.download_file(self.bucket, full_key, str(local_path))
            else:
                response = self.client.get_object(Bucket=self.bucket, Key=full_key)
                with open(local_path, "wb") as f:
                    f.write(response["Body"].read())

        except self.client.exceptions.NoSuchKey:
            raise StorageFileNotFoundError(f"File not found: {remote_path}")
        except self.client.exceptions.NoSuchBucket:
            raise StorageFileNotFoundError(f"Bucket not found: {self.bucket}")
        except Exception as e:
            if "AccessDenied" in str(e):
                raise StoragePermissionError(f"Access denied downloading {remote_path}: {e}")
            raise StorageError(f"Error downloading {remote_path}: {e}") from e

    def upload(self, local_path: Path, remote_path: str) -> None:
        """Upload local file to S3."""
        full_key = self._get_full_key(remote_path)

        try:
            # Check if file exists and get size
            if not local_path.exists():
                raise FileNotFoundError(f"Local file not found: {local_path}")

            file_size = local_path.stat().st_size

            # Determine content type
            content_type = self._guess_content_type(str(local_path))
            extra_args = {}
            if content_type:
                extra_args["ContentType"] = content_type

            # Use transfer manager for large files, direct put_object for small files
            if file_size > self.options.multipart_threshold:
                self.transfer.upload_file(str(local_path), self.bucket, full_key, extra_args=extra_args)
            else:
                with open(local_path, "rb") as f:
                    self.client.put_object(Bucket=self.bucket, Key=full_key, Body=f.read(), **extra_args)

        except Exception as e:
            if "AccessDenied" in str(e):
                raise StoragePermissionError(f"Access denied uploading {remote_path}: {e}")
            raise StorageError(f"Error uploading {remote_path}: {e}") from e

    def delete(self, path: str) -> None:
        """Delete a file from S3."""
        full_key = self._get_full_key(path)

        try:
            # Check if it's a directory-like path (ends with / or has objects under it)
            if self.is_directory(path):
                # Delete all objects with this prefix
                self._delete_prefix(full_key)
            else:
                # Delete single object
                self.client.delete_object(Bucket=self.bucket, Key=full_key)

        except Exception as e:
            if "NoSuchKey" in str(e):
                raise StorageFileNotFoundError(f"File not found: {path}")
            if "AccessDenied" in str(e):
                raise StoragePermissionError(f"Access denied deleting {path}: {e}")
            raise StorageError(f"Error deleting {path}: {e}") from e

    def _delete_prefix(self, prefix: str) -> None:
        """Delete all objects with a given prefix."""
        try:
            # List all objects with the prefix
            paginator = self.client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue

                # Batch delete objects (up to 1000 at a time)
                objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]

                if objects_to_delete:
                    self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": objects_to_delete})

        except Exception as e:
            raise StorageError(f"Error deleting prefix {prefix}: {e}") from e

    def list_files(self, pattern: str = "*", recursive: bool = True) -> t.Iterator[str]:
        """List files matching a pattern in S3."""
        try:
            # Convert glob pattern to regex for filtering
            if pattern != "*":
                regex_pattern = glob_translate(pattern)
                compiled_pattern = re.compile(regex_pattern)
            else:
                compiled_pattern = None

            # List objects with prefix
            paginator = self.client.get_paginator("list_objects_v2")
            page_kwargs = {"Bucket": self.bucket}

            if self.prefix:
                page_kwargs["Prefix"] = self.prefix

            if not recursive:
                page_kwargs["Delimiter"] = "/"

            for page in paginator.paginate(**page_kwargs):
                # Process regular objects
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    relative_path = self._strip_prefix(key)

                    # Skip if empty path (shouldn't happen but be safe)
                    if not relative_path:
                        continue

                    # Apply pattern filter
                    if compiled_pattern is None or compiled_pattern.match(relative_path):
                        yield relative_path

                # For non-recursive listing, also yield "directory" names
                if not recursive:
                    for prefix_info in page.get("CommonPrefixes", []):
                        prefix_key = prefix_info["Prefix"]
                        relative_path = self._strip_prefix(prefix_key).rstrip("/")

                        if relative_path and (compiled_pattern is None or compiled_pattern.match(relative_path)):
                            yield relative_path

        except Exception as e:
            raise StorageError(f"Error listing files with pattern '{pattern}': {e}") from e

    def get_metadata(self, path: str) -> FileMetadata:
        """Get file metadata from S3."""
        full_key = self._get_full_key(path)

        try:
            response = self.client.head_object(Bucket=self.bucket, Key=full_key)

            # Extract metadata
            size = response.get("ContentLength", 0)
            modified_time = response.get("LastModified", datetime.now())
            content_type = response.get("ContentType")
            etag = response.get("ETag", "").strip('"')

            # Convert datetime if needed
            if isinstance(modified_time, str):
                modified_time = datetime.fromisoformat(modified_time.replace("Z", "+00:00"))

            return FileMetadata(
                size=size,
                modified_time=modified_time,
                content_type=content_type,
                etag=etag,
                extra={
                    "storage_class": response.get("StorageClass"),
                    "server_side_encryption": response.get("ServerSideEncryption"),
                    "metadata": response.get("Metadata", {}),
                    "cache_control": response.get("CacheControl"),
                    "content_disposition": response.get("ContentDisposition"),
                    "content_encoding": response.get("ContentEncoding"),
                    "content_language": response.get("ContentLanguage"),
                },
            )

        except self.client.exceptions.NoSuchKey:
            raise StorageFileNotFoundError(f"File not found: {path}")
        except Exception as e:
            raise StorageError(f"Error getting metadata for {path}: {e}") from e

    def copy(self, src_path: str, dst_path: str) -> None:
        """Copy a file within S3 (server-side copy)."""
        src_key = self._get_full_key(src_path)
        dst_key = self._get_full_key(dst_path)

        try:
            copy_source = {"Bucket": self.bucket, "Key": src_key}
            self.client.copy_object(CopySource=copy_source, Bucket=self.bucket, Key=dst_key)

        except self.client.exceptions.NoSuchKey:
            raise StorageFileNotFoundError(f"Source file not found: {src_path}")
        except Exception as e:
            if "AccessDenied" in str(e):
                raise StoragePermissionError(f"Access denied copying {src_path} to {dst_path}: {e}")
            raise StorageError(f"Error copying {src_path} to {dst_path}: {e}") from e

    def move(self, src_path: str, dst_path: str) -> None:
        """Move a file within S3 (copy then delete)."""
        # S3 doesn't have native move, so copy then delete
        self.copy(src_path, dst_path)
        self.delete(src_path)

    def is_directory(self, path: str) -> bool:
        """Check if a path represents a directory in S3."""
        try:
            full_key = self._get_full_key(path)
            prefix = full_key if full_key.endswith("/") else full_key + "/"

            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)

            return "Contents" in response or "CommonPrefixes" in response

        except Exception:
            return False

    def is_dataset(self, path: str) -> bool:
        """Check if S3 path represents a dataset (directory with multiple files)."""
        full_key = self._get_full_key(path)

        # List objects with prefix to see if multiple files exist
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=full_key,
                MaxKeys=2,  # We only need to know if there's more than 1
            )

            contents = response.get("Contents", [])

            # If exactly one object matches the key exactly, it's a single file
            exact_matches = [obj for obj in contents if obj["Key"] == full_key]
            if len(exact_matches) == 1:
                return False

            # If multiple objects with this prefix, it's a dataset
            return len(contents) > 1
        except Exception:
            return False

    def download_dataset(
        self, remote_dataset_path: str, local_dataset_path: Path, file_pattern: str = "*"
    ) -> t.List[Path]:
        """Download entire dataset preserving S3 folder structure."""
        full_prefix = self._get_full_key(remote_dataset_path)
        downloaded_files = []

        try:
            # Get all objects with the prefix
            paginator = self.client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    obj_key = obj["Key"]

                    # Apply file pattern filter if specified
                    if file_pattern != "*":
                        filename = Path(obj_key).name
                        if not fnmatch.fnmatch(filename, file_pattern):
                            continue
                    
                    # For parquet datasets, only download .parquet files
                    if file_pattern == "*.parquet" and not obj_key.endswith(".parquet"):
                        continue

                    # Calculate local path preserving S3 structure
                    rel_path = obj_key[len(full_prefix) :].lstrip("/")
                    if not rel_path:  # Skip the directory itself
                        continue

                    local_file_path = local_dataset_path / rel_path
                    local_file_path.parent.mkdir(parents=True, exist_ok=True)

                    # Download file
                    self.download(self._strip_prefix(obj_key), local_file_path)
                    downloaded_files.append(local_file_path)

            return downloaded_files
        except Exception as e:
            raise StorageError(f"Error downloading dataset {remote_dataset_path}: {e}") from e

    def _guess_content_type(self, path: str) -> t.Optional[str]:
        """Guess content type from file extension."""
        extension = "." + path.split(".")[-1].lower() if "." in path else ""
        return CONTENT_TYPES.get(extension)

    def cleanup(self) -> None:
        """Clean up S3 client resources."""
        # boto3 clients are automatically cleaned up
        self._client = None
        self._transfer = None
        # Call parent cleanup for cache management
        super().cleanup()

    def __repr__(self) -> str:
        return f"S3Backend(bucket='{self.bucket}', prefix='{self.prefix}')"
