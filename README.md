# AutoStore - File Storage Made Simple

AutoStore provides a dictionary-like interface for reading and writing files with automatic backend detection and cross-cloud access.

AutoStore eliminates the cognitive overhead of managing different file formats and storage backends, letting you focus on your data and analysis rather than the mechanics of file I/O. It automatically detects storage backends from URI prefixes (s3://, gcs://, etc.), handles file format detection, type inference, and provides a clean, intuitive API for data persistence across local and cloud storage.

## Why Use AutoStore?

-   **Zero Configuration**: No manual backend registration - automatically detects storage type from URI prefixes
-   **Multi-Service Support**: Use multiple S3-compatible services (AWS, Conductor, MinIO, etc.) with different configurations
-   **Cross-Backend Access**: Access any storage backend from a single store instance using URI syntax
-   **Dataset Support**: Automatically handles both individual files and multi-file datasets (parquet, CSV collections)
-   **Smart Caching**: Intelligent caching system with configurable expiration reduces redundant downloads
-   **Multiple Data Types**: Built-in support for Polars DataFrames, JSON, CSV, images, PyTorch models, NumPy arrays, and more
-   **Type-Safe Options**: Dataclass-based configuration with IDE support and validation for each service
-   **Performance Optimized**: Upload/download operations with efficient handling of large files and datasets

## Getting Started

AutoStore requires Python 3.10+ and can be installed via pip.

```bash
pip install autostore
```

### Basic Usage - Zero Configuration

```python
from autostore import AutoStore

# Local storage - no configuration needed
store = AutoStore("./data")

# Write data - automatically saves with appropriate extensions
store["my_dataframe"] = df           # ./data/my_dataframe.parquet
store["config"] = {"key": "value"}   # ./data/config.json
store["logs"] = [{"event": "start"}] # ./data/logs.jsonl

# Read data
df = store["my_dataframe"]           # Returns a Polars DataFrame
config = store["config"]             # Returns a dict
logs = store["logs"]                 # Returns a list of dicts
```

### Cloud Storage - Automatic Detection

```python
from autostore import AutoStore
from autostore.s3 import S3Options

# S3 - automatically detected from s3:// prefix
store = AutoStore(
    "s3://my-bucket/data/",
    profile_name="my-profile",
    cache_enabled=True
)

# Or with explicit options
options = S3Options(
    profile_name="my-profile",
    region_name="us-east-1",
    cache_enabled=True,
    cache_expiry_hours=12
)
store = AutoStore("s3://my-bucket/data/", options=options)

# Write data to S3
store["experiment/results"] = {"accuracy": 0.95, "epochs": 100}

# Read data from S3
results = store["experiment/results"]  # Uses cache on subsequent loads
```

### Cross-Backend Access

```python
from autostore import AutoStore

# Create a local store as primary backend
store = AutoStore("./local-cache", cache_enabled=True)

# Access different backends using full URIs
store["local_file"] = {"type": "local"}                    # Primary backend
store["s3://bucket/remote.json"] = {"type": "s3"}          # S3 backend

# Read from any backend
local_data = store["local_file"]                           # From local
s3_data = store["s3://bucket/remote.json"]                 # From S3
```

### Multiple S3-Compatible Services

AutoStore supports multiple S3-compatible services with different configurations:

```python
from autostore import AutoStore
from autostore.s3 import S3Options

# Register new schemes for different S3-compatible services
AutoStore.register_scheme("minio", "autostore.s3")
AutoStore.register_scheme("digitalocean", "autostore.s3")

# Create service-specific options with different configurations
aws_options = S3Options(
    scheme="s3",
    profile_name="aws-production",
    region_name="us-east-1",
    cache_enabled=True
)

minio_options = S3Options(
    scheme="minio",
    endpoint_url="https://minio.mycompany.com",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1"
)

digitalocean_options = S3Options(
    scheme="digitalocean",
    endpoint_url="https://nyc3.digitaloceanspaces.com",
    region_name="nyc3",
    cache_enabled=True
)

# Create AutoStore with multiple backend options
store = AutoStore(
    "./local-cache",
    options=[aws_options, minio_options, digitalocean_options]
)

# Each scheme automatically uses its appropriate configuration
store["s3://aws-bucket/data.json"] = {"source": "aws"}
store["minio://my-bucket/data.json"] = {"source": "minio"}
store["digitalocean://my-space/data.json"] = {"source": "digitalocean"}

# Cross-backend data access with automatic option selection
aws_data = store["s3://aws-bucket/data.json"]
minio_data = store["minio://my-bucket/data.json"]
digitalocean_data = store["digitalocean://my-space/data.json"]
```

### Dataset Support

```python
from autostore import AutoStore

# Automatically detects and handles datasets
# For example, if you have multiple parquet files in an S3 bucket:
# ├── weather
# │   ├── 2024
# │   │   ├── january.parquet
# │   │   ├── february.parquet
# │   │   └── march.parquet
store = AutoStore("s3://my-bucket/datasets/")

# Access parquet dataset (multiple files)
weather_data = store["weather/2024/"]  # Loads entire dataset as LazyFrame

# Access individual file
single_file = store["weather/2024/january.parquet"]

# List files in dataset
files = list(store.list_files("weather/2024/*", recursive=True))
```

## Supported Data Types

| Data Type                  | File Extension         | Description                 | Library Required |
| -------------------------- | ---------------------- | --------------------------- | ---------------- |
| Polars DataFrame/LazyFrame | `.parquet`, `.csv`     | High-performance DataFrames | polars           |
| Python dict/list           | `.json`                | Standard JSON serialization | built-in         |
| List of dicts              | `.jsonl`               | JSON Lines format           | built-in         |
| Pydantic models            | `.pydantic.json`       | Structured data models      | pydantic         |
| Python dataclasses         | `.dataclass.json`      | Dataclass serialization     | built-in         |
| String data                | `.txt`, `.html`, `.md` | Plain text files            | built-in         |
| NumPy arrays               | `.npy`, `.npz`         | Numerical data              | numpy            |
| SciPy sparse matrices      | `.sparse`              | Sparse matrix data          | scipy            |
| PyTorch tensors/models     | `.pt`, `.pth`          | Deep learning models        | torch            |
| PIL/Pillow images          | `.png`, `.jpg`, etc.   | Image data                  | Pillow           |
| YAML data                  | `.yaml`, `.yml`        | Human-readable config files | PyYAML           |
| Any Python object          | `.pkl`                 | Pickle fallback             | built-in         |

## Supported Storage Backends

AutoStore automatically detects the storage backend from URI prefixes:

| Backend | URI Prefix          | Options Class | Example                       |
| ------- | ------------------- | ------------- | ----------------------------- |
| Local   | `./path` or `/path` | `Options`     | `./data`, `/Users/name/files` |
| S3      | `s3://`             | `S3Options`   | `s3://bucket/prefix/`         |

## Configuration Options

### Base Options (All Backends)

```python
from autostore import Options

base_options = Options(
    cache_enabled=True,           # Enable local caching
    cache_dir="./cache",          # Custom cache directory
    cache_expiry_hours=12,        # Cache expiration time (0 = never expire)
    timeout=30,                   # Request timeout in seconds
    max_retries=3,                # Maximum retry attempts
    retry_delay=1.0               # Delay between retries
)
```

### S3Options

```python
from autostore.s3 import S3Options

s3_options = S3Options(
    # Scheme specification for multi-backend support
    scheme="s3",                          # URI scheme this options applies to

    # Authentication
    aws_access_key_id="your-key",
    aws_secret_access_key="your-secret",
    profile_name="my-profile",            # AWS profile name

    # Configuration
    region_name="us-east-1",
    endpoint_url="custom-endpoint",       # For S3-compatible services

    # Performance
    multipart_threshold=64 * 1024 * 1024, # Files > 64MB use multipart
    multipart_chunksize=16 * 1024 * 1024, # Chunk size for uploads
    max_concurrency=10,                   # Concurrent operations

    # Inherited from Options
    cache_enabled=True,
    cache_expiry_hours=6          # 0 = never expire
)
```

### Usage Patterns

```python
# Method 1: Keyword arguments
store = AutoStore("s3://bucket/", profile_name="prod", cache_enabled=True)

# Method 2: Single options object
options = S3Options(scheme="s3", profile_name="prod", cache_enabled=True)
store = AutoStore("s3://bucket/", options=options)

# Method 3: Multiple options for different services
aws_options = S3Options(scheme="s3", profile_name="aws-prod")
minio_options = S3Options(scheme="minio", endpoint_url="https://minio.example.com")
store = AutoStore("./cache", options=[aws_options, minio_options])

# Method 4: Mixed (options object + additional kwargs)
base_options = S3Options(scheme="s3", profile_name="prod")
store = AutoStore("s3://bucket/", options=base_options, cache_enabled=True)
```

## Advanced Features

### Backend Management

```python
# Register new S3-compatible services
AutoStore.register_scheme("minio", "autostore.s3")
AutoStore.register_scheme("digitalocean", "autostore.s3")

# Check supported backends
backends = store.get_supported_backends()
print(f"Available: {backends}")  # ['s3', 'minio', 'digitalocean', 'file', '']

# View active backends
active = store.list_active_backends()
print(f"Active: {active}")  # ['primary: ./data', 's3: s3://bucket/', 'minio: minio://bucket/']

# Backend auto-loading with appropriate options
data = store["s3://bucket/file.json"]              # Uses AWS S3 options
data = store["minio://bucket/file.json"]           # Uses MinIO options
data = store["digitalocean://space/file.json"]     # Uses DigitalOcean options
```

### Dataset Operations

```python
# Dataset detection
is_dataset = store.primary_backend.is_dataset("path/to/data/")
is_directory = store.primary_backend.is_directory("path/")

# List dataset files
files = list(store.list_files("dataset/*", recursive=True))

# Load entire dataset (for parquet/CSV collections)
lazy_frame = store["weather_data/"]  # Loads all parquet files as one LazyFrame
```

### Caching System

AutoStore includes caching that:

-   Stores frequently accessed files locally
-   Uses ETags for cache validation
-   Automatically expires old cache entries (or never expires if cache_expiry_hours=0)
-   Works across all backends

```python
# Enable caching for any backend
store = AutoStore("s3://bucket/", cache_enabled=True, cache_expiry_hours=6)

# Never expire cache entries (useful for immutable data)
store = AutoStore("s3://bucket/", cache_enabled=True, cache_expiry_hours=0)

# Cache management
store.cleanup_cache()  # Remove expired cache entries

# Check cache status
metadata = store.get_metadata("large_file")
print(f"File size: {metadata.size} bytes")
print(f"ETag: {metadata.etag}")
```

### Custom Data Handlers

Add support for new data types by creating custom handlers:

```python
from pathlib import Path
from autostore.autostore import DataHandler

class CustomLogHandler(DataHandler):
    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".log"

    def can_handle_data(self, data) -> bool:
        return isinstance(data, list) and all(
            isinstance(item, dict) and "timestamp" in item
            for item in data
        )

    def read_from_file(self, file_path: Path, file_extension: str):
        logs = []
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    logs.append(json.loads(line))
        return logs

    def write_to_file(self, data, file_path: Path, file_extension: str):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w') as f:
            for entry in data:
                f.write(json.dumps(entry) + '\n')

    @property
    def extensions(self):
        return [".log"]

    @property
    def priority(self):
        return 15

# Register the handler
store.register_handler(CustomLogHandler())
```

### File Operations

```python
# Check existence
if "config" in store:
    print("Config file exists")

# List all files
for key in store.keys():
    print(f"File: {key}")

# Get file metadata
metadata = store.get_metadata("large_dataset")
print(f"Size: {metadata.size} bytes")
print(f"Modified: {metadata.modified_time}")

# Copy and move files
store.copy("original", "backup")
store.move("temp_file", "permanent_file")

# Delete files
del store["old_data"]
```

### Context Management

```python
# Automatic cleanup of temporary files and cache
with AutoStore("./data", config=config) as store:
    store["data"] = large_dataset
    results = store["data"]
# Temporary files are automatically cleaned up here
```

## Migration from v0.1.x

If you're upgrading from an earlier version, here are the key changes:

### Before (v0.1.x)

```python
from autostore import AutoStore
from autostore.s3 import S3Backend, S3StorageConfig

# Manual backend registration required
AutoStore.register_backend("s3", S3Backend)

# Separate config objects
config = S3StorageConfig(region_name="us-east-1")
store = AutoStore("s3://bucket/", config=config)
```

### After (v2.0+)

```python
from autostore import AutoStore, S3Options

# Automatic backend detection - no registration needed
store = AutoStore("s3://bucket/", region_name="us-east-1")

# Or with options object
options = S3Options(region_name="us-east-1")
store = AutoStore("s3://bucket/", options=options)
```

### Key Changes

-   ✅ No backend registration - automatic detection from URI prefixes
-   ✅ Unified Options system - consistent configuration across backends
-   ✅ Cross-backend access - access any backend from any store instance
-   ✅ Dataset support - automatic handling of multi-file datasets
-   ✅ Enhanced error handling - clear dependency and configuration messages

## Performance Considerations

### Large File Handling

AutoStore automatically optimizes for large files:

-   Multipart uploads/downloads for files > 64MB
-   Configurable chunk sizes and concurrency
-   Streaming operations to minimize memory usage

## When to Use AutoStore

Choose AutoStore when you need:

-   Multi-cloud data access with seamless backend switching
-   Dataset processing with automatic detection of file collections
-   Zero-configuration setup for rapid prototyping and development
-   Cross-backend operations without managing multiple client libraries
-   Data science projects with mixed file types across storage systems
-   Type-safe configuration with IDE support and validation
-   Intelligent caching to optimize cloud storage costs and performance

Don't choose AutoStore when:

-   You need complex queries or relational operations (use databases)
-   You only work with one data type and one storage backend consistently
-   You need zero dependencies (use built-in libraries like shelve)
-   You require advanced database features like transactions or indexing
-   You need fine-grained control over every storage operation

## Changes

-   0.1.8 - Auto scheme registration enhancement
-   0.1.7 - Cache expiry can be set to 0 to never expire cache entries.
-   0.1.6 - Scheme-based backend detection and Options system with automatic backend detection from URI schemes
    -   Unified Options dataclass system replacing separate config classes
    -   Cross-backend access from single store instance
    -   Dataset support with automatic multi-file detection
    -   Enhanced error handling with dependency management
    -   Breaking: Removed manual backend registration
    -   Breaking: Replaced `S3StorageConfig` with `S3Options`
-   0.1.5 - Added StorePath to use the Autostore instance in path-like operations
-   0.1.4 - parquet and csv are loaded as LazyFrames by default and sparse matrices are now saved as .sparse.npz
-   0.1.3
    -   Refactored to use different storage backends including local file system and S3.
    -   Implement S3 storage backend with basic operations
    -   Added S3StorageConfig for configuration management.
    -   Implemented S3Backend class for handling S3 interactions.
    -   Included methods for file operations: upload, download, delete, copy, move, and list files.
    -   Added support for directory-like structures in S3.
    -   Implemented metadata retrieval for files.
    -   Integrated error handling for common S3 exceptions.
    -   Added support for multipart uploads and downloads.
    -   Included utility functions for path parsing and glob pattern matching.
    -   Calling store.keys() now only returns keys without extensions.
-   0.1.2 - config, setup_logging, and load_dotenv are now imported at the module top level
-   0.1.1 - Added config, setup_logging, and load_dotenv
-   0.1.0 - Initial release
