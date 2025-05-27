# AutoStore - File Storage Made Simple

AutoStore provides a dictionary-like interface for reading and writing files with caching and different storage backends.

AutoStore eliminates the cognitive overhead of managing different file formats, letting you focus on your data and analysis rather than the mechanics of file I/O. It automatically handles file format detection, type inference, upload/download operations, and provides a clean, intuitive API for data persistence across local and cloud storage.

## Why Use AutoStore?

-   **Simplicity**: Store and retrieve data with dictionary syntax. No need to remember APIs for different file formats.
-   **Caching**: Caching system with configurable expiration reduces redundant downloads, especially for cloud storage.
-   **Multiple Storage Backends**: Seamlessly work with local files, S3, and other cloud storage services.
-   **Type Detection**: Automatically infers the best file format based on the data type.
-   **Multiple Data Types**: Built-in support for Polars DataFrames, JSON, CSV, images, PyTorch models, NumPy arrays, and more.
-   **Extensible Architecture**: Pluggable handler system for new data types and storage backends.
-   **Performance Optimized**: Upload/download operations with efficient handling of large files.
-   **Type-Safe Configuration**: Dataclass-based configuration with IDE support and validation.

## Getting Started

AutoStore requires Python 3.10+ and can be installed via pip.

```bash
pip install autostore
```

### Basic Usage

```python
from autostore import AutoStore

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

### Cloud Storage (S3)

```python
from autostore import AutoStore
from autostore.s3 import S3Backend, S3StorageConfig

# Register S3 backend
AutoStore.register_backend("s3", S3Backend)

# Configure S3 with caching
s3_config = S3StorageConfig(
    region_name="us-east-1",
    cache_enabled=True,
    cache_expiry_hours=12,
    multipart_threshold=64 * 1024 * 1024  # 64MB
)
store = AutoStore("s3://my-bucket/data/", config=s3_config)

# Write data to S3
store["experiment/results"] = {"accuracy": 0.95, "epochs": 100}

# Read data from S3
results = store["experiment/results"]  # Uses cache on subsequent loads
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

## Configuration Options

### S3StorageConfig

```python
from s3 import S3StorageConfig

config = S3StorageConfig(
    aws_access_key_id="your-key",
    aws_secret_access_key="your-secret",
    region_name="us-east-1",
    cache_enabled=True,
    cache_expiry_hours=12,
    multipart_threshold=64 * 1024 * 1024,  # Files larger than this use multipart upload
    multipart_chunksize=16 * 1024 * 1024,  # Chunk size for multipart uploads
    max_concurrency=10                     # Maximum concurrent uploads/downloads
)
```

## Advanced Features

### Caching System

AutoStore includes an caching system for S3 that:

-   Stores frequently accessed files locally
-   Uses ETags for cache validation
-   Automatically expires old cache entries

```python
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

## Multiple Storage Backends

AutoStore supports pluggable storage backends:

```python
# Local storage
local_store = AutoStore("./data")

# S3 storage
s3_store = AutoStore("s3://bucket/prefix/")
```

## Performance Considerations

### Large File Handling

AutoStore automatically optimizes for large files:

-   Multipart uploads/downloads for files > 64MB
-   Configurable chunk sizes and concurrency
-   Streaming operations to minimize memory usage

## When to Use AutoStore

Choose AutoStore when you need:

-   **Data science projects** with mixed file types and cloud storage
-   **Building data pipelines** with heterogeneous data sources
-   **Rapid prototyping** where you don't want to think about file formats
-   **Consistent data access patterns** across local and cloud environments
-   **Performance optimization** through intelligent caching
-   **Easy extensibility** for custom data types and storage backends
-   **Type-safe configuration** with dataclass-based settings

Don't choose AutoStore when:

-   You need complex queries (use TinyDB or databases)
-   You only work with one data type consistently
-   You need zero dependencies (use Shelve)
-   You require advanced database features

## Comparison with Alternatives

| Feature                   | AutoStore           | Shelve         | DiskCache      | TinyDB        | PickleDB      | SQLiteDict     |
| ------------------------- | ------------------- | -------------- | -------------- | ------------- | ------------- | -------------- |
| **Multi-format Support**  | ✅ 12+ formats      | ❌ Pickle only | ❌ Pickle only | ❌ JSON only  | ❌ JSON only  | ❌ Pickle only |
| **Auto Format Detection** | ✅ Smart inference  | ❌ Manual      | ❌ Manual      | ❌ Manual     | ❌ Manual     | ❌ Manual      |
| **Cloud Storage**         | ✅ S3, extensible   | ❌ Local only  | ❌ Local only  | ❌ Local only | ❌ Local only | ❌ Local only  |
| **Intelligent Caching**   | ✅ ETag-based       | ❌ None        | ✅ Advanced    | ❌ None       | ❌ None       | ❌ None        |
| **Type-Safe Config**      | ✅ Dataclasses      | ❌ None        | ✅ Classes     | ❌ Dicts      | ❌ None       | ❌ None        |
| **Large File Handling**   | ✅ Multipart        | ❌ Limited     | ✅ Good        | ❌ Limited    | ❌ Limited    | ❌ Limited     |
| **Extensibility**         | ✅ Handler system   | ❌ Limited     | ❌ Limited     | ✅ Middleware | ❌ Limited    | ❌ Limited     |
| **Performance**           | ✅ Cached/Optimized | 🔶 Medium      | ✅ Fast        | 🔶 Medium     | 🔶 Medium     | 🔶 Medium      |
| **Standard Library**      | ❌ External         | ✅ Built-in    | ❌ External    | ❌ External   | ❌ External   | ❌ External    |

## Changes

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
