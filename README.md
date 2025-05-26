# AutoStore - File Storage Made Simple

AutoStore provides a dictionary-like interface for reading and writing files.
AutoStore eliminates the cognitive overhead of managing different file formats, letting you focus on your data and
analysis rather than the mechanics of file I/O. It automatically handles file format detection, type inference, and
provides a clean, intuitive API for data persistence.

## Why Use AutoStore?

-   Simplicity: Store and retrieve data with dictionary syntax. No need to remember APIs for different file formats.
-   Type Detection: Automatically infers the best file format based on the data type.
-   Multiple Data Types: Built-in support for Polars DataFrames, JSON, CSV, images, PyTorch models, NumPy arrays, and more.
-   Extensible Architecture: Pluggable handler system for new data types without modifying core code.
-   Flexible File Management: Works with nested directories, supports pattern matching, and automatic file discovery.
-   Built-in Archiving: Create and extract zip archives.

## Getting Started

AutoStore requires Python 3.10+ and can be installed via pip:

```bash
pip install autostore
```

```python
from autostore import AutoStore
store = AutoStore("./data")

# Write data
store["my_dataframe"] = df           # Automatically saves as .parquet
store["config"] = {"key": "value"}   # Automatically saves as .json
store["logs"] = [{"event": "start"}] # Automatically saves as .jsonl

# Read data
df = store["my_dataframe"]           # Loads and returns the DataFrame
config = store["config"]             # Loads and returns the config dict
logs = store["logs"]                 # Loads and returns the list of logs
```

Supported Data Types Out of the Box

| Data Type                  | File Extension       | Description                 |
| -------------------------- | -------------------- | --------------------------- |
| Polars DataFrame/LazyFrame | `.parquet`, `.csv`   | High-performance DataFrames |
| Python dict/list           | `.json`              | Standard JSON serialization |
| List of dicts              | `.jsonl`             | JSON Lines format           |
| Pydantic models            | `.pydantic.json`     | Structured data models      |
| Python dataclasses         | `.dataclass.json`    | Dataclass serialization     |
| String data                | `.txt`, `.html`      | Plain text files            |
| NumPy arrays               | `.npy`, `.npz`       | Numerical data              |
| SciPy sparse matrices      | `.sparse`            | Sparse matrix data          |
| PyTorch tensors/models     | `.pt`, `.pth`        | Deep learning models        |
| PIL/Pillow images          | `.png`, `.jpg`, etc. | Image data                  |
| YAML data                  | `.yaml`, `.yml`      | Human-readable config files |
| Any Python object          | `.pkl`               | Pickle fallback             |

## When to Use AutoStore

-   Data science projects with mixed file types
-   Configuration management across different formats
-   Rapid prototyping where you don't want to think about file formats
-   Building data pipelines with heterogeneous data
-   Projects that need to support multiple serialization formats
-   Consistent data access patterns across projects
-   Easy extensibility for custom data types
-   Reduced boilerplate code for file I/O
-   Automatic best-practice file format selection

## Quick Start

```python
from pathlib import Path
from autostore import AutoStore

# Create a data shelf
store = AutoStore(Path("./my_data"))

# Save different types of data
store["users"] = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]  # → users.jsonl
store["config"] = {"api_key": "secret", "debug": True}                   # → config.json
store["model_weights"] = torch.randn(100, 50)                            # → model_weights.pt
store["features"] = pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})       # → features.parquet

# Load data back (format detection is automatic)
users = store["users"]           # Loads from users.jsonl
config = store["config"]         # Loads from config.json
weights = store["model_weights"] # Loads from model_weights.pt
df = store["features"]           # Loads from features.parquet

# File operations
print(list(ds.keys()))        # List all available data
"config" in ds                # Check if data exists
del store["old_data"]            # Delete data

# Archive operations
store.zip("backup")              # Create backup.zip
store.unzip("backup.zip")        # Extract archive
```

## Extending AutoStore

Add support for new data types by creating custom handlers:

```python
class CustomHandler(DataHandler):
    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".custom"

    def can_handle_data(self, data: Any) -> bool:
        return isinstance(data, MyCustomType)

    def read(self, file_path: Path) -> Any:
        # Custom loading logic
        pass

    def write(self, data: Any, file_path: Path) -> None:
        # Custom saving logic
        pass

    @property
    def extensions(self) -> List[str]:
        return [".custom"]

    @property
    def priority(self) -> int:
        return 10  # Higher priority means it will be tried first

# Register the handler
store.register_handler(CustomHandler())
```

## When to Choose AutoStore

Choose AutoStore when you need:

-   Multiple file formats with automatic selection
-   Data science workflow optimization
-   Extensibility for custom data types
-   Simple dictionary-like interface for complex storage needs

Don't choose AutoStore when:

-   You need complex queries (use TinyDB)
-   Performance is absolutely critical (use DiskCache)
-   You need zero dependencies (use Shelve)
-   You only work with one data type consistently
-   You need advanced caching features (use Klepto)

| Feature                   | AutoStore           | Shelve         | DiskCache      | TinyDB          | PickleDB     | SQLiteDict     | Klepto         |
| ------------------------- | ------------------- | -------------- | -------------- | --------------- | ------------ | -------------- | -------------- |
| **Multi-format Support**  | ✅ 12+ formats      | ❌ Pickle only | ❌ Pickle only | ❌ JSON only    | ❌ JSON only | ❌ Pickle only | ❌ Pickle only |
| **Auto Format Detection** | ✅ Smart inference  | ❌ Manual      | ❌ Manual      | ❌ Manual       | ❌ Manual    | ❌ Manual      | ❌ Manual      |
| **Extensibility**         | ✅ Handler system   | ❌ Limited     | ❌ Limited     | ✅ Middleware   | ❌ Limited   | ❌ Limited     | ✅ Keymaps     |
| **Standard Library**      | ❌ External         | ✅ Built-in    | ❌ External    | ❌ External     | ❌ External  | ❌ External    | ❌ External    |
| **Performance**           | 🔶 Variable         | 🔶 Medium      | ✅ Fast        | 🔶 Medium       | 🔶 Medium    | 🔶 Medium      | ✅ Fast        |
| **Thread Safety**         | ⚠️ Format dependent | ⚠️ Limited     | ✅ Yes         | ❌ No           | ❌ No        | ✅ Yes         | ✅ Yes         |
| **Query Capabilities**    | ❌ Key-only         | ❌ Key-only    | ❌ Key-only    | ✅ Rich queries | ❌ Key-only  | ❌ Key-only    | ❌ Key-only    |
| **Data Science Focus**    | ✅ Strong           | ❌ Generic     | ❌ Caching     | ❌ Documents    | ❌ Generic   | ❌ Generic     | ✅ Scientific  |

## Changes

-   0.1.0 - Initial release
