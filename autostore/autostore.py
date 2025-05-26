"""
AutoStore - File Storage Made Simple

AutoStore is a Python library that provides a dictionary-like interface for reading and writing files.
AutoStore eliminates the cognitive overhead of managing different file formats, letting you focus on your data and
analysis rather than the mechanics of file I/O. It automatically handles file format detection, type inference, and
provides a clean, intuitive API for data persistence.

Why Use AutoStore?

- Simplicity: Store and retrieve data with dictionary syntax. No need to remember APIs for different file formats.
- Type Detection: Automatically infers the best file format based on the data type.
- Multiple Data Types: Built-in support for Polars DataFrames, JSON, CSV, images, PyTorch models, NumPy arrays, and more.
- Extensible Architecture: Pluggable handler system for new data types without modifying core code.
- Flexible File Management: Works with nested directories, supports pattern matching, and automatic file discovery.
- Built-in Archiving: Create and extract zip archives.

```python
store = AutoStore("./data")
store["my_dataframe"] = df           # Automatically saves as .parquet
store["config"] = {"key": "value"}   # Automatically saves as .json
store["logs"] = [{"event": "start"}] # Automatically saves as .jsonl
df = store["my_dataframe"]           # Loads and returns the DataFrame
```

Supported Data Types Out of the Box

| Data Type | File Extension | Description |
|-----------|----------------|-------------|
| Polars DataFrame/LazyFrame | `.parquet`, `.csv` | High-performance DataFrames |
| Python dict/list | `.json` | Standard JSON serialization |
| List of dicts | `.jsonl` | JSON Lines format |
| Pydantic models | `.pydantic.json` | Structured data models |
| Python dataclasses | `.dataclass.json` | Dataclass serialization |
| String data | `.txt`, `.html` | Plain text files |
| NumPy arrays | `.npy`, `.npz` | Numerical data |
| SciPy sparse matrices | `.sparse` | Sparse matrix data |
| PyTorch tensors/models | `.pt`, `.pth` | Deep learning models |
| PIL/Pillow images | `.png`, `.jpg`, etc. | Image data |
| YAML data | `.yaml`, `.yml` | Human-readable config files |
| Any Python object | `.pkl` | Pickle fallback |

## When to Use AutoStore

- Data science projects with mixed file types
- Configuration management across different formats
- Rapid prototyping where you don't want to think about file formats
- Building data pipelines with heterogeneous data
- Projects that need to support multiple serialization formats
- Consistent data access patterns across projects
- Easy extensibility for custom data types
- Reduced boilerplate code for file I/O
- Automatic best-practice file format selection

Quick Start

```python
from pathlib import Path
from AutoStore import AutoStore

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

Extending AutoStore

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
        return 10

# Register the handler
ds.register_handler(CustomHandler())
```

When to Choose AutoStore

Choose AutoStore when you need:

- Multiple file formats with automatic selection
- Data science workflow optimization
- Extensibility for custom data types
- Simple dictionary-like interface for complex storage needs

Don't choose AutoStore when:

- You need complex queries (use TinyDB)
- Performance is absolutely critical (use DiskCache)
- You need zero dependencies (use Shelve)
- You only work with one data type consistently
- You need advanced caching features (use Klepto)

| Feature | AutoStore | Shelve | DiskCache | TinyDB | PickleDB | SQLiteDict | Klepto |
|---------|-----------|--------|-----------|--------|----------|------------|--------|
| **Multi-format Support** | ✅ 12+ formats | ❌ Pickle only | ❌ Pickle only | ❌ JSON only | ❌ JSON only | ❌ Pickle only | ❌ Pickle only |
| **Auto Format Detection** | ✅ Smart inference | ❌ Manual | ❌ Manual | ❌ Manual | ❌ Manual | ❌ Manual | ❌ Manual |
| **Extensibility** | ✅ Handler system | ❌ Limited | ❌ Limited | ✅ Middleware | ❌ Limited | ❌ Limited | ✅ Keymaps |
| **Standard Library** | ❌ External | ✅ Built-in | ❌ External | ❌ External | ❌ External | ❌ External | ❌ External |
| **Performance** | 🔶 Variable | 🔶 Medium | ✅ Fast | 🔶 Medium | 🔶 Medium | 🔶 Medium | ✅ Fast |
| **Thread Safety** | ⚠️ Format dependent | ⚠️ Limited | ✅ Yes | ❌ No | ❌ No | ✅ Yes | ✅ Yes |
| **Query Capabilities** | ❌ Key-only | ❌ Key-only | ❌ Key-only | ✅ Rich queries | ❌ Key-only | ❌ Key-only | ❌ Key-only |
| **Data Science Focus** | ✅ Strong | ❌ Generic | ❌ Caching | ❌ Documents | ❌ Generic | ❌ Generic | ✅ Scientific |

Changes
-------
0.1.0 - Initial release
"""

import os
import json
import pickle
import zipfile
import typing as t
from pathlib import Path
from fnmatch import fnmatch
from abc import ABC, abstractmethod


class DataHandler(ABC):
    """Abstract base class for all data handlers."""

    @abstractmethod
    def can_handle_extension(self, extension: str) -> bool:
        """Check if this handler can handle the given file extension."""
        pass

    @abstractmethod
    def can_handle_data(self, data: t.Any) -> bool:
        """Check if this handler can handle the given data instance for writing."""
        pass

    @abstractmethod
    def read(self, file_path: Path) -> t.Any:
        """Read data from the file path."""
        pass

    @abstractmethod
    def write(self, data: t.Any, file_path: Path) -> None:
        """Write data to the file path."""
        pass

    @property
    @abstractmethod
    def extensions(self) -> t.List[str]:
        """List of file extensions this handler supports."""
        pass

    @property
    @abstractmethod
    def priority(self) -> int:
        """Priority for type inference (higher = more preferred). Default: 0"""
        pass


class ParquetHandler(DataHandler):
    """Handler for Parquet files using Polars."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".parquet"

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import polars as pl  # type: ignore

            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False

    def read(self, file_path: Path) -> t.Any:
        try:
            import polars as pl  # type: ignore

            return pl.scan_parquet(file_path)
        except ImportError:
            raise ImportError("Polars is required to load .parquet files")

    def write(self, data: t.Any, file_path: Path) -> None:
        try:
            import polars as pl  # type: ignore

            if isinstance(data, pl.LazyFrame):
                data = data.collect()
            if isinstance(data, pl.DataFrame):
                file_path.parent.mkdir(parents=True, exist_ok=True)
                data.write_parquet(file_path)
            else:
                raise TypeError(f"Cannot save {type(data)} as parquet. Expected DataFrame or LazyFrame")
        except ImportError:
            raise ImportError("Polars is required to save .parquet files")

    @property
    def extensions(self) -> t.List[str]:
        return [".parquet"]

    @property
    def priority(self) -> int:
        return 10  # High priority for DataFrames


class CSVHandler(DataHandler):
    """Handler for CSV files using Polars."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".csv"

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import polars as pl  # type: ignore

            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False

    def read(self, file_path: Path) -> t.Any:
        try:
            import polars as pl  # type: ignore

            return pl.scan_csv(file_path)
        except ImportError:
            raise ImportError("Polars is required to load .csv files")

    def write(self, data: t.Any, file_path: Path) -> None:
        try:
            import polars as pl  # type: ignore

            if isinstance(data, pl.LazyFrame):
                data = data.collect()
            if isinstance(data, pl.DataFrame):
                file_path.parent.mkdir(parents=True, exist_ok=True)
                data.write_csv(file_path)
            else:
                raise TypeError(f"Cannot save {type(data)} as CSV. Expected DataFrame or LazyFrame")
        except ImportError:
            raise ImportError("Polars is required to save .csv files")

    @property
    def extensions(self) -> t.List[str]:
        return [".csv"]

    @property
    def priority(self) -> int:
        return 5  # Lower priority than Parquet for DataFrames


class JSONHandler(DataHandler):
    """Handler for JSON files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".json"

    def can_handle_data(self, data: t.Any) -> bool:
        return isinstance(data, (dict, list, int, float, bool, type(None)))

    def read(self, file_path: Path) -> t.Any:
        with open(file_path, "r") as f:
            return json.load(f)

    def write(self, data: t.Any, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f, default=str)

    @property
    def extensions(self) -> t.List[str]:
        return [".json"]

    @property
    def priority(self) -> int:
        return 8  # High priority for standard Python types


class JSONLHandler(DataHandler):
    """Handler for JSON Lines files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".jsonl"

    def can_handle_data(self, data: t.Any) -> bool:
        # Handle lists of dictionaries for JSONL
        return isinstance(data, list) and len(data) > 0 and all(isinstance(item, dict) for item in data)

    def read(self, file_path: Path) -> t.List[t.Any]:
        with open(file_path, "r") as f:
            return [json.loads(line.strip()) for line in f if line.strip()]

    def write(self, data: t.Any, file_path: Path) -> None:
        if not isinstance(data, list):
            raise TypeError(f"Cannot save {type(data)} as JSONL. Expected list")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            for item in data:
                json.dump(item, f, default=str)
                f.write("\n")

    @property
    def extensions(self) -> t.List[str]:
        return [".jsonl"]

    @property
    def priority(self) -> int:
        return 6  # Medium priority


class TorchHandler(DataHandler):
    """Handler for PyTorch model files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".pt", ".pth"]

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import torch  # type: ignore

            return (
                isinstance(data, torch.Tensor)
                or hasattr(data, "state_dict")
                or (
                    hasattr(data, "__class__")
                    and hasattr(data.__class__, "__module__")
                    and "torch" in str(data.__class__.__module__)
                )
            )
        except ImportError:
            return False

    def read(self, file_path: Path) -> t.Any:
        try:
            import torch  # type: ignore

            return torch.load(file_path, map_location="cpu")
        except ImportError:
            raise ImportError("PyTorch is required to load .pt/.pth files")

    def write(self, data: t.Any, file_path: Path) -> None:
        try:
            import torch  # type: ignore

            file_path.parent.mkdir(parents=True, exist_ok=True)
            torch.save(data, file_path)
        except ImportError:
            raise ImportError("PyTorch is required to save .pt/.pth files")

    @property
    def extensions(self) -> t.List[str]:
        return [".pt", ".pth"]

    @property
    def priority(self) -> int:
        return 9


class PickleHandler(DataHandler):
    """Handler for Pickle files - fallback for any Python object."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".pkl", ".pickle"]

    def can_handle_data(self, data: t.Any) -> bool:
        # Pickle can handle any Python object, but lowest priority
        return True

    def read(self, file_path: Path) -> t.Any:
        with open(file_path, "rb") as f:
            return pickle.load(f)

    def write(self, data: t.Any, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            pickle.dump(data, f)

    @property
    def extensions(self) -> t.List[str]:
        return [".pkl", ".pickle"]

    @property
    def priority(self) -> int:
        return 1  # Lowest priority - fallback option


class NumpyHandler(DataHandler):
    """Handler for NumPy arrays."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".npy", ".npz"]

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            import numpy as np  # type: ignore

            return isinstance(data, np.ndarray)
        except ImportError:
            return False

    def read(self, file_path: Path) -> t.Any:
        try:
            import numpy as np  # type: ignore

            return np.load(file_path)
        except ImportError:
            raise ImportError("NumPy is required to load .npy/.npz files")

    def write(self, data: t.Any, file_path: Path) -> None:
        try:
            import numpy as np  # type: ignore

            file_path.parent.mkdir(parents=True, exist_ok=True)
            if file_path.suffix.lower() == ".npy":
                if isinstance(data, np.ndarray):
                    np.save(file_path, data)
                else:
                    raise TypeError(f"Cannot save {type(data)} as .npy. Expected numpy array")
            elif file_path.suffix.lower() == ".npz":
                if isinstance(data, dict):
                    np.savez(file_path, **data)
                elif isinstance(data, np.ndarray):
                    np.savez(file_path, data)
                else:
                    raise TypeError(f"Cannot save {type(data)} as .npz. Expected dict or numpy array")
        except ImportError:
            raise ImportError("NumPy is required to save .npy/.npz files")

    @property
    def extensions(self) -> t.List[str]:
        return [".npy", ".npz"]

    @property
    def priority(self) -> int:
        return 9


class TextHandler(DataHandler):
    """Handler for plain text files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".txt", ".html", ".md"]

    def can_handle_data(self, data: t.Any) -> bool:
        return isinstance(data, str)

    def read(self, file_path: Path) -> str:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def write(self, data: t.Any, file_path: Path) -> None:
        if not isinstance(data, str):
            raise TypeError(f"Cannot save {type(data)} as text. Expected string")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(data)

    @property
    def extensions(self) -> t.List[str]:
        return [".txt", ".html", ".md"]

    @property
    def priority(self) -> int:
        return 7


class SparseHandler(DataHandler):
    """Handler for SciPy sparse matrices."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".sparse"

    def can_handle_data(self, data: t.Any) -> bool:
        try:
            from scipy import sparse  # type: ignore

            return sparse.issparse(data)
        except ImportError:
            return False

    def read(self, file_path: Path) -> t.Any:
        try:
            from scipy import sparse  # type: ignore

            return sparse.load_npz(file_path)
        except ImportError:
            raise ImportError("SciPy is required to load .sparse files")

    def write(self, data: t.Any, file_path: Path) -> None:
        try:
            from scipy import sparse  # type: ignore

            if not sparse.issparse(data):
                raise TypeError(f"Cannot save {type(data)} as .sparse. Expected scipy sparse matrix")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            sparse.save_npz(file_path, data)
        except ImportError:
            raise ImportError("SciPy is required to save .sparse files")

    @property
    def extensions(self) -> t.List[str]:
        return [".sparse"]

    @property
    def priority(self) -> int:
        return 9


class PydanticHandler(DataHandler):
    """Handler for Pydantic BaseModel instances."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".pydantic.json"

    def can_handle_data(self, data: t.Any) -> bool:
        # Check if it's a Pydantic BaseModel instance
        return (
            hasattr(data, "model_dump")
            and hasattr(data, "model_validate")
            and hasattr(data.__class__, "__pydantic_core_schema__")
        )

    def read(self, file_path: Path) -> t.Any:
        # This would need the original model class to reconstruct
        # For demo purposes, just return the JSON data
        with open(file_path, "r") as f:
            return json.load(f)

    def write(self, data: t.Any, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            # Use Pydantic's model_dump method
            json.dump(data.model_dump(), f, indent=2, default=str)

    @property
    def extensions(self) -> t.List[str]:
        return [".pydantic.json"]

    @property
    def priority(self) -> int:
        return 12  # Higher than regular JSON for Pydantic models


class DataclassHandler(DataHandler):
    """Handler for Python dataclass instances."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".dataclass.json"

    def can_handle_data(self, data: t.Any) -> bool:
        # Check if it's a dataclass instance
        return hasattr(data, "__dataclass_fields__")

    def read(self, file_path: Path) -> t.Any:
        # Similar limitation as Pydantic - would need original class
        with open(file_path, "r") as f:
            return json.load(f)

    def write(self, data: t.Any, file_path: Path) -> None:
        import dataclasses

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            # Use dataclasses.asdict for serialization
            json.dump(dataclasses.asdict(data), f, indent=2, default=str)

    @property
    def extensions(self) -> t.List[str]:
        return [".dataclass.json"]

    @property
    def priority(self) -> int:
        return 11  # Higher than regular JSON for dataclasses


class YAMLHandler(DataHandler):
    """Custom handler for YAML files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".yaml", ".yml"]

    def can_handle_data(self, data: t.Any) -> bool:
        # YAML can handle basic Python types like JSON
        return isinstance(data, (dict, list, str, int, float, bool, type(None)))

    def read(self, file_path: Path) -> t.Any:
        try:
            import yaml  # type: ignore

            with open(file_path, "r") as f:
                return yaml.safe_load(f)
        except ImportError:
            raise ImportError("PyYAML is required to load YAML files")

    def write(self, data: t.Any, file_path: Path) -> None:
        try:
            import yaml  # type: ignore

            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w") as f:
                yaml.dump(data, f, default_flow_style=False)
        except ImportError:
            raise ImportError("PyYAML is required to save YAML files")

    @property
    def extensions(self) -> t.List[str]:
        return [".yaml", ".yml"]

    @property
    def priority(self) -> int:
        return 7  # Same as JSON for basic types


class ImageHandler(DataHandler):
    """Handler for PIL/Pillow Image objects."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".png", ".jpg", ".jpeg", ".bmp", ".tiff"]

    def can_handle_data(self, data: t.Any) -> bool:
        # Check if it's a PIL Image
        return (
            hasattr(data, "save")
            and hasattr(data, "mode")
            and hasattr(data, "size")
            and hasattr(data.__class__, "__module__")
            and "PIL" in str(data.__class__.__module__)
        )

    def read(self, file_path: Path) -> t.Any:
        try:
            from PIL import Image  # type: ignore

            return Image.open(file_path)
        except ImportError:
            raise ImportError("Pillow is required to load image files")

    def write(self, data: t.Any, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        # PIL Image objects have a save method
        data.save(file_path)

    @property
    def extensions(self) -> t.List[str]:
        return [".png", ".jpg", ".jpeg", ".bmp", ".tiff"]

    @property
    def priority(self) -> int:
        return 10


class HandlerRegistry:
    """Registry for managing data handlers."""

    def __init__(self):
        self._handlers: t.List[DataHandler] = []
        self._extension_map: t.Dict[str, t.List[DataHandler]] = {}
        self._register_default_handlers()

    def _register_default_handlers(self):
        """Register all default handlers."""
        default_handlers = [
            ParquetHandler(),
            CSVHandler(),
            JSONHandler(),
            JSONLHandler(),
            PydanticHandler(),
            YAMLHandler(),
            ImageHandler(),
            TorchHandler(),
            NumpyHandler(),
            SparseHandler(),
            TextHandler(),
            PickleHandler(),  # Keep pickle as fallback
        ]

        for handler in default_handlers:
            self.register(handler)

    def register(self, handler: DataHandler) -> None:
        """Register a new handler."""
        self._handlers.append(handler)

        # Update extension mapping
        for ext in handler.extensions:
            ext_lower = ext.lower()
            if ext_lower not in self._extension_map:
                self._extension_map[ext_lower] = []
            self._extension_map[ext_lower].append(handler)
            # Sort by priority (higher priority first)
            self._extension_map[ext_lower].sort(key=lambda h: h.priority, reverse=True)

    def unregister(self, handler_class: t.Type[DataHandler]) -> None:
        """Unregister a handler by class type."""
        # Remove from main list
        self._handlers = [h for h in self._handlers if not isinstance(h, handler_class)]

        # Rebuild extension mapping
        self._extension_map.clear()
        for handler in self._handlers:
            for ext in handler.extensions:
                ext_lower = ext.lower()
                if ext_lower not in self._extension_map:
                    self._extension_map[ext_lower] = []
                self._extension_map[ext_lower].append(handler)
                self._extension_map[ext_lower].sort(key=lambda h: h.priority, reverse=True)

    def get_handler_for_extension(self, extension: str) -> t.Optional[DataHandler]:
        """Get the best handler for a given file extension."""
        ext_lower = extension.lower()
        handlers = self._extension_map.get(ext_lower, [])
        return handlers[0] if handlers else None

    def get_handler_for_data(self, data: t.Any) -> t.Optional[DataHandler]:
        """Get the best handler for a given data instance."""
        compatible_handlers = []
        for handler in self._handlers:
            if handler.can_handle_data(data):
                compatible_handlers.append(handler)

        # Sort by priority and return the best match
        compatible_handlers.sort(key=lambda h: h.priority, reverse=True)
        return compatible_handlers[0] if compatible_handlers else None

    def get_supported_extensions(self) -> t.List[str]:
        """Get all supported file extensions."""
        return list(self._extension_map.keys())


class AutoStore:
    """
    Read and write files like a dictionary.

    Supported data types:
    - Parquet (.parquet)
    - CSV (.csv)
    - JSON (.json)
    - JSON Lines (.jsonl)
    - YAML (.yaml, .yml)
    - Images (.png, .jpg, .jpeg, .bmp, .tiff)
    - Pydantic models (.pydantic.json)
    - Dataclass instances (.dataclass.json)
    - Torch model weights (.pt, .pth)
    - Pickle (.pkl, .pickle)
    - Numpy arrays (.npy, .npz)
    - Sparse numpy arrays (.sparse)
        - Sparse matrices are stored in scipy's compressed .npz format, only non-zero elements are stored,
        and maintains the specific sparse matrix type (CSR, CSC, etc.)
    - HTML (.html)
    - Text (.txt)

    Examples:

        >>> store = AutoStore(Path.home() / "data")
        >>> store["features] = pl.DataFrame({"id": [1, 2], "value": [0.1, 0.2]})  # Save parquet file
        >>> store["config"] = {"version": "1.0", "debug": True}  # Save JSON file
        >>> store["logs.jsonl"] = [{"event": "start", "time": "2023-01-01T00:00:00Z"}]  # Save JSON Lines
        >>> store["experiments/run_1/weights"] = np.random.rand(100, 768)  # Save Numpy array
        >>> store["features.parquet"]  # Returns a LazyFrame
        >>> store["features"]  # Omit the file extension
        >>> store["models/version_1/model.pt"]  # Nested directory, loads torch model weights
        >>> print(list(ds.keys()))  # All available keys, with and without extensions
        >>> print(list(ds.iter_files()))  # All files with extensions
        >>> del store["old_experiment"]  # Delete a file
        >>> "features" in ds  # Check if a file exists
        >>> store.zip("backup")  # Zips data directory to ../backup.zip
        >>> store.zip("models", output_dir=ds.data_dir / "zips")  # Zips models directory into an output directory
        >>> store.zip("models", pattern="*.pt")  # Only PyTorch files
        >>> store.zip("models", source_path="models", pattern="*.pt")  # Only PyTorch files from a source subdirectory
        >>> store.unzip("backup.zip")  # Unzips backup.zip into the current data directory
        >>> store.unzip("backup.zip", output_dir=ds.data_dir / "extracted")  # Unzips to a specified directory
    """

    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            self.data_dir.mkdir(parents=True, exist_ok=True)
        self.registry = HandlerRegistry()

    def register_handler(self, handler: DataHandler) -> None:
        """Register a custom handler."""
        self.registry.register(handler)

    def unregister_handler(self, handler_class: t.Type[DataHandler]) -> None:
        """Unregister a handler by class type."""
        self.registry.unregister(handler_class)

    def _infer_extension(self, data: t.Any) -> str:
        """Infer the appropriate file extension based on data instance."""
        # Get handler for this data instance
        handler = self.registry.get_handler_for_data(data)
        if handler and handler.extensions:
            return handler.extensions[0]  # Return the first (primary) extension

        # Fallback to pickle if no handler found
        return ".pkl"

    def _find_file(self, key: str) -> Path:
        """Find the actual file path for a given key."""
        # Normalize the key
        key = key.replace("\\", "/")

        # Try direct path first (with extension)
        potential_path = self.data_dir / key
        if potential_path.exists() and potential_path.is_file():
            return potential_path

        # If no extension provided, search for files with supported extensions
        if not Path(key).suffix:
            for ext in self.registry.get_supported_extensions():
                test_path = potential_path.with_suffix(ext)
                if test_path.exists():
                    return test_path

        # Search recursively through subdirectories
        key_lower = key.lower()
        key_stem = Path(key).stem.lower()

        for file_path in self.data_dir.rglob("*"):
            if file_path.is_file() and file_path.suffix.lower() in self.registry.get_supported_extensions():
                rel_path = file_path.relative_to(self.data_dir)
                rel_path_str = str(rel_path).replace("\\", "/")

                # Check exact match (case insensitive)
                if rel_path_str.lower() == key_lower:
                    return file_path

                # Check stem match (filename without extension)
                if rel_path.stem.lower() == key_stem:
                    return file_path

        raise FileNotFoundError(f"No supported file found for key: {key}")

    def __getitem__(self, key: str) -> t.Any:
        """Load and return data for the given key."""
        file_path = self._find_file(key)
        ext = file_path.suffix.lower()

        handler = self.registry.get_handler_for_extension(ext)
        if not handler:
            supported = ", ".join(self.registry.get_supported_extensions())
            raise ValueError(f"Unsupported file type: {ext}. Supported types: {supported}")

        try:
            result = handler.read(file_path)

            return result
        except Exception as e:
            raise RuntimeError(f"Failed to load {file_path}: {str(e)}") from e

    def __setitem__(self, key: str, data: t.Any) -> None:
        """Save data to the given key."""
        # Normalize the key
        key = key.replace("\\", "/")
        potential_path = self.data_dir / key

        # If no extension provided, infer it from the data type
        if not Path(key).suffix:
            extension = self._infer_extension(data)
            potential_path = potential_path.with_suffix(extension)

        # Get the appropriate handler
        ext = potential_path.suffix.lower()
        handler = self.registry.get_handler_for_extension(ext)

        if not handler:
            # Fallback to pickle for unknown extensions
            handler = self.registry.get_handler_for_extension(".pkl")
            potential_path = potential_path.with_suffix(".pkl")

        try:
            handler.write(data, potential_path)
        except Exception as e:
            raise RuntimeError(f"Failed to save data to {potential_path}: {str(e)}") from e

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the data store."""
        try:
            self._find_file(key)
            return True
        except FileNotFoundError:
            return False

    def __delitem__(self, key: str) -> None:
        """Delete a file from the data store."""
        file_path = self._find_file(key)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found for key: {key}")
        if not file_path.is_file():
            raise ValueError(f"Key {key} does not point to a file: {file_path}")
        file_path.unlink()

    def iter_files(self, pattern: str = "*") -> t.Iterator[str]:
        """
        Iterate over all available files matching the pattern.

        Args:
            pattern (str): Glob pattern to match files. Defaults to "*".

        Yields:
            str: Relative file paths matching the pattern.

        Raises:
            ValueError: If the pattern is not a valid glob pattern.

        Examples:

            >>> store = AutoStore(Path.home() / "data")
            >>> list(ds.iter_files("*.json"))  # Iterate over all JSON files
            >>> list(ds.iter_files("config/*.json"))  # Iterate over JSON files in a subdirectory
        """
        supported_extensions = self.registry.get_supported_extensions()
        for file_path in self.data_dir.rglob("*"):
            if file_path.is_file() and file_path.suffix.lower() in supported_extensions:
                rel_path = str(file_path.relative_to(self.data_dir)).replace("\\", "/")
                if fnmatch(rel_path, pattern):
                    yield rel_path

    def keys(self) -> t.Iterator[str]:
        """
        Iterate over all available keys (file paths with and without extensions).

        Yields:
            str: All available keys in the data shelf. Includes both file paths with and without extensions.

        Raises:
            ValueError: If the pattern is not a valid glob pattern.

        Examples:

            >>> store = AutoStore(Path.home() / "data")
            >>> list(ds.keys())  # List all available keys
        """
        seen = set()
        supported_extensions = self.registry.get_supported_extensions()
        for file_path in self.data_dir.rglob("*"):
            if file_path.is_file() and file_path.suffix.lower() in supported_extensions:
                rel_path = file_path.relative_to(self.data_dir)
                rel_path_str = str(rel_path).replace("\\", "/")
                rel_path_no_ext = str(rel_path.with_suffix("")).replace("\\", "/")
                for key in (rel_path_str, rel_path_no_ext):
                    if key not in seen:
                        seen.add(key)
                        yield key

    def __len__(self) -> int:
        """Return the number of files in the data shelf."""
        return sum(1 for _ in self.iter_files("*"))

    def __repr__(self) -> str:
        return f"AutoStore(data_dir='{self.data_dir}', handlers={len(self.registry._handlers)})"

    def zip(
        self,
        name: str,
        output_dir: t.Optional[t.Union[str, Path]] = None,
        source_path: t.Optional[str] = None,
        pattern: str = "*",
    ) -> Path:
        """
        Create a zip archive of the data directory or specified path/pattern.

        Args:
            name (str): Name of the zip file.
            output_dir (str or Path, optional): Directory to save the zip file. Defaults to the parent directory of data_dir.
            source_path (str, optional): Subdirectory to zip. Defaults to the data directory.
            pattern (str, optional): Glob pattern to filter files. Defaults to "*".

        Returns:
            Path: Path to the created zip file.

        Raises:
            FileNotFoundError: If the source directory does not exist.
            RuntimeError: If zipping fails.

        Examples:

            >>> store = AutoStore(Path.home() / "data")
            >>> store.zip("backup")  # Zips the entire data directory
            >>> store.zip("models", output_dir=ds.data_dir / "zips")  # Zips a subdirectory
            >>> store.zip("models", pattern="*.pt")  # Only PyTorch files
            >>> store.zip("models", source_path="models", pattern="*.pt")  # Only PyTorch files from a source subdirectory
        """
        if source_path:
            source_dir = self.data_dir / source_path
        else:
            source_dir = self.data_dir

        if output_dir is None:
            output_dir = self.data_dir.parent
        else:
            output_dir = Path(output_dir)

        name = name.replace("\\", "/")
        if not name.endswith(".zip"):
            name += ".zip"
        output_zip_path = output_dir / name

        output_dir.mkdir(parents=True, exist_ok=True)

        if output_zip_path.exists():
            output_zip_path.unlink()

        original_cwd = os.getcwd()
        try:
            if not source_dir.exists() or not source_dir.is_dir():
                raise FileNotFoundError(f"Source directory not found: {source_dir}")

            os.chdir(source_dir)

            with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk("."):
                    for file in files:
                        file_path = os.path.join(root, file)

                        if pattern != "*":
                            if not fnmatch(file, pattern):
                                rel_path = file_path.replace("\\", "/").lstrip("./")
                                if not fnmatch(rel_path, pattern):
                                    continue

                        zipf.write(file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to create zip file: {str(e)}") from e
        finally:
            os.chdir(original_cwd)

        return output_zip_path

    def unzip(
        self,
        zip_path: t.Union[str, Path],
        output_dir: t.Optional[t.Union[str, Path]] = None,
        delete_zip: bool = False,
    ) -> None:
        """
        Unzip a zip file into the data directory or specified output directory.

        Args:
            zip_path (str or Path): Path to the zip file.
            output_dir (str or Path, optional): Directory to extract files to. Defaults to the data directory.
            delete_zip (bool, optional): Whether to delete the zip file after extraction. Defaults to False.

        Raises:
            FileNotFoundError: If the zip file does not exist.
            RuntimeError: If extraction fails.

        Examples:

            >>> store = AutoStore(Path.home() / "data")
            >>> store.unzip("backup.zip", delete_zip=True)  # Unzips and deletes the zip file
            >>> store.unzip("backup.zip")  # Unzips backup.zip into the current data directory
            >>> store.unzip("backup.zip", output_dir=ds.data_dir / "extracted")  # Unzips to a specified directory
        """
        zip_path = Path(zip_path)
        if not zip_path.exists() or not zip_path.is_file():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

        if output_dir is None:
            output_dir = self.data_dir
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "r") as zipf:
                zipf.extractall(output_dir)
        except Exception as e:
            raise RuntimeError(f"Failed to unzip {zip_path}: {str(e)}") from e
        finally:
            if delete_zip and zip_path.exists():
                zip_path.unlink()
