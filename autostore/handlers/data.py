from pathlib import Path
from typing import Any, List
from autostore.handlers.base import DataHandler


class ParquetHandler(DataHandler):
    """Handler for Parquet files using Polars with dataset support."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".parquet"

    def can_handle_data(self, data: Any) -> bool:
        try:
            import polars as pl

            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        try:
            import polars as pl

            return pl.scan_parquet(file_path)
        except ImportError:
            raise ImportError("Polars is required to load Parquet files")

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        try:
            import polars as pl

            file_path.parent.mkdir(parents=True, exist_ok=True)

            if hasattr(data, "write_parquet"):
                data.write_parquet(file_path)
            else:
                pl.DataFrame(data).write_parquet(file_path)
        except ImportError:
            raise ImportError("Polars is required to save Parquet files")

    def read_dataset(self, dataset_path: Path, file_pattern: str = "*") -> Any:
        """Read Parquet dataset with multiple files."""
        try:
            import polars as pl

            pattern = f"{dataset_path}/**/*.parquet" if file_pattern == "*" else f"{dataset_path}/**/{file_pattern}"
            return pl.scan_parquet(pattern)
        except ImportError:
            raise ImportError("Polars is required to load Parquet datasets")

    @property
    def extensions(self) -> List[str]:
        return [".parquet"]

    @property
    def priority(self) -> int:
        return 10


class CSVHandler(DataHandler):
    """Handler for CSV files using Polars."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".csv"

    def can_handle_data(self, data: Any) -> bool:
        try:
            import polars as pl

            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        try:
            import polars as pl

            return pl.scan_csv(file_path)
        except ImportError:
            raise ImportError("Polars is required to load CSV files")

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        try:
            import polars as pl

            file_path.parent.mkdir(parents=True, exist_ok=True)

            if hasattr(data, "write_csv"):
                data.write_csv(file_path)
            else:
                pl.DataFrame(data).write_csv(file_path)
        except ImportError:
            raise ImportError("Polars is required to save CSV files")

    @property
    def extensions(self) -> List[str]:
        return [".csv"]

    @property
    def priority(self) -> int:
        return 9


class NumpyHandler(DataHandler):
    """Handler for NumPy array files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".npy", ".npz"]

    def can_handle_data(self, data: Any) -> bool:
        try:
            import numpy as np

            return isinstance(data, np.ndarray)
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        try:
            import numpy as np

            if file_extension.lower() == ".npz":
                return np.load(file_path)
            return np.load(file_path)
        except ImportError:
            raise ImportError("NumPy is required to load .npy/.npz files")

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        try:
            import numpy as np

            file_path.parent.mkdir(parents=True, exist_ok=True)

            if file_extension.lower() == ".npz":
                np.savez_compressed(file_path, data=data)
            else:
                np.save(file_path, data)
        except ImportError:
            raise ImportError("NumPy is required to save .npy/.npz files")

    @property
    def extensions(self) -> List[str]:
        return [".npy", ".npz"]

    @property
    def priority(self) -> int:
        return 8


class SparseHandler(DataHandler):
    """Handler for sparse matrices."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".sparse.npz"

    def can_handle_data(self, data: Any) -> bool:
        try:
            import scipy.sparse

            return scipy.sparse.issparse(data)
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        try:
            import scipy.sparse

            return scipy.sparse.load_npz(file_path)
        except ImportError:
            raise ImportError("SciPy is required to load sparse matrices")

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        try:
            import scipy.sparse

            file_path.parent.mkdir(parents=True, exist_ok=True)
            scipy.sparse.save_npz(file_path, data)
        except ImportError:
            raise ImportError("SciPy is required to save sparse matrices")

    @property
    def extensions(self) -> List[str]:
        return [".sparse.npz"]

    @property
    def priority(self) -> int:
        return 8


class ImageHandler(DataHandler):
    """Handler for image files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff"]

    def can_handle_data(self, data: Any) -> bool:
        try:
            from PIL import Image

            return isinstance(data, Image.Image)
        except ImportError:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        try:
            from PIL import Image

            return Image.open(file_path)
        except ImportError:
            raise ImportError("Pillow is required to load image files")

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        try:
            from PIL import Image

            file_path.parent.mkdir(parents=True, exist_ok=True)

            if hasattr(data, "save"):
                data.save(file_path)
            else:
                Image.fromarray(data).save(file_path)
        except ImportError:
            raise ImportError("Pillow is required to save image files")

    @property
    def extensions(self) -> List[str]:
        return [".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff"]

    @property
    def priority(self) -> int:
        return 7
