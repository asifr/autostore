from pathlib import Path
from typing import Any, List
from abc import ABC, abstractmethod


class DataHandler(ABC):
    """Abstract base class for all data handlers with dataset support."""

    @abstractmethod
    def can_handle_extension(self, extension: str) -> bool:
        """Check if this handler can handle the given file extension."""
        pass

    @abstractmethod
    def can_handle_data(self, data: Any) -> bool:
        """Check if this handler can handle the given data instance for writing."""
        pass

    @abstractmethod
    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        """Read data from file."""
        pass

    @abstractmethod
    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        """Write data to file."""
        pass

    @property
    @abstractmethod
    def extensions(self) -> List[str]:
        """List of file extensions this handler supports."""
        pass

    @property
    @abstractmethod
    def priority(self) -> int:
        """Priority for type inference (higher = more preferred)."""
        pass

    def can_handle_dataset(self, path: Path) -> bool:
        """Check if this handler can handle datasets in the given path."""
        # Default implementation: check if any files match our extensions
        for ext in self.extensions:
            if any(path.glob(f"*{ext}")) or any(path.rglob(f"*{ext}")):
                return True
        return False

    def read_dataset(self, dataset_path: Path, file_pattern: str = "*") -> Any:
        """Read data from a dataset (multiple files). Override for custom behavior."""
        # Default implementation: find all matching files and read the first one
        # Subclasses should override for proper dataset handling
        for ext in self.extensions:
            pattern = f"*{ext}" if file_pattern == "*" else file_pattern
            files = list(dataset_path.glob(pattern)) + list(dataset_path.rglob(pattern))
            if files:
                return self.read_from_file(files[0], ext)
        raise ValueError(f"No compatible files found in dataset: {dataset_path}")

    def write_dataset(self, data: Any, dataset_path: Path, partition_strategy: str = "auto") -> None:
        """Write data as a dataset (potentially multiple files). Override for custom behavior."""
        # Default implementation: write as single file
        dataset_path.mkdir(parents=True, exist_ok=True)
        ext = self.extensions[0] if self.extensions else ".dat"
        file_path = dataset_path / f"data{ext}"
        self.write_to_file(data, file_path, ext)
