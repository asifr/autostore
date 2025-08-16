import json
from pathlib import Path
from typing import Any, List
from autostore.handlers.base import DataHandler


class TorchHandler(DataHandler):
    """Handler for PyTorch model files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".pt", ".pth"]

    def can_handle_data(self, data: Any) -> bool:
        try:
            import torch

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

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        try:
            import torch

            return torch.load(file_path, map_location="cpu")
        except ImportError:
            raise ImportError("PyTorch is required to load .pt/.pth files")

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        try:
            import torch

            file_path.parent.mkdir(parents=True, exist_ok=True)
            torch.save(data, file_path)
        except ImportError:
            raise ImportError("PyTorch is required to save .pt/.pth files")

    @property
    def extensions(self) -> List[str]:
        return [".pt", ".pth"]

    @property
    def priority(self) -> int:
        return 9


class PydanticHandler(DataHandler):
    """Handler for Pydantic models."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".pydantic.json"

    def can_handle_data(self, data: Any) -> bool:
        try:
            return hasattr(data, "model_dump") or hasattr(data, "dict")
        except Exception:
            return False

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        # This is a generic handler - actual model class needed for reconstruction
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Try different ways to serialize Pydantic models
        if hasattr(data, "model_dump"):
            # Pydantic v2
            serialized = data.model_dump()
        elif hasattr(data, "dict"):
            # Pydantic v1
            serialized = data.dict()
        else:
            raise TypeError(f"Cannot serialize {type(data)} as Pydantic model")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(serialized, f, default=str, indent=2)

    @property
    def extensions(self) -> List[str]:
        return [".pydantic.json"]

    @property
    def priority(self) -> int:
        return 7


class DataclassHandler(DataHandler):
    """Handler for Python dataclasses."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".dataclass.json"

    def can_handle_data(self, data: Any) -> bool:
        import dataclasses

        return dataclasses.is_dataclass(data)

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        # This is a generic handler - actual dataclass type needed for reconstruction
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        import dataclasses

        if not dataclasses.is_dataclass(data):
            raise TypeError(f"Cannot save {type(data)} as dataclass. Expected dataclass instance")

        file_path.parent.mkdir(parents=True, exist_ok=True)
        serialized = dataclasses.asdict(data)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(serialized, f, default=str, indent=2)

    @property
    def extensions(self) -> List[str]:
        return [".dataclass.json"]

    @property
    def priority(self) -> int:
        return 6
