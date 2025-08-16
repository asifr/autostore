import json
import pickle
from pathlib import Path
from typing import Any, List
from autostore.handlers.base import DataHandler


class JSONHandler(DataHandler):
    """Handler for JSON files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".json"

    def can_handle_data(self, data: Any) -> bool:
        return isinstance(data, (dict, list, int, float, bool, type(None), str))

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str)

    @property
    def extensions(self) -> List[str]:
        return [".json"]

    @property
    def priority(self) -> int:
        return 8


class JSONLHandler(DataHandler):
    """Handler for JSON Lines files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".jsonl"

    def can_handle_data(self, data: Any) -> bool:
        return isinstance(data, list) and len(data) > 0 and all(isinstance(item, dict) for item in data)

    def read_from_file(self, file_path: Path, file_extension: str) -> List[Any]:
        result = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    result.append(json.loads(line))
        return result

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        if not isinstance(data, list):
            raise TypeError(f"Cannot save {type(data)} as JSONL. Expected list")

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, default=str) + "\n")

    @property
    def extensions(self) -> List[str]:
        return [".jsonl"]

    @property
    def priority(self) -> int:
        return 6


class TextHandler(DataHandler):
    """Handler for plain text files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".txt", ".html", ".md"]

    def can_handle_data(self, data: Any) -> bool:
        return isinstance(data, str)

    def read_from_file(self, file_path: Path, file_extension: str) -> str:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        if not isinstance(data, str):
            raise TypeError(f"Cannot save {type(data)} as text. Expected str")

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(data)

    @property
    def extensions(self) -> List[str]:
        return [".txt", ".html", ".md"]

    @property
    def priority(self) -> int:
        return 2


class PickleHandler(DataHandler):
    """Handler for Pickle files - fallback for any data type."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() == ".pkl"

    def can_handle_data(self, data: Any) -> bool:
        # Pickle can handle almost anything
        return True

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        with open(file_path, "rb") as f:
            return pickle.load(f)

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            pickle.dump(data, f)

    @property
    def extensions(self) -> List[str]:
        return [".pkl"]

    @property
    def priority(self) -> int:
        return 1  # Lowest priority - fallback only


class YAMLHandler(DataHandler):
    """Handler for YAML files."""

    def can_handle_extension(self, extension: str) -> bool:
        return extension.lower() in [".yaml", ".yml"]

    def can_handle_data(self, data: Any) -> bool:
        return isinstance(data, (dict, list, int, float, bool, type(None), str))

    def read_from_file(self, file_path: Path, file_extension: str) -> Any:
        try:
            import yaml

            with open(file_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except ImportError:
            raise ImportError("PyYAML is required to load YAML files")

    def write_to_file(self, data: Any, file_path: Path, file_extension: str) -> None:
        try:
            import yaml

            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, default_flow_style=False)
        except ImportError:
            raise ImportError("PyYAML is required to save YAML files")

    @property
    def extensions(self) -> List[str]:
        return [".yaml", ".yml"]

    @property
    def priority(self) -> int:
        return 7
