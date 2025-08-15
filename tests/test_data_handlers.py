"""Tests for data handlers in AutoStore."""

import pytest
import tempfile
import json
import pickle
from pathlib import Path
from unittest.mock import patch, MagicMock

from autostore.autostore import (
    JSONHandler, JSONLHandler, TextHandler, PickleHandler,
    PydanticHandler, DataclassHandler, HandlerRegistry,
    ParquetHandler, CSVHandler, TorchHandler, YAMLHandler,
    ImageHandler, SparseHandler, NumpyHandler
)


class TestJSONHandler:
    """Test the JSONHandler class."""

    @pytest.fixture
    def handler(self):
        return JSONHandler()

    @pytest.fixture
    def temp_file(self):
        temp_file = Path(tempfile.mktemp(suffix=".json"))
        yield temp_file
        if temp_file.exists():
            temp_file.unlink()

    def test_can_handle_extension(self, handler):
        """Test extension detection."""
        assert handler.can_handle_extension(".json")
        assert handler.can_handle_extension(".JSON")
        assert not handler.can_handle_extension(".txt")
        assert not handler.can_handle_extension(".csv")

    def test_can_handle_data(self, handler):
        """Test data type detection."""
        assert handler.can_handle_data({"key": "value"})
        assert handler.can_handle_data([1, 2, 3])
        assert handler.can_handle_data("string")
        assert handler.can_handle_data(42)
        assert handler.can_handle_data(3.14)
        assert handler.can_handle_data(True)
        assert handler.can_handle_data(None)
        assert not handler.can_handle_data(object())

    def test_write_read_cycle(self, handler, temp_file):
        """Test writing and reading JSON data."""
        test_data = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3],
            "nested": {"inner": "value"}
        }
        
        # Write data
        handler.write_to_file(test_data, temp_file, ".json")
        assert temp_file.exists()
        
        # Read data back
        loaded_data = handler.read_from_file(temp_file, ".json")
        assert loaded_data == test_data

    def test_extensions_property(self, handler):
        """Test extensions property."""
        assert handler.extensions == [".json"]

    def test_priority_property(self, handler):
        """Test priority property."""
        assert handler.priority == 8


class TestJSONLHandler:
    """Test the JSONLHandler class."""

    @pytest.fixture
    def handler(self):
        return JSONLHandler()

    @pytest.fixture
    def temp_file(self):
        temp_file = Path(tempfile.mktemp(suffix=".jsonl"))
        yield temp_file
        if temp_file.exists():
            temp_file.unlink()

    def test_can_handle_extension(self, handler):
        """Test extension detection."""
        assert handler.can_handle_extension(".jsonl")
        assert handler.can_handle_extension(".JSONL")
        assert not handler.can_handle_extension(".json")

    def test_can_handle_data(self, handler):
        """Test data type detection."""
        # Valid JSONL data - list of dicts
        assert handler.can_handle_data([{"key": "value"}, {"key2": "value2"}])
        
        # Invalid cases
        assert not handler.can_handle_data([])  # Empty list
        assert not handler.can_handle_data([1, 2, 3])  # List of non-dicts
        assert not handler.can_handle_data({"key": "value"})  # Single dict
        assert not handler.can_handle_data("string")

    def test_write_read_cycle(self, handler, temp_file):
        """Test writing and reading JSONL data."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35}
        ]
        
        # Write data
        handler.write_to_file(test_data, temp_file, ".jsonl")
        assert temp_file.exists()
        
        # Read data back
        loaded_data = handler.read_from_file(temp_file, ".jsonl")
        assert loaded_data == test_data

    def test_write_invalid_data_type(self, handler, temp_file):
        """Test writing invalid data type raises error."""
        with pytest.raises(TypeError, match="Cannot save .* as JSONL"):
            handler.write_to_file("not a list", temp_file, ".jsonl")


class TestTextHandler:
    """Test the TextHandler class."""

    @pytest.fixture
    def handler(self):
        return TextHandler()

    @pytest.fixture
    def temp_file(self):
        temp_file = Path(tempfile.mktemp(suffix=".txt"))
        yield temp_file
        if temp_file.exists():
            temp_file.unlink()

    def test_can_handle_extension(self, handler):
        """Test extension detection."""
        assert handler.can_handle_extension(".txt")
        assert handler.can_handle_extension(".html")
        assert handler.can_handle_extension(".md")
        assert handler.can_handle_extension(".TXT")
        assert not handler.can_handle_extension(".json")

    def test_can_handle_data(self, handler):
        """Test data type detection."""
        assert handler.can_handle_data("text string")
        assert handler.can_handle_data("")
        assert not handler.can_handle_data(123)
        assert not handler.can_handle_data({"key": "value"})

    def test_write_read_cycle(self, handler, temp_file):
        """Test writing and reading text data."""
        test_data = "This is a test string\nwith multiple lines\nand unicode: 🚀"
        
        # Write data
        handler.write_to_file(test_data, temp_file, ".txt")
        assert temp_file.exists()
        
        # Read data back
        loaded_data = handler.read_from_file(temp_file, ".txt")
        assert loaded_data == test_data

    def test_write_invalid_data_type(self, handler, temp_file):
        """Test writing invalid data type raises error."""
        with pytest.raises(TypeError, match="Cannot save .* as text"):
            handler.write_to_file(123, temp_file, ".txt")


class TestPickleHandler:
    """Test the PickleHandler class."""

    @pytest.fixture
    def handler(self):
        return PickleHandler()

    @pytest.fixture
    def temp_file(self):
        temp_file = Path(tempfile.mktemp(suffix=".pkl"))
        yield temp_file
        if temp_file.exists():
            temp_file.unlink()

    def test_can_handle_extension(self, handler):
        """Test extension detection."""
        assert handler.can_handle_extension(".pkl")
        assert handler.can_handle_extension(".pickle")
        assert handler.can_handle_extension(".PKL")
        assert not handler.can_handle_extension(".json")

    def test_can_handle_data(self, handler):
        """Test data type detection - pickle handles everything."""
        assert handler.can_handle_data("string")
        assert handler.can_handle_data(123)
        assert handler.can_handle_data({"key": "value"})
        assert handler.can_handle_data([1, 2, 3])
        assert handler.can_handle_data(object())

    def test_write_read_cycle_complex_object(self, handler, temp_file):
        """Test writing and reading complex Python objects."""
        # Use serializable objects instead of local classes
        test_data = {
            "string": "value",
            "number": 42,
            "custom": {"type": "custom", "value": "test"},
            "nested": {"list": [1, 2, {"type": "custom", "value": "nested"}]}
        }
        
        # Write data
        handler.write_to_file(test_data, temp_file, ".pkl")
        assert temp_file.exists()
        
        # Read data back
        loaded_data = handler.read_from_file(temp_file, ".pkl")
        assert loaded_data["string"] == test_data["string"]
        assert loaded_data["number"] == test_data["number"]
        assert loaded_data["custom"] == test_data["custom"]

    def test_priority_is_lowest(self, handler):
        """Test that pickle has lowest priority (fallback)."""
        assert handler.priority == 1


class TestPydanticHandler:
    """Test the PydanticHandler class."""

    @pytest.fixture
    def handler(self):
        return PydanticHandler()

    def test_can_handle_extension(self, handler):
        """Test extension detection."""
        assert handler.can_handle_extension(".pydantic.json")
        assert not handler.can_handle_extension(".json")

    def test_can_handle_data(self, handler):
        """Test data type detection for Pydantic models."""
        # Mock Pydantic model
        mock_model = MagicMock()
        mock_model.model_dump = MagicMock()
        mock_model.model_validate = MagicMock()
        mock_model.__class__.__pydantic_core_schema__ = "schema"
        
        assert handler.can_handle_data(mock_model)
        assert not handler.can_handle_data({"key": "value"})
        assert not handler.can_handle_data("string")


class TestDataclassHandler:
    """Test the DataclassHandler class."""

    @pytest.fixture
    def handler(self):
        return DataclassHandler()

    @pytest.fixture
    def temp_file(self):
        temp_file = Path(tempfile.mktemp(suffix=".dataclass.json"))
        yield temp_file
        if temp_file.exists():
            temp_file.unlink()

    def test_can_handle_extension(self, handler):
        """Test extension detection."""
        assert handler.can_handle_extension(".dataclass.json")
        assert not handler.can_handle_extension(".json")

    def test_can_handle_data(self, handler):
        """Test data type detection for dataclasses."""
        from dataclasses import dataclass
        
        @dataclass
        class TestDataclass:
            name: str
            value: int
        
        test_obj = TestDataclass("test", 42)
        
        assert handler.can_handle_data(test_obj)
        assert not handler.can_handle_data({"key": "value"})

    def test_write_read_cycle(self, handler, temp_file):
        """Test writing and reading dataclass objects."""
        from dataclasses import dataclass
        
        @dataclass
        class TestDataclass:
            name: str
            value: int
            
        test_obj = TestDataclass("test", 42)
        
        # Write data
        handler.write_to_file(test_obj, temp_file, ".dataclass.json")
        assert temp_file.exists()
        
        # Read data back (returns dict, not original dataclass)
        loaded_data = handler.read_from_file(temp_file, ".dataclass.json")
        assert loaded_data == {"name": "test", "value": 42}


class TestHandlersWithOptionalDependencies:
    """Test handlers that require optional dependencies."""

    def test_parquet_handler_without_polars(self):
        """Test ParquetHandler when polars is not available."""
        with patch.dict('sys.modules', {'polars': None}):
            handler = ParquetHandler()
            
            # Should handle gracefully when polars not available
            assert not handler.can_handle_data({"fake": "data"})
            
            with pytest.raises(ImportError, match="Polars is required"):
                handler.read_from_file(Path("fake.parquet"), ".parquet")

    def test_csv_handler_without_polars(self):
        """Test CSVHandler when polars is not available."""
        with patch.dict('sys.modules', {'polars': None}):
            handler = CSVHandler()
            
            assert not handler.can_handle_data({"fake": "data"})
            
            with pytest.raises(ImportError, match="Polars is required"):
                handler.read_from_file(Path("fake.csv"), ".csv")

    def test_torch_handler_without_pytorch(self):
        """Test TorchHandler when PyTorch is not available."""
        with patch.dict('sys.modules', {'torch': None}):
            handler = TorchHandler()
            
            assert not handler.can_handle_data({"fake": "data"})
            
            with pytest.raises(ImportError, match="PyTorch is required"):
                handler.read_from_file(Path("fake.pt"), ".pt")

    def test_yaml_handler_without_pyyaml(self):
        """Test YAMLHandler when PyYAML is not available."""
        with patch.dict('sys.modules', {'yaml': None}):
            handler = YAMLHandler()
            
            with pytest.raises(ImportError, match="PyYAML is required"):
                handler.read_from_file(Path("fake.yaml"), ".yaml")

    def test_image_handler_without_pillow(self):
        """Test ImageHandler when Pillow is not available."""
        with patch.dict('sys.modules', {'PIL': None}):
            handler = ImageHandler()
            
            with pytest.raises(ImportError, match="Pillow is required"):
                handler.read_from_file(Path("fake.png"), ".png")

    def test_numpy_handler_without_numpy(self):
        """Test NumpyHandler when NumPy is not available."""
        with patch.dict('sys.modules', {'numpy': None}):
            handler = NumpyHandler()
            
            assert not handler.can_handle_data({"fake": "data"})
            
            with pytest.raises(ImportError, match="NumPy is required"):
                handler.read_from_file(Path("fake.npy"), ".npy")

    def test_sparse_handler_without_scipy(self):
        """Test SparseHandler when SciPy is not available."""
        with patch.dict('sys.modules', {'scipy': None}):
            handler = SparseHandler()
            
            assert not handler.can_handle_data({"fake": "data"})
            
            with pytest.raises(ImportError, match="SciPy is required"):
                handler.read_from_file(Path("fake.sparse.npz"), ".sparse.npz")


class TestHandlerRegistry:
    """Test the HandlerRegistry class."""

    @pytest.fixture
    def registry(self):
        return HandlerRegistry()

    def test_default_handlers_registered(self, registry):
        """Test that default handlers are registered."""
        extensions = registry.get_supported_extensions()
        
        # Check for common extensions
        expected_extensions = [".json", ".txt", ".pkl", ".jsonl"]
        for ext in expected_extensions:
            assert ext in extensions

    def test_get_handler_for_extension(self, registry):
        """Test getting handler by extension."""
        json_handler = registry.get_handler_for_extension(".json")
        assert isinstance(json_handler, JSONHandler)
        
        txt_handler = registry.get_handler_for_extension(".txt")
        assert isinstance(txt_handler, TextHandler)
        
        # Test case insensitive
        json_handler_upper = registry.get_handler_for_extension(".JSON")
        assert isinstance(json_handler_upper, JSONHandler)

    def test_get_handler_for_data(self, registry):
        """Test getting handler by data type."""
        # JSON data should get JSON handler (higher priority than text)
        json_handler = registry.get_handler_for_data({"key": "value"})
        assert isinstance(json_handler, JSONHandler)
        
        # String should get JSON handler (higher priority than text)
        string_handler = registry.get_handler_for_data("test string")
        assert isinstance(string_handler, JSONHandler)
        
        # Complex object should get pickle handler
        pickle_handler = registry.get_handler_for_data(object())
        assert isinstance(pickle_handler, PickleHandler)

    def test_register_custom_handler(self, registry):
        """Test registering custom handler."""
        class CustomHandler:
            def can_handle_extension(self, ext):
                return ext == ".custom"
            
            def can_handle_data(self, data):
                return isinstance(data, str) and data.startswith("CUSTOM:")
            
            def read_from_file(self, file_path, ext):
                return "custom_read"
            
            def write_to_file(self, data, file_path, ext):
                pass
            
            @property
            def extensions(self):
                return [".custom"]
            
            @property
            def priority(self):
                return 15  # Higher than JSON
        
        custom_handler = CustomHandler()
        registry.register(custom_handler)
        
        # Test it's registered
        handler = registry.get_handler_for_extension(".custom")
        assert handler is custom_handler
        
        # Test priority works
        high_priority_handler = registry.get_handler_for_data("CUSTOM:test")
        assert high_priority_handler is custom_handler

    def test_unregister_handler(self, registry):
        """Test unregistering handler."""
        # Verify JSON handler exists initially
        assert registry.get_handler_for_extension(".json") is not None
        
        # Unregister JSON handler
        registry.unregister(JSONHandler)
        
        # Verify it's gone
        assert registry.get_handler_for_extension(".json") is None

    def test_handler_priority_ordering(self, registry):
        """Test that handlers are ordered by priority."""
        # String data could be handled by both JSON and Text handlers
        # JSON has higher priority (8) than Text (7)
        handler = registry.get_handler_for_data("test string")
        assert isinstance(handler, JSONHandler)

    def test_get_supported_extensions(self, registry):
        """Test getting all supported extensions."""
        extensions = registry.get_supported_extensions()
        
        assert isinstance(extensions, list)
        assert len(extensions) > 0
        assert ".json" in extensions
        assert ".txt" in extensions
        assert ".pkl" in extensions


class TestHandlerErrorCases:
    """Test error cases for handlers."""

    def test_numpy_handler_invalid_extension_for_data(self):
        """Test NumpyHandler with invalid extension for data type."""
        handler = NumpyHandler()
        temp_file = Path(tempfile.mktemp(suffix=".npy"))
        
        try:
            # Mock numpy being available
            with patch('autostore.autostore.NumpyHandler.can_handle_data', return_value=True):
                with patch('numpy.save') as mock_save:
                    with patch('numpy.savez') as mock_savez:
                        # Test with wrong data type for .npy
                        with pytest.raises(TypeError, match="Cannot save .* as .npy"):
                            handler.write_to_file("not an array", temp_file, ".npy")
                        
                        # Test with wrong data type for .npz
                        temp_file_npz = Path(tempfile.mktemp(suffix=".npz"))
                        try:
                            with pytest.raises(TypeError, match="Cannot save .* as .npz"):
                                handler.write_to_file("not an array", temp_file_npz, ".npz")
                        finally:
                            if temp_file_npz.exists():
                                temp_file_npz.unlink()
        finally:
            if temp_file.exists():
                temp_file.unlink()

    def test_sparse_handler_invalid_data_type(self):
        """Test SparseHandler with invalid data type."""
        handler = SparseHandler()
        temp_file = Path(tempfile.mktemp(suffix=".sparse.npz"))
        
        try:
            with patch('scipy.sparse.issparse', return_value=False):
                with pytest.raises(TypeError, match="Cannot save .* as .sparse"):
                    handler.write_to_file("not sparse", temp_file, ".sparse.npz")
        finally:
            if temp_file.exists():
                temp_file.unlink()


class TestHandlerIntegration:
    """Integration tests for handlers."""

    def test_multiple_handlers_same_data(self):
        """Test that multiple handlers can handle the same data with priority."""
        registry = HandlerRegistry()
        
        # String data can be handled by both JSON and Text handlers
        string_data = "test string"
        
        json_handler = registry.get_handler_for_extension(".json")
        text_handler = registry.get_handler_for_extension(".txt")
        
        assert json_handler.can_handle_data(string_data)
        assert text_handler.can_handle_data(string_data)
        
        # But registry should return higher priority handler
        chosen_handler = registry.get_handler_for_data(string_data)
        assert isinstance(chosen_handler, JSONHandler)  # Higher priority

    def test_handler_directory_creation(self):
        """Test that handlers create parent directories."""
        handler = JSONHandler()
        temp_dir = Path(tempfile.mkdtemp())
        nested_file = temp_dir / "sub" / "dir" / "test.json"
        
        try:
            # Parent directories don't exist
            assert not nested_file.parent.exists()
            
            # Write should create them
            handler.write_to_file({"test": "data"}, nested_file, ".json")
            
            assert nested_file.exists()
            assert nested_file.parent.exists()
            
        finally:
            import shutil
            shutil.rmtree(temp_dir)