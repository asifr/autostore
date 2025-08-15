"""Tests for the path-like syntax functionality in AutoStore."""

import pytest
import shutil
import tempfile
from pathlib import Path

from autostore import AutoStore
from autostore.autostore import StorePath


class TestStorePath:
    """Test the StorePath class functionality."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_storepath_creation(self, temp_store):
        """Test StorePath object creation."""
        path = StorePath(temp_store, "data/models")
        assert path.store is temp_store
        assert path.path == "data/models"

    def test_storepath_normalization(self, temp_store):
        """Test path normalization in StorePath."""
        # Test backslash normalization
        path1 = StorePath(temp_store, "data\\models")
        assert path1.path == "data/models"

        # Test leading/trailing slash removal
        path2 = StorePath(temp_store, "/data/models/")
        assert path2.path == "data/models"

    def test_storepath_truediv(self, temp_store):
        """Test the / operator on StorePath."""
        base_path = StorePath(temp_store, "data")
        models_path = base_path / "models"

        assert isinstance(models_path, StorePath)
        assert models_path.path == "data/models"
        assert models_path.store is temp_store

    def test_storepath_chained_truediv(self, temp_store):
        """Test chaining multiple / operations."""
        path = StorePath(temp_store, "") / "projects" / "ml" / "experiments"
        assert path.path == "projects/ml/experiments"

    def test_storepath_empty_base(self, temp_store):
        """Test StorePath with empty base path."""
        path = StorePath(temp_store, "")
        new_path = path / "data"
        assert new_path.path == "data"

    def test_storepath_setitem_getitem(self, temp_store):
        """Test setting and getting items through StorePath."""
        path = StorePath(temp_store, "data")
        test_data = {"key": "value", "number": 42}

        # Set data
        path["test.json"] = test_data

        # Get data back
        loaded_data = path["test.json"]
        assert loaded_data == test_data

    def test_storepath_contains(self, temp_store):
        """Test the 'in' operator on StorePath."""
        path = StorePath(temp_store, "data")
        test_data = {"test": True}

        # Initially should not contain the key
        assert "config.json" not in path

        # After setting, should contain the key
        path["config.json"] = test_data
        assert "config.json" in path

    def test_storepath_delitem(self, temp_store):
        """Test deleting items through StorePath."""
        path = StorePath(temp_store, "data")
        test_data = {"to_delete": True}

        # Set and verify existence
        path["temp.json"] = test_data
        assert "temp.json" in path

        # Delete and verify removal
        del path["temp.json"]
        assert "temp.json" not in path

    def test_storepath_exists(self, temp_store):
        """Test the exists method on StorePath."""
        path = StorePath(temp_store, "data")

        # Test exists with key
        assert not path.exists("nonexistent.json")

        path["exists.json"] = {"data": "test"}
        assert path.exists("exists.json")

        # Test exists without key (path itself)
        assert path.exists()  # Empty path should always exist

    def test_storepath_list_files(self, temp_store):
        """Test listing files through StorePath."""
        path = StorePath(temp_store, "data")

        # Add some test files
        path["file1.json"] = {"data": 1}
        path["file2.json"] = {"data": 2}
        path["file3.txt"] = "text data"

        # List all files
        files = list(path.list_files())
        assert "file1.json" in files
        assert "file2.json" in files
        assert "file3.txt" in files

        # List with pattern
        json_files = list(path.list_files("*.json"))
        assert len(json_files) == 2
        assert all(f.endswith(".json") for f in json_files)

    def test_storepath_get_metadata(self, temp_store):
        """Test getting metadata through StorePath."""
        path = StorePath(temp_store, "data")
        test_data = {"metadata_test": True}

        path["meta_test.json"] = test_data
        metadata = path.get_metadata("meta_test.json")

        assert metadata.size > 0
        assert metadata.content_type == "application/json"

    def test_storepath_str_repr(self, temp_store):
        """Test string representation of StorePath."""
        path = StorePath(temp_store, "data/models")

        assert str(path) == "data/models"
        assert repr(path) == "StorePath('data/models')"
    
    def test_storepath_with_pathlib_path(self, temp_store):
        """Test StorePath accepts pathlib.Path objects."""
        # Test creating StorePath with pathlib.Path
        path_obj = Path("data") / "models" / "experiments"
        store_path = StorePath(temp_store, path_obj)
        
        assert store_path.path == "data/models/experiments"
        
        # Test chaining with pathlib.Path
        new_path = store_path / Path("results")
        assert new_path.path == "data/models/experiments/results"
        
        # Test mixed usage
        mixed_path = store_path / "files" / Path("output.json")
        assert mixed_path.path == "data/models/experiments/files/output.json"


class TestAutoStorePathSyntax:
    """Test AutoStore integration with path syntax."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_autostore_truediv(self, temp_store):
        """Test the / operator on AutoStore."""
        path = temp_store / "data"

        assert isinstance(path, StorePath)
        assert path.store is temp_store
        assert path.path == "data"

    def test_autostore_chained_paths(self, temp_store):
        """Test chaining paths from AutoStore."""
        path = temp_store / "projects" / "ml" / "models"

        assert isinstance(path, StorePath)
        assert path.path == "projects/ml/models"

    def test_mixed_syntax_compatibility(self, temp_store):
        """Test that traditional and path syntax work together."""
        test_data = {"compatibility": "test"}

        # Save using traditional syntax
        temp_store["traditional.json"] = test_data

        # Access using path syntax
        root_path = temp_store / ""
        loaded_data = root_path["traditional.json"]
        assert loaded_data == test_data

        # Save using path syntax
        path = temp_store / "data"
        path["new_style.json"] = test_data

        # Access using traditional syntax
        loaded_traditional = temp_store["data/new_style.json"]
        assert loaded_traditional == test_data

    def test_deep_nested_paths(self, temp_store):
        """Test deeply nested path operations."""
        deep_path = temp_store / "level1" / "level2" / "level3" / "level4"
        test_data = {"deep": "nested", "level": 4}

        # Save data at deep path
        deep_path["deep_file.json"] = test_data

        # Verify we can read it back
        loaded_data = deep_path["deep_file.json"]
        assert loaded_data == test_data

        # Verify traditional access works
        traditional_data = temp_store["level1/level2/level3/level4/deep_file.json"]
        assert traditional_data == test_data

    def test_path_operations_with_various_extensions(self, temp_store):
        """Test path operations with different file types."""
        data_path = temp_store / "mixed_data"

        # JSON data
        data_path["config.json"] = {"setting": "value"}

        # Text data
        data_path["readme.txt"] = "This is a readme file"

        # Verify all files exist
        assert data_path.exists("config.json")
        assert data_path.exists("readme.txt")

        # Verify correct data types
        config = data_path["config.json"]
        readme = data_path["readme.txt"]

        assert isinstance(config, dict)
        assert isinstance(readme, str)
        assert config["setting"] == "value"
        assert readme == "This is a readme file"


class TestPathSyntaxIntegration:
    """Integration tests for path syntax with various data types."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_path_with_automatic_extension_inference(self, temp_store):
        """Test that paths work with automatic extension inference."""
        models_path = temp_store / "models"

        # Save different data types without specifying extensions
        models_path["config"] = {"model_type": "transformer"}  # Should become .json
        models_path["readme"] = "Model documentation"  # String -> JSON (higher priority than text)

        # Verify files were created with correct extensions
        files = list(models_path.list_files())

        # Check if files exist with inferred extensions
        config_files = [f for f in files if "config" in f]
        readme_files = [f for f in files if "readme" in f]

        assert len(config_files) > 0, f"No config files found. Files: {files}"
        assert len(readme_files) > 0, f"No readme files found. Files: {files}"

        # Verify the actual extensions (both should be .json due to handler priorities)
        assert any(f.endswith(".json") for f in config_files), f"Config file should be .json: {config_files}"
        assert any(f.endswith(".json") for f in readme_files), (
            f"Readme string should be .json (JSON handler has higher priority): {readme_files}"
        )

        # Test explicit .txt extension to ensure TextHandler works
        models_path["explicit_readme.txt"] = "This will be saved as text"
        explicit_files = [f for f in models_path.list_files() if "explicit_readme" in f]
        assert any(f.endswith(".txt") for f in explicit_files), "Explicitly specified .txt extension should work"

    def test_path_operations_preserve_data_integrity(self, temp_store):
        """Test that complex operations preserve data integrity."""
        experiments_path = temp_store / "experiments"

        # Create complex nested data
        experiment_data = {
            "id": "exp_001",
            "parameters": {"learning_rate": 0.001, "batch_size": 32, "epochs": 100},
            "results": {"accuracy": 0.95, "loss": 0.05, "metrics": [0.1, 0.2, 0.3, 0.4, 0.5]},
        }

        # Save and load through path syntax
        experiments_path["exp_001.json"] = experiment_data
        loaded_data = experiments_path["exp_001.json"]

        # Verify data integrity
        assert loaded_data == experiment_data
        assert loaded_data["parameters"]["learning_rate"] == 0.001
        assert len(loaded_data["results"]["metrics"]) == 5

    def test_path_error_handling(self, temp_store):
        """Test error handling in path operations."""
        path = temp_store / "nonexistent"

        # Test accessing non-existent file
        with pytest.raises(Exception):  # Should raise StorageFileNotFoundError
            _ = path["missing.json"]

        # Test deleting non-existent file
        with pytest.raises(Exception):  # Should raise StorageFileNotFoundError
            del path["missing.json"]
