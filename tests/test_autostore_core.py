"""Tests for AutoStore core functionality."""

import pytest
import tempfile
import shutil
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from autostore.autostore import (
    AutoStore, StorageConfig, LocalFileConfig,
    StorageError, StorageFileNotFoundError,
    JSONHandler, TextHandler, PickleHandler
)


class TestAutoStoreInitialization:
    """Test AutoStore initialization and configuration."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_init_with_path_string(self, temp_dir):
        """Test initialization with path string."""
        store = AutoStore(str(temp_dir))
        
        assert store.storage_uri == str(temp_dir)
        # Use samefile to handle symlinks and /private differences on macOS
        assert store.backend.root_path.samefile(temp_dir)
        assert isinstance(store.backend.config, LocalFileConfig)

    def test_init_with_path_object(self, temp_dir):
        """Test initialization with Path object."""
        store = AutoStore(temp_dir)
        
        assert store.storage_uri == str(temp_dir)
        assert store.backend.root_path.samefile(temp_dir)

    def test_init_with_file_uri(self, temp_dir):
        """Test initialization with file:// URI."""
        uri = f"file://{temp_dir}"
        store = AutoStore(uri)
        
        assert store.storage_uri == uri
        assert store.backend.root_path.samefile(temp_dir)

    def test_init_with_custom_config(self, temp_dir):
        """Test initialization with custom configuration."""
        config = LocalFileConfig(cache_enabled=True, cache_expiry_hours=48)
        store = AutoStore(temp_dir, config)
        
        assert store.backend.config is config
        assert store.backend.config.cache_enabled
        assert store.backend.config.cache_expiry_hours == 48

    def test_init_unsupported_scheme(self):
        """Test initialization with unsupported scheme raises error."""
        with pytest.raises(ValueError, match="Unsupported storage scheme"):
            AutoStore("ftp://example.com/path")

    def test_init_backend_failure(self):
        """Test initialization handles backend creation failure."""
        with patch('autostore.autostore.LocalFileBackend.__init__', side_effect=Exception("Backend error")):
            with pytest.raises(StorageError, match="Failed to initialize"):
                AutoStore("/tmp/test")

    def test_repr(self, temp_dir):
        """Test string representation."""
        store = AutoStore(temp_dir)
        repr_str = repr(store)
        
        assert "AutoStore" in repr_str
        assert str(temp_dir) in repr_str
        assert "LocalFileBackend" in repr_str


class TestAutoStoreBasicOperations:
    """Test basic AutoStore operations."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_setitem_getitem_json(self, temp_store):
        """Test storing and retrieving JSON data."""
        test_data = {"key": "value", "number": 42, "list": [1, 2, 3]}
        
        temp_store["test.json"] = test_data
        loaded_data = temp_store["test.json"]
        
        assert loaded_data == test_data

    def test_setitem_getitem_text(self, temp_store):
        """Test storing and retrieving text data."""
        test_text = "This is test text\nwith multiple lines"
        
        temp_store["test.txt"] = test_text
        loaded_text = temp_store["test.txt"]
        
        assert loaded_text == test_text

    def test_setitem_without_extension_infers_type(self, temp_store):
        """Test that file extension is inferred from data type."""
        test_data = {"inferred": True}
        
        temp_store["no_extension"] = test_data
        
        # Should be saved as JSON and retrievable
        loaded_data = temp_store["no_extension"]
        assert loaded_data == test_data
        
        # Should find the .json file
        files = list(temp_store.list_files())
        json_files = [f for f in files if f.endswith(".json")]
        assert len(json_files) == 1

    def test_setitem_fallback_to_pickle(self, temp_store):
        """Test fallback to pickle for unknown extensions."""
        # Use a simple serializable object instead of local class
        test_obj = {"custom_type": "test", "value": 42, "data": [1, 2, 3]}
        
        # Force unknown extension - should fall back to pickle
        temp_store["test.unknown"] = test_obj
        loaded_obj = temp_store["test.unknown"]
        
        assert loaded_obj == test_obj

    def test_contains(self, temp_store):
        """Test __contains__ method."""
        test_data = {"test": True}
        
        assert "test_file" not in temp_store
        
        temp_store["test_file.json"] = test_data
        
        assert "test_file" in temp_store
        assert "test_file.json" in temp_store
        assert "nonexistent" not in temp_store

    def test_delitem(self, temp_store):
        """Test deleting items."""
        test_data = {"delete": "me"}
        
        temp_store["to_delete.json"] = test_data
        assert "to_delete" in temp_store
        
        del temp_store["to_delete"]
        assert "to_delete" not in temp_store

    def test_delitem_nonexistent(self, temp_store):
        """Test deleting nonexistent item raises error."""
        with pytest.raises(StorageFileNotFoundError, match="No file found"):
            del temp_store["nonexistent"]

    def test_exists(self, temp_store):
        """Test exists method."""
        assert not temp_store.exists("test_file")
        
        temp_store["test_file.json"] = {"exists": True}
        
        assert temp_store.exists("test_file")
        assert temp_store.exists("test_file.json")

    def test_get_size(self, temp_store):
        """Test getting file size."""
        test_data = {"size": "test"}
        temp_store["size_test.json"] = test_data
        
        size = temp_store.get_size("size_test")
        assert size > 0
        assert isinstance(size, int)

    def test_len(self, temp_store):
        """Test __len__ method."""
        assert len(temp_store) == 0
        
        temp_store["file1.json"] = {"data": 1}
        temp_store["file2.txt"] = "text"
        
        assert len(temp_store) == 2


class TestAutoStoreFileOperations:
    """Test file operations in AutoStore."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_copy(self, temp_store):
        """Test copying files."""
        original_data = {"original": "data"}
        temp_store["original.json"] = original_data
        
        temp_store.copy("original", "copy")
        
        # Both files should exist
        assert temp_store.exists("original")
        assert temp_store.exists("copy")
        
        # Copy should have same content
        copied_data = temp_store["copy"]
        assert copied_data == original_data

    def test_copy_with_extension_inference(self, temp_store):
        """Test copy with extension inference for destination."""
        temp_store["source.json"] = {"data": "test"}
        
        # Destination without extension should infer from source
        temp_store.copy("source", "dest")
        
        assert temp_store.exists("dest")
        # Should find dest.json file
        files = list(temp_store.list_files())
        dest_files = [f for f in files if "dest" in f]
        assert any(f.endswith(".json") for f in dest_files)

    def test_move(self, temp_store):
        """Test moving files."""
        original_data = {"move": "me"}
        temp_store["original.json"] = original_data
        
        temp_store.move("original", "moved")
        
        # Original should be gone, moved should exist
        assert not temp_store.exists("original")
        assert temp_store.exists("moved")
        
        # Moved file should have same content
        moved_data = temp_store["moved"]
        assert moved_data == original_data

    def test_list_files(self, temp_store):
        """Test listing files."""
        # Create test files
        temp_store["file1.json"] = {"data": 1}
        temp_store["file2.txt"] = "text"
        temp_store["nested/file3.json"] = {"nested": True}
        
        # List all files
        all_files = list(temp_store.list_files())
        assert "file1.json" in all_files
        assert "file2.txt" in all_files
        assert "nested/file3.json" in all_files

    def test_list_files_with_pattern(self, temp_store):
        """Test listing files with pattern."""
        temp_store["test1.json"] = {"data": 1}
        temp_store["test2.json"] = {"data": 2}
        temp_store["other.txt"] = "text"
        
        # List only JSON files
        json_files = list(temp_store.list_files("*.json"))
        assert "test1.json" in json_files
        assert "test2.json" in json_files
        assert "other.txt" not in json_files

    def test_get_metadata(self, temp_store):
        """Test getting file metadata."""
        test_data = {"metadata": "test"}
        temp_store["metadata_test.json"] = test_data
        
        metadata = temp_store.get_metadata("metadata_test")
        
        assert metadata.size > 0
        assert metadata.content_type == "application/json"
        assert metadata.modified_time is not None


class TestAutoStoreKeys:
    """Test key-related operations in AutoStore."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_keys_empty_store(self, temp_store):
        """Test keys on empty store."""
        keys = list(temp_store.keys())
        assert keys == []

    def test_keys_with_files(self, temp_store):
        """Test keys with various files."""
        temp_store["file1.json"] = {"data": 1}
        temp_store["file2.txt"] = "text"
        temp_store["nested/file3.json"] = {"nested": True}
        
        keys = list(temp_store.keys())
        
        # Should get filenames without extensions
        assert "file1" in keys
        assert "file2" in keys
        assert "nested/file3" in keys
        assert len(keys) == 3

    def test_keys_deduplication(self, temp_store):
        """Test that keys are deduplicated."""
        # This test depends on handler priority - JSON handler might be used for both
        temp_store["same_name.json"] = {"format": "json"}
        
        keys = list(temp_store.keys())
        same_name_keys = [k for k in keys if k == "same_name"]
        assert len(same_name_keys) == 1

    def test_find_file_key(self, temp_store):
        """Test _find_file_key method."""
        temp_store["test.json"] = {"data": "test"}
        
        # Should find exact match
        result = temp_store._find_file_key("test.json")
        assert result == "test.json"
        
        # Should find with extension inference
        result = temp_store._find_file_key("test")
        assert result == "test.json"

    def test_find_file_key_nonexistent(self, temp_store):
        """Test _find_file_key with nonexistent file."""
        with pytest.raises(StorageFileNotFoundError, match="No file found"):
            temp_store._find_file_key("nonexistent")

    def test_find_file_fuzzy(self, temp_store):
        """Test fuzzy file finding."""
        temp_store["Test_File.json"] = {"data": "test"}
        
        # Case insensitive match
        result = temp_store.find_file_fuzzy("test_file")
        assert result == "Test_File.json"
        
        # Stem match
        result = temp_store.find_file_fuzzy("Test_File.txt")
        assert result == "Test_File.json"
        
        # No match
        result = temp_store.find_file_fuzzy("completely_different")
        assert result is None


class TestAutoStoreHandlerManagement:
    """Test data handler management in AutoStore."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_register_custom_handler(self, temp_store):
        """Test registering custom data handler."""
        class CustomHandler:
            def can_handle_extension(self, ext):
                return ext == ".custom"
            
            def can_handle_data(self, data):
                return isinstance(data, str) and data.startswith("CUSTOM:")
            
            def read_from_file(self, file_path, ext):
                return f"CUSTOM:{file_path.read_text()}"
            
            def write_to_file(self, data, file_path, ext):
                content = data.replace("CUSTOM:", "")
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text(content)
            
            @property
            def extensions(self):
                return [".custom"]
            
            @property
            def priority(self):
                return 20  # High priority
        
        custom_handler = CustomHandler()
        temp_store.register_handler(custom_handler)
        
        # Test custom handler works
        temp_store["test.custom"] = "CUSTOM:test data"
        loaded = temp_store["test.custom"]
        assert loaded.startswith("CUSTOM:")

    def test_unregister_handler(self, temp_store):
        """Test unregistering data handler."""
        # JSON should work initially
        temp_store["test.json"] = {"data": "test"}
        assert temp_store["test.json"] == {"data": "test"}
        
        # Unregister JSON handler
        temp_store.unregister_handler(JSONHandler)
        
        # Should fall back to pickle for unknown extension
        with patch('autostore.autostore.log.warning') as mock_warning:
            temp_store["test2.json"] = {"data": "test2"}
            mock_warning.assert_called()  # Should warn about fallback

    def test_infer_extension(self, temp_store):
        """Test extension inference."""
        # Test with dict (should get .json)
        ext = temp_store._infer_extension({"key": "value"})
        assert ext == ".json"
        
        # Test with string (should get .json due to priority)
        ext = temp_store._infer_extension("test string")
        assert ext == ".json"
        
        # Test with unknown type (should get .pkl)
        ext = temp_store._infer_extension(object())
        assert ext == ".pkl"


class TestAutoStoreErrorHandling:
    """Test error handling in AutoStore."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_getitem_nonexistent_file(self, temp_store):
        """Test getting nonexistent file raises appropriate error."""
        with pytest.raises(StorageFileNotFoundError):
            _ = temp_store["nonexistent"]

    def test_getitem_unsupported_extension(self, temp_store):
        """Test getting file with unsupported extension."""
        # Create file directly in backend
        backend_path = temp_store.backend.root_path
        (backend_path / "test.xyz").write_text("unsupported format")
        
        with pytest.raises(ValueError, match="Unsupported file type"):
            _ = temp_store["test.xyz"]

    def test_setitem_handler_error(self, temp_store):
        """Test setitem when handler raises error."""
        # Mock handler to raise error
        with patch.object(temp_store.handler_registry, 'get_handler_for_extension') as mock_get:
            mock_handler = MagicMock()
            mock_handler.write_to_file.side_effect = Exception("Handler error")
            mock_get.return_value = mock_handler
            
            with pytest.raises(StorageError, match="Failed to save data"):
                temp_store["error.json"] = {"data": "test"}

    def test_copy_nonexistent_source(self, temp_store):
        """Test copying nonexistent source file."""
        with pytest.raises(StorageFileNotFoundError, match="No file found"):
            temp_store.copy("nonexistent", "destination")

    def test_move_nonexistent_source(self, temp_store):
        """Test moving nonexistent source file."""
        with pytest.raises(StorageFileNotFoundError, match="No file found"):
            temp_store.move("nonexistent", "destination")


class TestAutoStoreContextManager:
    """Test AutoStore as context manager."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_context_manager(self, temp_dir):
        """Test AutoStore as context manager."""
        with AutoStore(temp_dir) as store:
            assert isinstance(store, AutoStore)
            store["test.json"] = {"context": "manager"}
            assert store["test.json"] == {"context": "manager"}
        
        # Should exit cleanly without errors

    def test_context_manager_cleanup_called(self, temp_dir):
        """Test that cleanup is called on context exit."""
        with patch('autostore.autostore.AutoStore.cleanup') as mock_cleanup:
            with AutoStore(temp_dir) as store:
                pass
            mock_cleanup.assert_called_once()


class TestAutoStoreClassMethods:
    """Test AutoStore class methods."""

    def test_register_backend(self):
        """Test registering custom backend."""
        from autostore.autostore import StorageBackend
        
        class CustomBackend(StorageBackend):
            def exists(self, path): return True
            def download(self, remote_path, local_path): pass
            def upload(self, local_path, remote_path): pass
            def delete(self, path): pass
            def list_files(self, pattern="*", recursive=True): return iter([])
            def get_metadata(self, path): return None
        
        AutoStore.register_backend("custom", CustomBackend)
        
        # Should be able to create store with custom scheme
        supported = AutoStore.get_supported_backends()
        assert "custom" in supported

    def test_unregister_backend(self):
        """Test unregistering backend."""
        # Register a backend first
        from autostore.autostore import LocalFileBackend
        AutoStore.register_backend("temp", LocalFileBackend)
        
        assert "temp" in AutoStore.get_supported_backends()
        
        # Unregister it
        AutoStore.unregister_backend("temp")
        
        assert "temp" not in AutoStore.get_supported_backends()

    def test_get_supported_backends(self):
        """Test getting supported backends."""
        backends = AutoStore.get_supported_backends()
        
        assert isinstance(backends, list)
        assert "file" in backends
        assert "" in backends  # Empty scheme for local files


class TestAutoStoreCacheIntegration:
    """Test AutoStore with caching functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_cache_cleanup(self, temp_dir):
        """Test cache cleanup functionality."""
        config = LocalFileConfig(cache_enabled=True)
        store = AutoStore(temp_dir, config)
        
        # Create some cached content (though LocalFileBackend doesn't actually cache)
        store["test.json"] = {"cached": True}
        
        # Should not raise error
        store.cleanup_cache()

    def test_store_cleanup(self, temp_dir):
        """Test store cleanup."""
        store = AutoStore(temp_dir)
        
        # Should not raise error
        store.cleanup()


class TestAutoStoreIntegration:
    """Integration tests for AutoStore functionality."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_complex_data_workflow(self, temp_store):
        """Test complex workflow with various data types."""
        # Store different types of data
        temp_store["config.json"] = {
            "app_name": "test_app",
            "version": "1.0.0",
            "features": ["auth", "api", "web"]
        }
        
        temp_store["readme.txt"] = """
        # Test Application
        
        This is a test application for AutoStore.
        
        ## Features
        - Authentication
        - REST API
        - Web Interface
        """
        
        temp_store["users/user1.json"] = {
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "active": True
        }
        
        temp_store["users/user2.json"] = {
            "id": 2,
            "name": "Bob", 
            "email": "bob@example.com",
            "active": False
        }
        
        # Test retrieval
        config = temp_store["config"]
        assert config["app_name"] == "test_app"
        assert len(config["features"]) == 3
        
        readme = temp_store["readme"]
        assert "Test Application" in readme
        
        user1 = temp_store["users/user1"]
        assert user1["name"] == "Alice"
        assert user1["active"] is True
        
        # Test listing
        all_files = list(temp_store.list_files())
        user_files = [f for f in all_files if f.startswith("users/")]
        assert len(user_files) == 2
        
        # Test keys
        keys = list(temp_store.keys())
        assert "config" in keys
        assert "readme" in keys
        assert "users/user1" in keys
        assert "users/user2" in keys
        
        # Test file operations
        temp_store.copy("users/user1", "users/user1_backup")
        backup = temp_store["users/user1_backup"]
        assert backup == user1
        
        # Test existence
        assert temp_store.exists("config")
        assert temp_store.exists("users/user1")
        assert not temp_store.exists("nonexistent")
        
        # Test metadata
        config_meta = temp_store.get_metadata("config")
        assert config_meta.content_type == "application/json"
        assert config_meta.size > 0

    def test_nested_directory_structure(self, temp_store):
        """Test working with deeply nested directory structures."""
        # Create nested structure
        temp_store["projects/ml/experiments/exp1/config.json"] = {
            "experiment": "exp1",
            "model": "transformer",
            "params": {"lr": 0.001, "batch_size": 32}
        }
        
        temp_store["projects/ml/experiments/exp1/results.json"] = {
            "accuracy": 0.95,
            "loss": 0.05,
            "metrics": {"precision": 0.94, "recall": 0.96}
        }
        
        temp_store["projects/ml/experiments/exp2/config.json"] = {
            "experiment": "exp2", 
            "model": "cnn",
            "params": {"lr": 0.01, "batch_size": 64}
        }
        
        # Test retrieval from nested paths
        exp1_config = temp_store["projects/ml/experiments/exp1/config"]
        assert exp1_config["experiment"] == "exp1"
        
        exp1_results = temp_store["projects/ml/experiments/exp1/results"]
        assert exp1_results["accuracy"] == 0.95
        
        # Test listing with patterns
        exp_files = list(temp_store.list_files("projects/ml/experiments/**/*.json"))
        assert len(exp_files) == 3
        
        config_files = list(temp_store.list_files("**/config.json"))
        assert len(config_files) == 2
        
        # Test directory operations
        temp_store.copy("projects/ml/experiments/exp1/config", 
                       "projects/ml/experiments/exp1/config_backup")
        
        backup = temp_store["projects/ml/experiments/exp1/config_backup"]
        assert backup == exp1_config

    def test_error_recovery_and_robustness(self, temp_store):
        """Test error recovery and robustness."""
        # Store valid data
        temp_store["valid.json"] = {"status": "ok"}
        
        # Try to store invalid data for handler
        try:
            # This should work but use fallback handler
            temp_store["unknown.xyz"] = {"data": "fallback"}
        except Exception:
            pass  # Some handlers might not support unknown extensions
        
        # Original data should still be accessible
        assert temp_store["valid"] == {"status": "ok"}
        
        # Test with missing files
        assert not temp_store.exists("missing")
        
        try:
            _ = temp_store["missing"]
            assert False, "Should have raised exception"
        except StorageFileNotFoundError:
            pass  # Expected
        
        # Store should still work after errors
        temp_store["recovery.json"] = {"recovered": True}
        assert temp_store["recovery"]["recovered"] is True