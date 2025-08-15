"""Tests for the bug fixes in AutoStore."""

import pytest
import tempfile
import shutil
import os
from pathlib import Path

from autostore import AutoStore
from autostore.autostore import StorageConfig, LocalFileConfig, CacheManager, load_dotenv


class TestKeysBugFix:
    """Test the fix for the keys() method bug."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_keys_returns_full_filenames_not_characters(self, temp_store):
        """Test that keys() returns full filenames, not individual characters."""
        # Save some test files
        temp_store["test_file1.json"] = {"data": "test1"}
        temp_store["test_file2.json"] = {"data": "test2"}
        temp_store["another_test.txt"] = "text content"
        temp_store["nested/path/file.json"] = {"nested": True}

        # Get all keys
        keys = list(temp_store.keys())
        
        # Verify we get full filenames without extensions, not individual characters
        assert "test_file1" in keys
        assert "test_file2" in keys  
        assert "another_test" in keys
        assert "nested/path/file" in keys
        
        # Verify we don't get individual characters
        assert "t" not in keys  # Would be first character of "test_file1"
        assert "e" not in keys  # Would be second character of "test_file1"
        
        # Verify reasonable number of keys (should be 4, not 50+ characters)
        assert len(keys) == 4

    def test_keys_handles_duplicate_stems(self, temp_store):
        """Test that keys() properly handles files with same stem but different extensions."""
        # Save files with same name but different extensions
        temp_store["data.json"] = {"format": "json"}
        temp_store["data.txt"] = "format: text"
        
        keys = list(temp_store.keys())
        
        # Should only get one "data" key (deduplication works)
        data_keys = [k for k in keys if k == "data"]
        assert len(data_keys) == 1
        assert "data" in keys

    def test_keys_empty_store(self, temp_store):
        """Test keys() on empty store."""
        keys = list(temp_store.keys())
        assert keys == []

    def test_keys_with_unsupported_extensions(self, temp_store):
        """Test that keys() only includes supported file types."""
        # Create files directly in the filesystem with unsupported extensions
        backend_path = temp_store.backend.root_path
        (backend_path / "supported.json").write_text('{"test": true}')
        (backend_path / "unsupported.xyz").write_text("some data")
        
        keys = list(temp_store.keys())
        
        # Should only include supported file
        assert "supported" in keys
        assert len(keys) == 1


class TestCacheManagerBugFix:
    """Test the fix for the cache manager type annotation bug."""

    def test_cache_manager_initialization_disabled(self):
        """Test cache manager is None when caching is disabled."""
        config = StorageConfig(cache_enabled=False)
        temp_dir = Path(tempfile.mkdtemp())
        try:
            store = AutoStore(temp_dir, config)
            
            # Cache manager should be None
            assert store.backend.cache_manager is None
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_cache_manager_initialization_enabled(self):
        """Test cache manager is properly initialized when caching is enabled."""
        config = StorageConfig(cache_enabled=True)
        temp_dir = Path(tempfile.mkdtemp())
        try:
            store = AutoStore(temp_dir, config)
            
            # Cache manager should be initialized
            assert store.backend.cache_manager is not None
            assert isinstance(store.backend.cache_manager, CacheManager)
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_cache_manager_with_custom_config(self):
        """Test cache manager with custom configuration."""
        cache_dir = Path(tempfile.mkdtemp())
        config = StorageConfig(
            cache_enabled=True,
            cache_dir=str(cache_dir),
            cache_expiry_hours=48
        )
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            store = AutoStore(temp_dir, config)
            
            # Verify cache manager is configured correctly
            assert store.backend.cache_manager is not None
            assert store.backend.cache_manager.expiry_hours == 48
            assert str(cache_dir) in str(store.backend.cache_manager.cache_dir)
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
            shutil.rmtree(cache_dir, ignore_errors=True)


class TestLoadDotenvBugFix:
    """Test the fix for the load_dotenv unicode decode vulnerability."""

    @pytest.fixture
    def temp_env_file(self):
        """Create a temporary .env file for testing."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False)
        yield temp_file.name
        os.unlink(temp_file.name)

    def test_load_dotenv_safe_escape_handling(self, temp_env_file):
        """Test that load_dotenv safely handles escape sequences."""
        # Write test .env content with various escape sequences
        env_content = '''
# Test various escape sequences
TEST_SIMPLE="simple value"
TEST_NEWLINE="line1\\nline2"
TEST_TAB="value1\\tvalue2"
TEST_QUOTE="He said \\"hello\\""
TEST_BACKSLASH="path\\\\to\\\\file"
TEST_MIXED="start\\ttab\\nend"
'''
        
        with open(temp_env_file, 'w') as f:
            f.write(env_content)
        
        # Clear existing env vars to ensure clean test
        test_vars = ['TEST_SIMPLE', 'TEST_NEWLINE', 'TEST_TAB', 'TEST_QUOTE', 'TEST_BACKSLASH', 'TEST_MIXED']
        for var in test_vars:
            os.environ.pop(var, None)
        
        # Load the .env file
        load_dotenv(temp_env_file)
        
        # Verify values are properly escaped
        assert os.environ['TEST_SIMPLE'] == 'simple value'
        assert os.environ['TEST_NEWLINE'] == 'line1\nline2'
        assert os.environ['TEST_TAB'] == 'value1\tvalue2'
        assert os.environ['TEST_QUOTE'] == 'He said "hello"'
        assert os.environ['TEST_BACKSLASH'] == 'path\\to\\file'
        assert os.environ['TEST_MIXED'] == 'start\ttab\nend'
        
        # Clean up
        for var in test_vars:
            os.environ.pop(var, None)

    def test_load_dotenv_single_quotes(self, temp_env_file):
        """Test load_dotenv with single-quoted values."""
        env_content = '''
SINGLE_QUOTE='single value'
SINGLE_WITH_ESCAPE='don\\'t escape much'
'''
        
        with open(temp_env_file, 'w') as f:
            f.write(env_content)
        
        # Clear and load
        os.environ.pop('SINGLE_QUOTE', None)
        os.environ.pop('SINGLE_WITH_ESCAPE', None)
        
        load_dotenv(temp_env_file)
        
        assert os.environ['SINGLE_QUOTE'] == 'single value'
        assert os.environ['SINGLE_WITH_ESCAPE'] == "don't escape much"
        
        # Clean up
        os.environ.pop('SINGLE_QUOTE', None)
        os.environ.pop('SINGLE_WITH_ESCAPE', None)

    def test_load_dotenv_unquoted_values(self, temp_env_file):
        """Test load_dotenv with unquoted values."""
        env_content = '''
UNQUOTED_VALUE=simple_value
UNQUOTED_WITH_ESCAPES=value\\nwith\\tescapes
'''
        
        with open(temp_env_file, 'w') as f:
            f.write(env_content)
        
        # Clear and load
        os.environ.pop('UNQUOTED_VALUE', None)
        os.environ.pop('UNQUOTED_WITH_ESCAPES', None)
        
        load_dotenv(temp_env_file)
        
        assert os.environ['UNQUOTED_VALUE'] == 'simple_value'
        assert os.environ['UNQUOTED_WITH_ESCAPES'] == 'value\nwith\tescapes'
        
        # Clean up
        os.environ.pop('UNQUOTED_VALUE', None)
        os.environ.pop('UNQUOTED_WITH_ESCAPES', None)


class TestHashObjLoggingFix:
    """Test the fix for the logging call in hash_obj function."""

    def test_hash_obj_uses_module_logger(self, caplog):
        """Test that hash_obj uses the module logger instead of root logger."""
        from autostore.autostore import hash_obj
        
        # Test with an object that can't be serialized as string
        class NonSerializableObject:
            pass
        
        obj = NonSerializableObject()
        
        # Clear any existing log records
        caplog.clear()
        
        # This should trigger the warning log
        result = hash_obj(obj)
        
        # Verify a hash was generated
        assert isinstance(result, str)
        assert len(result) == 32  # MD5 hash length
        
        # Verify the warning was logged with the correct logger
        assert len(caplog.records) == 1
        log_record = caplog.records[0]
        assert log_record.levelname == 'WARNING'
        assert 'cannot be serialized' in log_record.message
        # Verify it's using the module logger (not root logger)
        assert log_record.name == 'autostore.autostore'


class TestIntegrationAfterFixes:
    """Integration tests to verify all fixes work together."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary AutoStore for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        store = AutoStore(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_complete_workflow_after_fixes(self, temp_store):
        """Test complete workflow works correctly after all bug fixes."""
        # Test data storage and retrieval
        test_data = {
            "config": {"setting1": "value1", "setting2": 42},
            "results": [1, 2, 3, 4, 5],
            "metadata": {"created": "2024-01-01", "version": "1.0"}
        }
        
        # Store multiple files
        temp_store["experiment1/config.json"] = test_data["config"]
        temp_store["experiment1/results.json"] = test_data["results"]
        temp_store["experiment1/metadata.json"] = test_data["metadata"]
        temp_store["experiment2/config.json"] = test_data["config"]
        
        # Test keys() method works correctly
        keys = list(temp_store.keys())
        expected_keys = [
            "experiment1/config",
            "experiment1/results", 
            "experiment1/metadata",
            "experiment2/config"
        ]
        
        for expected_key in expected_keys:
            assert expected_key in keys
        
        # Verify we get reasonable number of keys (not character explosion)
        assert len(keys) == 4
        
        # Test data retrieval works
        loaded_config = temp_store["experiment1/config.json"]
        assert loaded_config == test_data["config"]
        
        # Test path syntax integration
        exp1_path = temp_store / "experiment1"
        loaded_results = exp1_path["results.json"]
        assert loaded_results == test_data["results"]

    def test_cache_and_keys_interaction(self):
        """Test that caching and keys() method work together."""
        config = StorageConfig(cache_enabled=True)
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            store = AutoStore(temp_dir, config)
            
            # Store some data
            store["cached_file.json"] = {"cached": True}
            
            # Verify cache manager is working
            assert store.backend.cache_manager is not None
            
            # Verify keys() works with caching enabled
            keys = list(store.keys())
            assert "cached_file" in keys
            assert len(keys) == 1
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)