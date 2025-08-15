"""Tests for utility functions in AutoStore."""

import pytest
import tempfile
import os
from pathlib import Path

from autostore.autostore import (
    hash_obj, 
    config, 
    setup_logging, 
    find_dotenv, 
    _walk_to_root,
    CONTENT_TYPES
)


class TestHashObj:
    """Test the hash_obj function."""

    def test_hash_string(self):
        """Test hashing of strings."""
        result1 = hash_obj("test_string")
        result2 = hash_obj("test_string")
        result3 = hash_obj("different_string")
        
        # Same input should produce same hash
        assert result1 == result2
        # Different input should produce different hash
        assert result1 != result3
        # Should be 32-character MD5 hash
        assert len(result1) == 32
        assert len(result3) == 32

    def test_hash_with_seed(self):
        """Test hashing with different seeds."""
        string = "test_string"
        result1 = hash_obj(string, seed=123)
        result2 = hash_obj(string, seed=456)
        
        # Different seeds should produce different hashes
        assert result1 != result2

    def test_hash_list_tuple(self):
        """Test hashing of lists and tuples."""
        list_obj = ["a", "b", "c"]
        tuple_obj = ("a", "b", "c")
        
        result_list = hash_obj(list_obj)
        result_tuple = hash_obj(tuple_obj)
        
        # Lists and tuples with same elements should produce same hash
        assert result_list == result_tuple
        assert len(result_list) == 32

    def test_hash_bytes(self):
        """Test hashing of bytes objects."""
        bytes_obj = b"test_bytes"
        result = hash_obj(bytes_obj)
        
        assert len(result) == 32
        assert isinstance(result, str)

    def test_hash_dict(self):
        """Test hashing of dictionaries."""
        dict1 = {"key1": "value1", "key2": "value2"}
        dict2 = {"key2": "value2", "key1": "value1"}  # Different order
        
        result1 = hash_obj(dict1)
        result2 = hash_obj(dict2)
        
        # Should produce same hash regardless of key order (sorted)
        assert result1 == result2
        assert len(result1) == 32

    def test_hash_non_serializable_object(self, caplog):
        """Test hashing of non-serializable objects."""
        class CustomObject:
            pass
        
        obj = CustomObject()
        result = hash_obj(obj)
        
        # Should still produce a valid hash using object ID
        assert len(result) == 32
        # Should log a warning
        assert len(caplog.records) == 1
        assert "cannot be serialized" in caplog.records[0].message


class TestConfigFunction:
    """Test the config function for environment variable handling."""

    def test_config_existing_env_var(self):
        """Test reading existing environment variable."""
        os.environ["TEST_CONFIG_VAR"] = "test_value"
        try:
            result = config("TEST_CONFIG_VAR")
            assert result == "test_value"
        finally:
            del os.environ["TEST_CONFIG_VAR"]

    def test_config_missing_env_var_with_default(self):
        """Test missing environment variable with default."""
        # Ensure the key doesn't exist
        os.environ.pop("NONEXISTENT_VAR", None)
        
        result = config("NONEXISTENT_VAR", default="default_value")
        assert result == "default_value"

    def test_config_type_casting_int(self):
        """Test type casting to integer."""
        os.environ["TEST_INT_VAR"] = "42"
        try:
            result = config("TEST_INT_VAR", cast=int)
            assert result == 42
            assert isinstance(result, int)
        finally:
            del os.environ["TEST_INT_VAR"]

    def test_config_type_casting_float(self):
        """Test type casting to float."""
        os.environ["TEST_FLOAT_VAR"] = "3.14"
        try:
            result = config("TEST_FLOAT_VAR", cast=float)
            assert result == 3.14
            assert isinstance(result, float)
        finally:
            del os.environ["TEST_FLOAT_VAR"]

    def test_config_bool_casting_true_values(self):
        """Test boolean casting for true values."""
        true_values = ["true", "True", "1"]
        
        for value in true_values:
            os.environ["TEST_BOOL_VAR"] = value
            try:
                result = config("TEST_BOOL_VAR", cast=bool)
                assert result is True
            finally:
                del os.environ["TEST_BOOL_VAR"]

    def test_config_bool_casting_false_values(self):
        """Test boolean casting for false values."""
        false_values = ["false", "False", "0"]
        
        for value in false_values:
            os.environ["TEST_BOOL_VAR"] = value
            try:
                result = config("TEST_BOOL_VAR", cast=bool)
                assert result is False
            finally:
                del os.environ["TEST_BOOL_VAR"]

    def test_config_bool_casting_invalid_value(self):
        """Test boolean casting with invalid value."""
        os.environ["TEST_BOOL_VAR"] = "invalid"
        try:
            with pytest.raises(ValueError, match="Not a valid bool"):
                config("TEST_BOOL_VAR", cast=bool)
        finally:
            del os.environ["TEST_BOOL_VAR"]

    def test_config_invalid_type_casting(self):
        """Test invalid type casting."""
        os.environ["TEST_INVALID_VAR"] = "not_a_number"
        try:
            with pytest.raises(ValueError, match="Not a valid int"):
                config("TEST_INVALID_VAR", cast=int)
        finally:
            del os.environ["TEST_INVALID_VAR"]

    def test_config_cast_default_value(self):
        """Test casting default value when env var doesn't exist."""
        os.environ.pop("NONEXISTENT_VAR", None)
        
        result = config("NONEXISTENT_VAR", cast=int, default="42")
        assert result == 42
        assert isinstance(result, int)


class TestSetupLogging:
    """Test the setup_logging function."""

    def test_setup_logging_default(self):
        """Test logging setup with default parameters."""
        # This is hard to test without affecting global logging state
        # Just ensure it doesn't raise an exception
        setup_logging()

    def test_setup_logging_with_file(self):
        """Test logging setup with file output."""
        temp_dir = Path(tempfile.mkdtemp())
        log_file = temp_dir / "test.log"
        
        try:
            setup_logging(file=str(log_file))
            assert log_file.exists()
        finally:
            if log_file.exists():
                log_file.unlink()
            temp_dir.rmdir()

    def test_setup_logging_string_level(self):
        """Test logging setup with string level."""
        # Should not raise an exception
        setup_logging(level="DEBUG")

    def test_setup_logging_disable_stdout(self):
        """Test logging setup with stdout disabled."""
        # Should not raise an exception
        setup_logging(disable_stdout=True)

    def test_setup_logging_disable_stdout_no_file(self):
        """Test logging setup with stdout disabled and no file."""
        # Should return early without setting up logging
        result = setup_logging(disable_stdout=True, file=None)
        assert result is None


class TestWalkToRoot:
    """Test the _walk_to_root function."""

    def test_walk_to_root_valid_path(self):
        """Test walking to root from valid path."""
        temp_dir = Path(tempfile.mkdtemp())
        sub_dir = temp_dir / "sub" / "dir"
        sub_dir.mkdir(parents=True)
        
        try:
            paths = list(_walk_to_root(str(sub_dir)))
            
            # Should include the sub_dir and parent directories
            assert str(sub_dir) in paths
            assert str(temp_dir / "sub") in paths
            assert str(temp_dir) in paths
            
            # Paths should be in order from deepest to root
            assert paths[0] == str(sub_dir)
            
        finally:
            import shutil
            shutil.rmtree(temp_dir)

    def test_walk_to_root_file_path(self):
        """Test walking to root from file path."""
        temp_dir = Path(tempfile.mkdtemp())
        test_file = temp_dir / "test.txt"
        test_file.write_text("test")
        
        try:
            paths = list(_walk_to_root(str(test_file)))
            
            # Should start from the file's directory, not the file itself
            assert str(temp_dir) in paths
            assert str(test_file) not in paths
            
        finally:
            import shutil
            shutil.rmtree(temp_dir)

    def test_walk_to_root_nonexistent_path(self):
        """Test walking to root from nonexistent path."""
        with pytest.raises(IOError, match="Starting path not found"):
            list(_walk_to_root("/nonexistent/path"))


class TestFindDotenv:
    """Test the find_dotenv function."""

    def test_find_dotenv_existing_file(self):
        """Test finding existing .env file."""
        temp_dir = Path(tempfile.mkdtemp())
        env_file = temp_dir / ".env"
        env_file.write_text("TEST_VAR=test_value")
        
        try:
            # Change to temp directory
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            result = find_dotenv(usecwd=True)
            assert Path(result).samefile(env_file)
            
        finally:
            os.chdir(original_cwd)
            import shutil
            shutil.rmtree(temp_dir)

    def test_find_dotenv_custom_filename(self):
        """Test finding custom named env file."""
        temp_dir = Path(tempfile.mkdtemp())
        env_file = temp_dir / "custom.env"
        env_file.write_text("TEST_VAR=test_value")
        
        try:
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            result = find_dotenv(filename="custom.env", usecwd=True)
            assert Path(result).samefile(env_file)
            
        finally:
            os.chdir(original_cwd)
            import shutil
            shutil.rmtree(temp_dir)

    def test_find_dotenv_not_found_no_error(self):
        """Test find_dotenv when file not found without raising error."""
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            result = find_dotenv(usecwd=True)
            assert result == ""
            
        finally:
            os.chdir(original_cwd)
            import shutil
            shutil.rmtree(temp_dir)

    def test_find_dotenv_not_found_raise_error(self):
        """Test find_dotenv when file not found with error raising."""
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            with pytest.raises(IOError, match="File not found"):
                find_dotenv(raise_error_if_not_found=True, usecwd=True)
                
        finally:
            os.chdir(original_cwd)
            import shutil
            shutil.rmtree(temp_dir)

    def test_find_dotenv_in_parent_directory(self):
        """Test finding .env file in parent directory."""
        temp_dir = Path(tempfile.mkdtemp())
        sub_dir = temp_dir / "sub"
        sub_dir.mkdir()
        env_file = temp_dir / ".env"
        env_file.write_text("TEST_VAR=test_value")
        
        try:
            original_cwd = os.getcwd()
            os.chdir(sub_dir)
            
            result = find_dotenv(usecwd=True)
            assert Path(result).samefile(env_file)
            
        finally:
            os.chdir(original_cwd)
            import shutil
            shutil.rmtree(temp_dir)


class TestContentTypes:
    """Test the CONTENT_TYPES constant."""

    def test_content_types_common_extensions(self):
        """Test that common file extensions have correct content types."""
        assert CONTENT_TYPES[".txt"] == "text/plain"
        assert CONTENT_TYPES[".json"] == "application/json"
        assert CONTENT_TYPES[".html"] == "text/html"
        assert CONTENT_TYPES[".csv"] == "text/csv"
        assert CONTENT_TYPES[".png"] == "image/png"
        assert CONTENT_TYPES[".jpg"] == "image/jpeg"
        assert CONTENT_TYPES[".jpeg"] == "image/jpeg"

    def test_content_types_binary_formats(self):
        """Test that binary formats have octet-stream content type."""
        binary_extensions = [".parquet", ".pkl", ".pt", ".pth", ".npy", ".npz"]
        
        for ext in binary_extensions:
            assert CONTENT_TYPES[ext] == "application/octet-stream"

    def test_content_types_yaml_formats(self):
        """Test YAML content types."""
        assert CONTENT_TYPES[".yaml"] == "application/x-yaml"
        assert CONTENT_TYPES[".yml"] == "application/x-yaml"

    def test_content_types_completeness(self):
        """Test that content types dict contains expected entries."""
        expected_extensions = [
            ".txt", ".html", ".json", ".csv", ".yaml", ".yml",
            ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".zip",
            ".parquet", ".pkl", ".pt", ".pth", ".npy", ".npz"
        ]
        
        for ext in expected_extensions:
            assert ext in CONTENT_TYPES, f"Missing content type for {ext}"


class TestUtilityIntegration:
    """Integration tests for utility functions."""

    def test_config_and_hash_integration(self):
        """Test integration between config and hash functions."""
        # Set up environment variable
        os.environ["HASH_TEST_VAR"] = "test_value_for_hashing"
        
        try:
            # Get value from config
            value = config("HASH_TEST_VAR")
            
            # Hash the value
            hash_result = hash_obj(value)
            
            assert len(hash_result) == 32
            assert isinstance(hash_result, str)
            
            # Same value should produce same hash
            hash_result2 = hash_obj(value)
            assert hash_result == hash_result2
            
        finally:
            del os.environ["HASH_TEST_VAR"]

    def test_find_dotenv_and_config_integration(self):
        """Test integration between find_dotenv and environment configuration."""
        temp_dir = Path(tempfile.mkdtemp())
        env_file = temp_dir / ".env"
        env_file.write_text("INTEGRATION_TEST_VAR=integration_value")
        
        try:
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            # Find the .env file
            found_path = find_dotenv(usecwd=True)
            # Use resolved paths for comparison to handle symlinks (e.g., /var -> /private/var on macOS)
            assert Path(found_path).resolve() == env_file.resolve()
            
            # Load it (we'll test load_dotenv in another test file)
            # For now, just verify the file exists and has content
            assert env_file.exists()
            assert "INTEGRATION_TEST_VAR=integration_value" in env_file.read_text()
            
        finally:
            os.chdir(original_cwd)
            import shutil
            shutil.rmtree(temp_dir)