"""Tests for storage backend functionality in AutoStore."""

import pytest
import tempfile
import shutil
import os
from pathlib import Path
from unittest.mock import Mock, patch

from autostore.autostore import (
    StorageBackend, LocalFileBackend, LocalFileConfig, StorageConfig,
    BackendRegistry, CacheManager, FileMetadata,
    StorageError, StorageFileNotFoundError, StoragePermissionError,
    CONTENT_TYPES
)


class TestLocalFileBackend:
    """Test the LocalFileBackend class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def backend(self, temp_dir):
        """Create a LocalFileBackend for testing."""
        config = LocalFileConfig()
        return LocalFileBackend(str(temp_dir), config)

    def test_initialization_with_file_scheme(self, temp_dir):
        """Test initialization with file:// scheme."""
        uri = f"file://{temp_dir}"
        config = LocalFileConfig()
        backend = LocalFileBackend(uri, config)
        
        assert backend.root_path.samefile(temp_dir)
        assert backend.scheme == "file"

    def test_initialization_with_no_scheme(self, temp_dir):
        """Test initialization with no scheme (plain path)."""
        config = LocalFileConfig()
        backend = LocalFileBackend(str(temp_dir), config)
        
        assert backend.root_path.samefile(temp_dir)

    def test_initialization_creates_directory(self):
        """Test that initialization creates the root directory."""
        temp_dir = Path(tempfile.mkdtemp())
        shutil.rmtree(temp_dir)  # Remove it first
        
        assert not temp_dir.exists()
        
        config = LocalFileConfig()
        backend = LocalFileBackend(str(temp_dir), config)
        
        assert temp_dir.exists()
        shutil.rmtree(temp_dir)

    def test_initialization_invalid_scheme(self, temp_dir):
        """Test initialization with invalid scheme raises error."""
        config = LocalFileConfig()
        
        with pytest.raises(ValueError, match="Unsupported scheme"):
            LocalFileBackend(f"http://{temp_dir}", config)

    def test_get_full_path_normal(self, backend):
        """Test _get_full_path with normal paths."""
        result = backend._get_full_path("test/file.txt")
        expected = backend.root_path / "test" / "file.txt"
        assert result == expected

    def test_get_full_path_backslashes(self, backend):
        """Test _get_full_path normalizes backslashes."""
        result = backend._get_full_path("test\\file.txt")
        expected = backend.root_path / "test" / "file.txt"
        assert result == expected

    def test_get_full_path_security_check(self, backend):
        """Test _get_full_path prevents directory traversal."""
        with pytest.raises(StoragePermissionError, match="resolves outside of root"):
            backend._get_full_path("../../../etc/passwd")

    def test_exists_file(self, backend):
        """Test exists() with existing file."""
        test_file = backend.root_path / "test.txt"
        test_file.write_text("test content")
        
        assert backend.exists("test.txt")
        assert not backend.exists("nonexistent.txt")

    def test_exists_directory(self, backend):
        """Test exists() with existing directory."""
        test_dir = backend.root_path / "testdir"
        test_dir.mkdir()
        
        assert backend.exists("testdir")

    def test_download_file(self, backend):
        """Test downloading (copying) a file."""
        # Create source file
        source_file = backend.root_path / "source.txt"
        source_content = "test content"
        source_file.write_text(source_content)
        
        # Download to temp location
        temp_dest = Path(tempfile.mktemp())
        try:
            backend.download("source.txt", temp_dest)
            
            assert temp_dest.exists()
            assert temp_dest.read_text() == source_content
        finally:
            if temp_dest.exists():
                temp_dest.unlink()

    def test_download_nonexistent_file(self, backend):
        """Test downloading nonexistent file raises error."""
        temp_dest = Path(tempfile.mktemp())
        
        with pytest.raises(StorageFileNotFoundError, match="File not found"):
            backend.download("nonexistent.txt", temp_dest)

    def test_upload_file(self, backend):
        """Test uploading (copying) a file."""
        # Create source file
        temp_source = Path(tempfile.mktemp())
        source_content = "upload test content"
        temp_source.write_text(source_content)
        
        try:
            backend.upload(temp_source, "uploaded.txt")
            
            dest_file = backend.root_path / "uploaded.txt"
            assert dest_file.exists()
            assert dest_file.read_text() == source_content
        finally:
            temp_source.unlink()

    def test_upload_creates_directories(self, backend):
        """Test upload creates parent directories."""
        temp_source = Path(tempfile.mktemp())
        temp_source.write_text("test")
        
        try:
            backend.upload(temp_source, "nested/dir/file.txt")
            
            dest_file = backend.root_path / "nested" / "dir" / "file.txt"
            assert dest_file.exists()
            assert dest_file.read_text() == "test"
        finally:
            temp_source.unlink()

    def test_delete_file(self, backend):
        """Test deleting a file."""
        test_file = backend.root_path / "to_delete.txt"
        test_file.write_text("delete me")
        
        assert test_file.exists()
        backend.delete("to_delete.txt")
        assert not test_file.exists()

    def test_delete_directory(self, backend):
        """Test deleting a directory."""
        test_dir = backend.root_path / "to_delete_dir"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content")
        
        assert test_dir.exists()
        backend.delete("to_delete_dir")
        assert not test_dir.exists()

    def test_delete_nonexistent(self, backend):
        """Test deleting nonexistent file raises error."""
        with pytest.raises(StorageFileNotFoundError, match="File not found"):
            backend.delete("nonexistent.txt")

    def test_list_files_simple(self, backend):
        """Test listing files with simple pattern."""
        # Create test files
        (backend.root_path / "file1.txt").write_text("1")
        (backend.root_path / "file2.txt").write_text("2")
        (backend.root_path / "file3.json").write_text("{}")
        
        # List all files
        files = list(backend.list_files("*"))
        assert "file1.txt" in files
        assert "file2.txt" in files
        assert "file3.json" in files

    def test_list_files_pattern(self, backend):
        """Test listing files with specific pattern."""
        # Create test files
        (backend.root_path / "test1.txt").write_text("1")
        (backend.root_path / "test2.txt").write_text("2")
        (backend.root_path / "other.json").write_text("{}")
        
        # List only txt files
        txt_files = list(backend.list_files("*.txt"))
        assert "test1.txt" in txt_files
        assert "test2.txt" in txt_files
        assert "other.json" not in txt_files

    def test_list_files_recursive(self, backend):
        """Test recursive file listing."""
        # Create nested structure
        nested_dir = backend.root_path / "nested" / "deep"
        nested_dir.mkdir(parents=True)
        
        (backend.root_path / "root.txt").write_text("root")
        (backend.root_path / "nested" / "nested.txt").write_text("nested")
        (nested_dir / "deep.txt").write_text("deep")
        
        # List recursively
        files = list(backend.list_files("*.txt", recursive=True))
        assert "root.txt" in files
        assert "nested/nested.txt" in files
        assert "nested/deep/deep.txt" in files

    def test_list_files_non_recursive(self, backend):
        """Test non-recursive file listing."""
        # Create nested structure
        nested_dir = backend.root_path / "nested"
        nested_dir.mkdir()
        
        (backend.root_path / "root.txt").write_text("root")
        (nested_dir / "nested.txt").write_text("nested")
        
        # List non-recursively
        files = list(backend.list_files("*.txt", recursive=False))
        assert "root.txt" in files
        assert "nested/nested.txt" not in files

    def test_get_metadata(self, backend):
        """Test getting file metadata."""
        test_file = backend.root_path / "metadata_test.txt"
        test_content = "test content for metadata"
        test_file.write_text(test_content)
        
        metadata = backend.get_metadata("metadata_test.txt")
        
        assert isinstance(metadata, FileMetadata)
        assert metadata.size == len(test_content.encode('utf-8'))
        assert metadata.content_type == "text/plain"
        assert metadata.modified_time is not None
        assert metadata.created_time is not None
        assert "mode" in metadata.extra

    def test_get_metadata_nonexistent(self, backend):
        """Test getting metadata for nonexistent file."""
        with pytest.raises(StorageFileNotFoundError, match="File not found"):
            backend.get_metadata("nonexistent.txt")

    def test_guess_content_type(self, backend):
        """Test content type guessing."""
        assert backend._guess_content_type(".txt") == "text/plain"
        assert backend._guess_content_type(".json") == "application/json"
        assert backend._guess_content_type(".unknown") is None

    def test_copy_file(self, backend):
        """Test copying a file."""
        # Create source file
        source_file = backend.root_path / "source.txt"
        source_content = "copy test"
        source_file.write_text(source_content)
        
        backend.copy("source.txt", "destination.txt")
        
        dest_file = backend.root_path / "destination.txt"
        assert dest_file.exists()
        assert dest_file.read_text() == source_content
        assert source_file.exists()  # Original should still exist

    def test_move_file(self, backend):
        """Test moving a file."""
        # Create source file
        source_file = backend.root_path / "source.txt"
        source_content = "move test"
        source_file.write_text(source_content)
        
        backend.move("source.txt", "destination.txt")
        
        dest_file = backend.root_path / "destination.txt"
        assert dest_file.exists()
        assert dest_file.read_text() == source_content
        assert not source_file.exists()  # Original should be gone

    def test_get_size(self, backend):
        """Test getting file size."""
        test_file = backend.root_path / "size_test.txt"
        test_content = "size test content"
        test_file.write_text(test_content)
        
        size = backend.get_size("size_test.txt")
        assert size == len(test_content.encode('utf-8'))

    def test_is_directory(self, backend):
        """Test directory detection."""
        # Create directory with files
        test_dir = backend.root_path / "testdir"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content")
        
        # Create regular file
        test_file = backend.root_path / "testfile.txt"
        test_file.write_text("content")
        
        assert backend.is_directory("testdir")
        assert not backend.is_directory("testfile.txt")
        assert not backend.is_directory("nonexistent")

    def test_download_with_cache_local(self, backend):
        """Test download_with_cache returns original path for local files."""
        test_file = backend.root_path / "cache_test.txt"
        test_file.write_text("cache test")
        
        result = backend.download_with_cache("cache_test.txt")
        assert result == test_file

    def test_context_manager(self, backend):
        """Test backend as context manager."""
        with backend as ctx_backend:
            assert ctx_backend is backend
        # Should not raise any exceptions

    def test_repr(self, backend):
        """Test string representation."""
        repr_str = repr(backend)
        assert "LocalFileBackend" in repr_str
        assert backend.uri in repr_str


class TestStorageBackendAbstract:
    """Test the abstract StorageBackend class."""

    def test_abstract_methods(self):
        """Test that StorageBackend cannot be instantiated."""
        config = StorageConfig()
        
        with pytest.raises(TypeError):
            StorageBackend("test://uri", config)

    def test_concrete_methods(self):
        """Test concrete methods in StorageBackend."""
        # Create a mock concrete implementation
        class MockBackend(StorageBackend):
            def exists(self, path): return True
            def download(self, remote_path, local_path): pass
            def upload(self, local_path, remote_path): pass
            def delete(self, path): pass
            def list_files(self, pattern="*", recursive=True): return iter([])
            def get_metadata(self, path): return FileMetadata(0, None)
        
        config = StorageConfig()
        backend = MockBackend("test://uri", config)
        
        assert backend.uri == "test://uri"
        assert backend.config is config
        assert backend.scheme == "test"

    def test_get_temp_dir(self):
        """Test temporary directory creation."""
        class MockBackend(StorageBackend):
            def exists(self, path): return True
            def download(self, remote_path, local_path): pass
            def upload(self, local_path, remote_path): pass
            def delete(self, path): pass
            def list_files(self, pattern="*", recursive=True): return iter([])
            def get_metadata(self, path): return FileMetadata(0, None)
        
        config = StorageConfig()
        backend = MockBackend("test://uri", config)
        
        temp_dir1 = backend.get_temp_dir()
        temp_dir2 = backend.get_temp_dir()
        
        assert temp_dir1.exists()
        assert temp_dir1 == temp_dir2  # Should return same dir
        assert "autostore_temp_" in temp_dir1.name

    def test_download_with_cache_no_cache_manager(self):
        """Test download_with_cache without cache manager."""
        class MockBackend(StorageBackend):
            def exists(self, path): return True
            def download(self, remote_path, local_path): 
                local_path.write_text("test")
            def upload(self, local_path, remote_path): pass
            def delete(self, path): pass
            def list_files(self, pattern="*", recursive=True): return iter([])
            def get_metadata(self, path): return FileMetadata(0, None)
        
        config = StorageConfig(cache_enabled=False)
        backend = MockBackend("test://uri", config)
        
        result = backend.download_with_cache("test.txt")
        assert result.exists()
        assert result.read_text() == "test"


class TestBackendRegistry:
    """Test the BackendRegistry class."""

    @pytest.fixture
    def registry(self):
        """Create a fresh registry for testing."""
        return BackendRegistry()

    def test_default_backends_registered(self, registry):
        """Test that default backends are registered."""
        schemes = registry.get_supported_schemes()
        assert "file" in schemes
        assert "" in schemes  # Empty scheme for local files

    def test_get_backend_class(self, registry):
        """Test getting backend class by scheme."""
        backend_class = registry.get_backend_class("file")
        assert backend_class is LocalFileBackend
        
        backend_class = registry.get_backend_class("")
        assert backend_class is LocalFileBackend

    def test_get_backend_class_case_insensitive(self, registry):
        """Test getting backend class is case insensitive."""
        backend_class = registry.get_backend_class("FILE")
        assert backend_class is LocalFileBackend

    def test_get_backend_class_nonexistent(self, registry):
        """Test getting nonexistent backend class returns None."""
        backend_class = registry.get_backend_class("nonexistent")
        assert backend_class is None

    def test_register_custom_backend(self, registry):
        """Test registering custom backend."""
        class CustomBackend(StorageBackend):
            def exists(self, path): return True
            def download(self, remote_path, local_path): pass
            def upload(self, local_path, remote_path): pass
            def delete(self, path): pass
            def list_files(self, pattern="*", recursive=True): return iter([])
            def get_metadata(self, path): return FileMetadata(0, None)
        
        registry.register("custom", CustomBackend)
        
        backend_class = registry.get_backend_class("custom")
        assert backend_class is CustomBackend
        
        schemes = registry.get_supported_schemes()
        assert "custom" in schemes

    def test_unregister_backend(self, registry):
        """Test unregistering backend."""
        # Verify file backend exists
        assert registry.get_backend_class("file") is not None
        
        # Unregister it
        registry.unregister("file")
        
        # Verify it's gone
        assert registry.get_backend_class("file") is None
        assert "file" not in registry.get_supported_schemes()

    def test_register_overwrites_existing(self, registry):
        """Test that registering overwrites existing backend."""
        class NewLocalBackend(StorageBackend):
            def exists(self, path): return True
            def download(self, remote_path, local_path): pass
            def upload(self, local_path, remote_path): pass
            def delete(self, path): pass
            def list_files(self, pattern="*", recursive=True): return iter([])
            def get_metadata(self, path): return FileMetadata(0, None)
        
        # Register over existing "file" scheme
        registry.register("file", NewLocalBackend)
        
        backend_class = registry.get_backend_class("file")
        assert backend_class is NewLocalBackend


class TestFileMetadata:
    """Test the FileMetadata dataclass."""

    def test_basic_metadata(self):
        """Test basic metadata creation."""
        from datetime import datetime
        
        now = datetime.now()
        metadata = FileMetadata(
            size=1024,
            modified_time=now,
            created_time=now,
            content_type="text/plain"
        )
        
        assert metadata.size == 1024
        assert metadata.modified_time == now
        assert metadata.created_time == now
        assert metadata.content_type == "text/plain"
        assert metadata.etag is None
        assert metadata.extra == {}

    def test_metadata_with_extra(self):
        """Test metadata with extra fields."""
        from datetime import datetime
        
        metadata = FileMetadata(
            size=2048,
            modified_time=datetime.now(),
            extra={"custom_field": "value", "permissions": "755"}
        )
        
        assert metadata.extra["custom_field"] == "value"
        assert metadata.extra["permissions"] == "755"


class TestStorageErrors:
    """Test storage exception classes."""

    def test_storage_error_hierarchy(self):
        """Test that storage errors inherit properly."""
        assert issubclass(StorageFileNotFoundError, StorageError)
        assert issubclass(StoragePermissionError, StorageError)
        assert issubclass(StorageError, Exception)

    def test_storage_error_messages(self):
        """Test storage error messages."""
        file_error = StorageFileNotFoundError("File not found: test.txt")
        assert str(file_error) == "File not found: test.txt"
        
        perm_error = StoragePermissionError("Permission denied: test.txt")
        assert str(perm_error) == "Permission denied: test.txt"


class TestStorageBackendIntegration:
    """Integration tests for storage backend functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_complete_file_lifecycle(self, temp_dir):
        """Test complete file lifecycle operations."""
        config = LocalFileConfig()
        backend = LocalFileBackend(str(temp_dir), config)
        
        # Test file doesn't exist initially
        assert not backend.exists("lifecycle.txt")
        
        # Create and upload file
        temp_file = Path(tempfile.mktemp())
        temp_file.write_text("lifecycle test content")
        
        try:
            backend.upload(temp_file, "lifecycle.txt")
            
            # Test file exists
            assert backend.exists("lifecycle.txt")
            
            # Test metadata
            metadata = backend.get_metadata("lifecycle.txt")
            assert metadata.size > 0
            assert metadata.content_type == "text/plain"
            
            # Test download
            download_file = Path(tempfile.mktemp())
            try:
                backend.download("lifecycle.txt", download_file)
                assert download_file.read_text() == "lifecycle test content"
            finally:
                if download_file.exists():
                    download_file.unlink()
            
            # Test copy
            backend.copy("lifecycle.txt", "lifecycle_copy.txt")
            assert backend.exists("lifecycle_copy.txt")
            
            # Test move
            backend.move("lifecycle_copy.txt", "lifecycle_moved.txt")
            assert backend.exists("lifecycle_moved.txt")
            assert not backend.exists("lifecycle_copy.txt")
            
            # Test listing
            files = list(backend.list_files("lifecycle*.txt"))
            assert "lifecycle.txt" in files
            assert "lifecycle_moved.txt" in files
            
            # Test deletion
            backend.delete("lifecycle.txt")
            backend.delete("lifecycle_moved.txt")
            assert not backend.exists("lifecycle.txt")
            assert not backend.exists("lifecycle_moved.txt")
            
        finally:
            if temp_file.exists():
                temp_file.unlink()

    def test_nested_directory_operations(self, temp_dir):
        """Test operations with nested directories."""
        config = LocalFileConfig()
        backend = LocalFileBackend(str(temp_dir), config)
        
        # Create nested structure
        temp_file = Path(tempfile.mktemp())
        temp_file.write_text("nested content")
        
        try:
            backend.upload(temp_file, "level1/level2/level3/nested.txt")
            
            # Test file exists in nested location
            assert backend.exists("level1/level2/level3/nested.txt")
            
            # Test listing with patterns
            all_files = list(backend.list_files("**/*.txt"))
            assert "level1/level2/level3/nested.txt" in all_files
            
            # Test directory detection
            assert backend.is_directory("level1")
            assert backend.is_directory("level1/level2")
            
        finally:
            if temp_file.exists():
                temp_file.unlink()

    def test_backend_with_caching(self, temp_dir):
        """Test backend with caching enabled."""
        config = LocalFileConfig(cache_enabled=True)
        backend = LocalFileBackend(str(temp_dir), config)
        
        # Create test file
        test_file = backend.root_path / "cached_test.txt"
        test_file.write_text("cached content")
        
        # For LocalFileBackend, download_with_cache should return original path
        result = backend.download_with_cache("cached_test.txt")
        assert result == test_file
        
        # Test cleanup
        backend.cleanup()

    def test_error_conditions(self, temp_dir):
        """Test various error conditions."""
        config = LocalFileConfig()
        backend = LocalFileBackend(str(temp_dir), config)
        
        # Test download nonexistent file
        temp_dest = Path(tempfile.mktemp())
        with pytest.raises(StorageFileNotFoundError):
            backend.download("nonexistent.txt", temp_dest)
        
        # Test delete nonexistent file
        with pytest.raises(StorageFileNotFoundError):
            backend.delete("nonexistent.txt")
        
        # Test metadata for nonexistent file
        with pytest.raises(StorageFileNotFoundError):
            backend.get_metadata("nonexistent.txt")
        
        # Test path traversal security
        with pytest.raises(StoragePermissionError):
            backend._get_full_path("../../../etc/passwd")