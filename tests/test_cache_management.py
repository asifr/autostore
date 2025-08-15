"""Tests for cache management functionality in AutoStore."""

import pytest
import tempfile
import shutil
import time
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from autostore.autostore import CacheManager, CacheEntry


class TestCacheEntry:
    """Test the CacheEntry dataclass."""

    def test_cache_entry_creation(self):
        """Test basic cache entry creation."""
        file_path = Path("/tmp/test.txt")
        created_time = datetime.now()
        
        entry = CacheEntry(
            file_path=file_path,
            created_time=created_time,
            etag="abc123",
            size=1024
        )
        
        assert entry.file_path == file_path
        assert entry.created_time == created_time
        assert entry.etag == "abc123"
        assert entry.size == 1024

    def test_cache_entry_defaults(self):
        """Test cache entry with default values."""
        file_path = Path("/tmp/test.txt")
        created_time = datetime.now()
        
        entry = CacheEntry(
            file_path=file_path,
            created_time=created_time
        )
        
        assert entry.etag is None
        assert entry.size == 0


class TestCacheManager:
    """Test the CacheManager class."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary cache directory."""
        temp_dir = Path(tempfile.mkdtemp(prefix="autostore_test_cache_"))
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def cache_manager(self, temp_cache_dir):
        """Create a CacheManager for testing."""
        return CacheManager(cache_dir=str(temp_cache_dir), expiry_hours=1)

    def test_initialization_default_cache_dir(self):
        """Test initialization with default cache directory."""
        cache_manager = CacheManager()
        
        assert cache_manager.cache_dir.exists()
        assert "autostore_cache" in str(cache_manager.cache_dir)
        assert cache_manager.expiry_hours == 24

    def test_initialization_custom_cache_dir(self, temp_cache_dir):
        """Test initialization with custom cache directory."""
        cache_manager = CacheManager(cache_dir=str(temp_cache_dir), expiry_hours=12)
        
        assert cache_manager.cache_dir == temp_cache_dir
        assert cache_manager.expiry_hours == 12

    def test_cache_dir_creation(self):
        """Test that cache directory is created if it doesn't exist."""
        temp_dir = Path(tempfile.mkdtemp())
        cache_dir = temp_dir / "nonexistent_cache"
        shutil.rmtree(temp_dir)
        
        assert not cache_dir.exists()
        
        cache_manager = CacheManager(cache_dir=str(cache_dir))
        assert cache_dir.exists()
        
        shutil.rmtree(cache_dir.parent)

    def test_get_temp_dir(self, cache_manager):
        """Test temporary directory creation."""
        temp_dir1 = cache_manager.get_temp_dir()
        temp_dir2 = cache_manager.get_temp_dir()
        
        assert temp_dir1.exists()
        assert temp_dir1 == temp_dir2  # Should return same directory
        assert "autostore_temp_" in temp_dir1.name

    def test_get_cache_key(self, cache_manager):
        """Test cache key generation."""
        backend_uri = "file:///test/path"
        file_path = "data/test.txt"
        
        key1 = cache_manager._get_cache_key(backend_uri, file_path)
        key2 = cache_manager._get_cache_key(backend_uri, file_path)
        key3 = cache_manager._get_cache_key(backend_uri, "different/path.txt")
        
        # Same inputs should produce same key
        assert key1 == key2
        # Different inputs should produce different keys
        assert key1 != key3
        # Should be valid hash (32 characters)
        assert len(key1) == 32
        assert len(key3) == 32

    def test_is_expired(self, cache_manager):
        """Test expiry checking."""
        now = datetime.now()
        
        # Not expired entry
        recent_entry = CacheEntry(
            file_path=Path("/tmp/test.txt"),
            created_time=now - timedelta(minutes=30)  # 30 minutes ago
        )
        assert not cache_manager._is_expired(recent_entry)
        
        # Expired entry
        old_entry = CacheEntry(
            file_path=Path("/tmp/test.txt"),
            created_time=now - timedelta(hours=2)  # 2 hours ago (expiry is 1 hour)
        )
        assert cache_manager._is_expired(old_entry)

    def test_cache_file(self, cache_manager):
        """Test caching a file."""
        # Create source file
        source_file = Path(tempfile.mktemp())
        source_content = "test cache content"
        source_file.write_text(source_content)
        
        try:
            # Cache the file
            cached_path = cache_manager.cache_file(
                backend_uri="file:///test",
                file_path="test.txt",
                local_file_path=source_file,
                etag="test_etag"
            )
            
            # Verify cached file exists and has correct content
            assert cached_path.exists()
            assert cached_path.read_text() == source_content
            assert cached_path.parent == cache_manager.cache_dir
            
            # Verify cache entry was created
            cache_key = cache_manager._get_cache_key("file:///test", "test.txt")
            assert cache_key in cache_manager._cache_index
            
            entry = cache_manager._cache_index[cache_key]
            assert entry.file_path == cached_path
            assert entry.etag == "test_etag"
            assert entry.size > 0
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_get_cached_file_hit(self, cache_manager):
        """Test cache hit when getting cached file."""
        # Create and cache a file first
        source_file = Path(tempfile.mktemp())
        source_file.write_text("cached content")
        
        try:
            cached_path = cache_manager.cache_file(
                "file:///test", "test.txt", source_file, "etag123"
            )
            
            # Now try to get it from cache
            result = cache_manager.get_cached_file(
                "file:///test", "test.txt", "etag123"
            )
            
            assert result == cached_path
            assert result.exists()
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_get_cached_file_miss_no_entry(self, cache_manager):
        """Test cache miss when no entry exists."""
        result = cache_manager.get_cached_file(
            "file:///test", "nonexistent.txt"
        )
        assert result is None

    def test_get_cached_file_miss_file_deleted(self, cache_manager):
        """Test cache miss when cached file was deleted."""
        # Create and cache a file
        source_file = Path(tempfile.mktemp())
        source_file.write_text("content")
        
        try:
            cached_path = cache_manager.cache_file(
                "file:///test", "test.txt", source_file
            )
            
            # Delete the cached file
            cached_path.unlink()
            
            # Try to get it from cache
            result = cache_manager.get_cached_file("file:///test", "test.txt")
            assert result is None
            
            # Entry should be removed from index
            cache_key = cache_manager._get_cache_key("file:///test", "test.txt")
            assert cache_key not in cache_manager._cache_index
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_get_cached_file_expired(self, cache_manager):
        """Test cache miss when entry is expired."""
        # Create and cache a file
        source_file = Path(tempfile.mktemp())
        source_file.write_text("content")
        
        try:
            cached_path = cache_manager.cache_file(
                "file:///test", "test.txt", source_file
            )
            
            # Manually set creation time to past expiry
            cache_key = cache_manager._get_cache_key("file:///test", "test.txt")
            entry = cache_manager._cache_index[cache_key]
            entry.created_time = datetime.now() - timedelta(hours=2)  # Expired
            
            # Try to get from cache
            result = cache_manager.get_cached_file("file:///test", "test.txt")
            assert result is None
            
            # File should be deleted and entry removed
            assert not cached_path.exists()
            assert cache_key not in cache_manager._cache_index
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_get_cached_file_etag_mismatch(self, cache_manager):
        """Test cache miss when etag doesn't match."""
        # Create and cache a file
        source_file = Path(tempfile.mktemp())
        source_file.write_text("content")
        
        try:
            cached_path = cache_manager.cache_file(
                "file:///test", "test.txt", source_file, "old_etag"
            )
            
            # Try to get with different etag
            result = cache_manager.get_cached_file(
                "file:///test", "test.txt", "new_etag"
            )
            assert result is None
            
            # File should be deleted and entry removed
            assert not cached_path.exists()
            cache_key = cache_manager._get_cache_key("file:///test", "test.txt")
            assert cache_key not in cache_manager._cache_index
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_get_cached_file_no_etag_comparison(self, cache_manager):
        """Test cache behavior when no etag is provided."""
        # Create and cache a file with etag
        source_file = Path(tempfile.mktemp())
        source_file.write_text("content")
        
        try:
            cached_path = cache_manager.cache_file(
                "file:///test", "test.txt", source_file, "some_etag"
            )
            
            # Get without providing etag (should work)
            result = cache_manager.get_cached_file("file:///test", "test.txt")
            assert result == cached_path
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_cleanup_temp(self, cache_manager):
        """Test cleanup of temporary directory."""
        # Create temp directory
        temp_dir = cache_manager.get_temp_dir()
        assert temp_dir.exists()
        
        # Create some files in temp dir
        (temp_dir / "temp_file.txt").write_text("temp")
        
        # Cleanup
        cache_manager.cleanup_temp()
        
        # Temp directory should be gone
        assert not temp_dir.exists()
        
        # Getting temp dir again should create new one
        new_temp_dir = cache_manager.get_temp_dir()
        assert new_temp_dir.exists()
        assert new_temp_dir != temp_dir

    def test_cleanup_expired(self, cache_manager):
        """Test cleanup of expired cache entries."""
        # Create multiple cache entries
        source1 = Path(tempfile.mktemp())
        source2 = Path(tempfile.mktemp())
        source1.write_text("content1")
        source2.write_text("content2")
        
        try:
            # Cache two files
            cached1 = cache_manager.cache_file("uri1", "file1.txt", source1)
            cached2 = cache_manager.cache_file("uri2", "file2.txt", source2)
            
            # Manually expire one entry
            key1 = cache_manager._get_cache_key("uri1", "file1.txt")
            entry1 = cache_manager._cache_index[key1]
            entry1.created_time = datetime.now() - timedelta(hours=2)  # Expired
            
            # Cleanup expired entries
            cache_manager.cleanup_expired()
            
            # Expired entry should be gone
            assert key1 not in cache_manager._cache_index
            assert not cached1.exists()
            
            # Non-expired entry should remain
            key2 = cache_manager._get_cache_key("uri2", "file2.txt")
            assert key2 in cache_manager._cache_index
            assert cached2.exists()
            
        finally:
            for f in [source1, source2]:
                if f.exists():
                    f.unlink()

    def test_cache_key_uniqueness(self, cache_manager):
        """Test that different backend/path combinations produce unique keys."""
        keys = set()
        
        test_cases = [
            ("file:///path1", "test.txt"),
            ("file:///path2", "test.txt"),
            ("file:///path1", "test2.txt"),
            ("s3://bucket1", "test.txt"),
            ("s3://bucket2", "test.txt"),
        ]
        
        for backend_uri, file_path in test_cases:
            key = cache_manager._get_cache_key(backend_uri, file_path)
            assert key not in keys, f"Duplicate key for {backend_uri}:{file_path}"
            keys.add(key)

    def test_cache_with_special_characters(self, cache_manager):
        """Test caching files with special characters in names."""
        source_file = Path(tempfile.mktemp())
        source_file.write_text("special content")
        
        try:
            # Test with special characters
            cached_path = cache_manager.cache_file(
                "file:///test", "file with spaces & symbols!.txt", source_file
            )
            
            assert cached_path.exists()
            assert cached_path.read_text() == "special content"
            
            # Should be able to retrieve it
            result = cache_manager.get_cached_file(
                "file:///test", "file with spaces & symbols!.txt"
            )
            assert result == cached_path
            
        finally:
            if source_file.exists():
                source_file.unlink()


class TestCacheManagerIntegration:
    """Integration tests for cache manager functionality."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary cache directory."""
        temp_dir = Path(tempfile.mkdtemp(prefix="autostore_test_cache_"))
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_cache_lifecycle(self, temp_cache_dir):
        """Test complete cache lifecycle."""
        cache_manager = CacheManager(str(temp_cache_dir), expiry_hours=1)
        
        # Create source file
        source_file = Path(tempfile.mktemp())
        source_file.write_text("lifecycle test")
        
        try:
            backend_uri = "file:///test/backend"
            file_path = "test/file.txt"
            etag = "lifecycle_etag"
            
            # 1. Cache miss initially
            result = cache_manager.get_cached_file(backend_uri, file_path, etag)
            assert result is None
            
            # 2. Cache the file
            cached_path = cache_manager.cache_file(
                backend_uri, file_path, source_file, etag
            )
            assert cached_path.exists()
            
            # 3. Cache hit
            result = cache_manager.get_cached_file(backend_uri, file_path, etag)
            assert result == cached_path
            
            # 4. Cache miss with wrong etag
            result = cache_manager.get_cached_file(backend_uri, file_path, "wrong_etag")
            assert result is None
            
            # 5. Re-cache with new etag
            cached_path2 = cache_manager.cache_file(
                backend_uri, file_path, source_file, "new_etag"
            )
            assert cached_path2.exists()
            
            # 6. Cache hit with new etag
            result = cache_manager.get_cached_file(backend_uri, file_path, "new_etag")
            assert result == cached_path2
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_concurrent_cache_operations(self, temp_cache_dir):
        """Test cache operations with multiple files."""
        cache_manager = CacheManager(str(temp_cache_dir), expiry_hours=2)
        
        # Create multiple source files
        files = []
        cached_paths = []
        
        try:
            for i in range(5):
                source = Path(tempfile.mktemp())
                source.write_text(f"content {i}")
                files.append(source)
                
                # Cache each file
                cached = cache_manager.cache_file(
                    f"backend_{i}", f"file_{i}.txt", source, f"etag_{i}"
                )
                cached_paths.append(cached)
            
            # Verify all cached files exist
            for cached_path in cached_paths:
                assert cached_path.exists()
            
            # Verify we can retrieve all cached files
            for i in range(5):
                result = cache_manager.get_cached_file(
                    f"backend_{i}", f"file_{i}.txt", f"etag_{i}"
                )
                assert result == cached_paths[i]
            
            # Expire some entries and cleanup
            for i in [1, 3]:
                key = cache_manager._get_cache_key(f"backend_{i}", f"file_{i}.txt")
                entry = cache_manager._cache_index[key]
                entry.created_time = datetime.now() - timedelta(hours=3)
            
            cache_manager.cleanup_expired()
            
            # Check that expired entries are gone
            for i in [1, 3]:
                result = cache_manager.get_cached_file(
                    f"backend_{i}", f"file_{i}.txt", f"etag_{i}"
                )
                assert result is None
            
            # Check that non-expired entries remain
            for i in [0, 2, 4]:
                result = cache_manager.get_cached_file(
                    f"backend_{i}", f"file_{i}.txt", f"etag_{i}"
                )
                assert result == cached_paths[i]
                
        finally:
            for source_file in files:
                if source_file.exists():
                    source_file.unlink()

    def test_cache_persistence_across_instances(self, temp_cache_dir):
        """Test that cache persists across CacheManager instances."""
        # Create first cache manager and cache a file
        cache_manager1 = CacheManager(str(temp_cache_dir), expiry_hours=1)
        
        source_file = Path(tempfile.mktemp())
        source_file.write_text("persistent content")
        
        try:
            cached_path = cache_manager1.cache_file(
                "test_backend", "persistent.txt", source_file, "persist_etag"
            )
            assert cached_path.exists()
            
            # Create second cache manager with same cache directory
            cache_manager2 = CacheManager(str(temp_cache_dir), expiry_hours=1)
            
            # The cached file should still exist on disk
            assert cached_path.exists()
            
            # But it won't be in the new cache manager's index
            # (This is expected behavior - cache index is in-memory only)
            result = cache_manager2.get_cached_file(
                "test_backend", "persistent.txt", "persist_etag"
            )
            assert result is None
            
            # However, we can re-cache and it will work
            cached_path2 = cache_manager2.cache_file(
                "test_backend", "persistent.txt", source_file, "persist_etag"
            )
            
            # Should be able to retrieve it now
            result = cache_manager2.get_cached_file(
                "test_backend", "persistent.txt", "persist_etag"
            )
            assert result == cached_path2
            
        finally:
            if source_file.exists():
                source_file.unlink()

    def test_cache_size_tracking(self, temp_cache_dir):
        """Test that cache entries track file sizes correctly."""
        cache_manager = CacheManager(str(temp_cache_dir))
        
        # Create files with different sizes
        test_files = []
        try:
            for i, content in enumerate(["small", "medium content", "large content with more text"]):
                source = Path(tempfile.mktemp())
                source.write_text(content)
                test_files.append(source)
                
                cached_path = cache_manager.cache_file(
                    "size_test", f"file_{i}.txt", source
                )
                
                # Check that size is tracked correctly
                key = cache_manager._get_cache_key("size_test", f"file_{i}.txt")
                entry = cache_manager._cache_index[key]
                expected_size = len(content.encode('utf-8'))
                assert entry.size == expected_size
                
        finally:
            for source in test_files:
                if source.exists():
                    source.unlink()