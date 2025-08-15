"""Test configuration for AutoStore tests."""

import pytest
import tempfile
import shutil
from pathlib import Path

from autostore import AutoStore


@pytest.fixture
def temp_store():
    """Create a temporary AutoStore for testing."""
    temp_dir = Path(tempfile.mkdtemp())
    store = AutoStore(temp_dir)
    yield store
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return {
        "json_data": {"key": "value", "numbers": [1, 2, 3]},
        "text_data": "This is sample text content",
        "complex_data": {
            "nested": {
                "object": {
                    "with": ["multiple", "levels", "of", "nesting"],
                    "and": {"various": "data", "types": True, "numbers": 42}
                }
            }
        }
    }