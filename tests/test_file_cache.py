"""Test file (SQLite) cache backend operations via inheritance-based test suite.
"""
import pytest
from fixtures.backend_suite import _GenericAsyncBackendTestSuite
from fixtures.backend_suite import _GenericBackendTestSuiteWithTTL


class TestFileBackend(_GenericBackendTestSuiteWithTTL):
    """Sync tests for file backend via decorator.
    """

    backend = 'file'

    @pytest.fixture(autouse=True)
    def setup_temp_dir(self, temp_cache_dir):
        """Ensure temp cache dir is available.
        """


class TestAsyncFileBackend(_GenericAsyncBackendTestSuite):
    """Async tests for file backend via decorator.
    """

    backend = 'file'

    @pytest.fixture(autouse=True)
    def setup_temp_dir(self, temp_cache_dir):
        """Ensure temp cache dir is available.
        """
