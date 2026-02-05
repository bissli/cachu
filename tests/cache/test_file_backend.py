"""Test file (SQLite) cache backend operations via inheritance-based test suite.
"""
import tempfile

import pytest
from _fixtures.backend_suite import _GenericAsyncBackendTestSuite
from _fixtures.backend_suite import _GenericAsyncDirectBackendTestSuite
from _fixtures.backend_suite import _GenericBackendTestSuiteWithTTL
from _fixtures.backend_suite import _GenericDirectBackendTestSuite
from cachu.backends.sqlite import SqliteBackend


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


class TestSqliteBackendDirect(_GenericDirectBackendTestSuite):
    """Direct API tests for SqliteBackend.
    """

    @pytest.fixture(autouse=True)
    def setup_backend(self):
        """Create temp file for SQLite database.
        """
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            self._filepath = f.name

    def create_backend(self):
        """Create SqliteBackend instance.
        """
        return SqliteBackend(self._filepath)


@pytest.mark.asyncio
class TestAsyncSqliteBackendDirect(_GenericAsyncDirectBackendTestSuite):
    """Async direct API tests for SqliteBackend.
    """

    @pytest.fixture(autouse=True)
    def setup_backend(self):
        """Create temp file for SQLite database.
        """
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            self._filepath = f.name

    def create_backend(self):
        """Create SqliteBackend instance.
        """
        return SqliteBackend(self._filepath)
