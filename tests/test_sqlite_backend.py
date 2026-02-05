"""Test SQLite cache backend operations via inheritance-based test suite.
"""
import tempfile
import time

import pytest
from cachu.api import NO_VALUE
from cachu.backends.sqlite import SqliteBackend
from fixtures.backend_suite import _GenericAsyncDirectBackendTestSuite
from fixtures.backend_suite import _GenericDirectBackendTestSuite


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


class TestSqliteBackendSpecific:
    """SQLite-specific tests not covered by generic suite.
    """

    @pytest.fixture
    def sqlite_backend(self):
        """Provide a SQLite backend for testing.
        """
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            filepath = f.name
        return SqliteBackend(filepath)

    def test_complex_values_roundtrip(self, sqlite_backend):
        """Verify SQLite backend can handle complex values (dicts, lists).
        """
        data = {
            'users': [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
            ],
            'count': 2,
        }
        sqlite_backend.set('complex', data, 300)
        result = sqlite_backend.get('complex')

        assert result == data

    @pytest.mark.slow
    def test_cleanup_expired(self, sqlite_backend):
        """Verify cleanup_expired removes expired entries.
        """
        sqlite_backend.set('short', 'value1', 1)
        sqlite_backend.set('long', 'value2', 300)

        time.sleep(1.5)

        count = sqlite_backend.cleanup_expired()
        assert count == 1

        result1 = sqlite_backend.get('short')
        result2 = sqlite_backend.get('long')

        assert result1 is NO_VALUE
        assert result2 == 'value2'
