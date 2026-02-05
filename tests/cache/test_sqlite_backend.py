"""SQLite-specific backend tests not covered by generic suite.
"""
import tempfile
import time

import pytest
from cachu.api import NO_VALUE
from cachu.backends.sqlite import SqliteBackend


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
