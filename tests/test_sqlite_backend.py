"""Test SQLite cache backend operations.
"""
import tempfile

import pytest

from cachu.backends.sqlite import SqliteBackend
from cachu.backends import NO_VALUE


@pytest.fixture
def sqlite_backend():
    """Provide a SQLite backend for testing.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        filepath = f.name

    return SqliteBackend(filepath)


def test_sqlite_backend_set_get(sqlite_backend):
    """Verify SQLite backend can set and get values.
    """
    sqlite_backend.set('key1', 'value1', 300)
    result = sqlite_backend.get('key1')
    assert result == 'value1'


def test_sqlite_backend_get_nonexistent(sqlite_backend):
    """Verify SQLite backend returns NO_VALUE for nonexistent keys.
    """
    result = sqlite_backend.get('nonexistent')
    assert result is NO_VALUE


def test_sqlite_backend_get_with_metadata(sqlite_backend):
    """Verify SQLite backend returns value with metadata.
    """
    sqlite_backend.set('key1', 'value1', 300)
    value, created_at = sqlite_backend.get_with_metadata('key1')
    assert value == 'value1'
    assert created_at is not None


def test_sqlite_backend_delete(sqlite_backend):
    """Verify SQLite backend can delete values.
    """
    sqlite_backend.set('key1', 'value1', 300)
    sqlite_backend.delete('key1')
    result = sqlite_backend.get('key1')
    assert result is NO_VALUE


def test_sqlite_backend_clear(sqlite_backend):
    """Verify SQLite backend can clear all entries.
    """
    sqlite_backend.set('key1', 'value1', 300)
    sqlite_backend.set('key2', 'value2', 300)

    count = sqlite_backend.clear()
    assert count == 2

    result1 = sqlite_backend.get('key1')
    result2 = sqlite_backend.get('key2')
    assert result1 is NO_VALUE
    assert result2 is NO_VALUE


def test_sqlite_backend_clear_pattern(sqlite_backend):
    """Verify SQLite backend can clear entries matching pattern.
    """
    sqlite_backend.set('user:1', 'value1', 300)
    sqlite_backend.set('user:2', 'value2', 300)
    sqlite_backend.set('other:1', 'value3', 300)

    count = sqlite_backend.clear('user:*')
    assert count == 2

    result1 = sqlite_backend.get('user:1')
    result2 = sqlite_backend.get('user:2')
    result3 = sqlite_backend.get('other:1')

    assert result1 is NO_VALUE
    assert result2 is NO_VALUE
    assert result3 == 'value3'


def test_sqlite_backend_keys(sqlite_backend):
    """Verify SQLite backend can iterate over keys.
    """
    sqlite_backend.set('key1', 'value1', 300)
    sqlite_backend.set('key2', 'value2', 300)

    keys = list(sqlite_backend.keys())
    assert set(keys) == {'key1', 'key2'}


def test_sqlite_backend_keys_pattern(sqlite_backend):
    """Verify SQLite backend can filter keys by pattern.
    """
    sqlite_backend.set('user:1', 'value1', 300)
    sqlite_backend.set('user:2', 'value2', 300)
    sqlite_backend.set('other:1', 'value3', 300)

    keys = list(sqlite_backend.keys('user:*'))
    assert set(keys) == {'user:1', 'user:2'}


def test_sqlite_backend_count(sqlite_backend):
    """Verify SQLite backend can count entries.
    """
    sqlite_backend.set('key1', 'value1', 300)
    sqlite_backend.set('key2', 'value2', 300)

    count = sqlite_backend.count()
    assert count == 2


def test_sqlite_backend_count_pattern(sqlite_backend):
    """Verify SQLite backend can count entries matching pattern.
    """
    sqlite_backend.set('user:1', 'value1', 300)
    sqlite_backend.set('user:2', 'value2', 300)
    sqlite_backend.set('other:1', 'value3', 300)

    count = sqlite_backend.count('user:*')
    assert count == 2


def test_sqlite_backend_complex_values(sqlite_backend):
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


def test_sqlite_backend_cleanup_expired(sqlite_backend):
    """Verify cleanup_expired removes expired entries.
    """
    import time

    sqlite_backend.set('short', 'value1', 1)
    sqlite_backend.set('long', 'value2', 300)

    time.sleep(1.5)

    count = sqlite_backend.cleanup_expired()
    assert count == 1

    result1 = sqlite_backend.get('short')
    result2 = sqlite_backend.get('long')

    assert result1 is NO_VALUE
    assert result2 == 'value2'
