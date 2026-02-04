"""Test async SQLite cache backend operations.
"""
import tempfile

import cachu
import pytest
from cachu.backends import NO_VALUE
from cachu.backends.sqlite import SqliteBackend


@pytest.fixture(autouse=True)
async def clear_async_backends():
    """Clear async backends before and after each test.
    """
    await cachu.clear_async_backends()
    yield
    await cachu.clear_async_backends()


@pytest.fixture
async def async_sqlite_backend():
    """Provide a SQLite backend for async testing.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        filepath = f.name

    backend = SqliteBackend(filepath)
    yield backend
    await backend.aclose()


async def test_async_sqlite_backend_set_get(async_sqlite_backend):
    """Verify async SQLite backend can set and get values.
    """
    await async_sqlite_backend.aset('key1', 'value1', 300)
    result = await async_sqlite_backend.aget('key1')
    assert result == 'value1'


async def test_async_sqlite_backend_get_nonexistent(async_sqlite_backend):
    """Verify async SQLite backend returns NO_VALUE for nonexistent keys.
    """
    result = await async_sqlite_backend.aget('nonexistent')
    assert result is NO_VALUE


async def test_async_sqlite_backend_get_with_metadata(async_sqlite_backend):
    """Verify async SQLite backend returns value with metadata.
    """
    await async_sqlite_backend.aset('key1', 'value1', 300)
    value, created_at = await async_sqlite_backend.aget_with_metadata('key1')
    assert value == 'value1'
    assert created_at is not None


async def test_async_sqlite_backend_delete(async_sqlite_backend):
    """Verify async SQLite backend can delete values.
    """
    await async_sqlite_backend.aset('key1', 'value1', 300)
    await async_sqlite_backend.adelete('key1')
    result = await async_sqlite_backend.aget('key1')
    assert result is NO_VALUE


async def test_async_sqlite_backend_clear(async_sqlite_backend):
    """Verify async SQLite backend can clear all entries.
    """
    await async_sqlite_backend.aset('key1', 'value1', 300)
    await async_sqlite_backend.aset('key2', 'value2', 300)

    count = await async_sqlite_backend.aclear()
    assert count == 2

    result1 = await async_sqlite_backend.aget('key1')
    result2 = await async_sqlite_backend.aget('key2')
    assert result1 is NO_VALUE
    assert result2 is NO_VALUE


async def test_async_sqlite_backend_clear_pattern(async_sqlite_backend):
    """Verify async SQLite backend can clear entries matching pattern.
    """
    await async_sqlite_backend.aset('user:1', 'value1', 300)
    await async_sqlite_backend.aset('user:2', 'value2', 300)
    await async_sqlite_backend.aset('other:1', 'value3', 300)

    count = await async_sqlite_backend.aclear('user:*')
    assert count == 2

    result1 = await async_sqlite_backend.aget('user:1')
    result2 = await async_sqlite_backend.aget('user:2')
    result3 = await async_sqlite_backend.aget('other:1')

    assert result1 is NO_VALUE
    assert result2 is NO_VALUE
    assert result3 == 'value3'


async def test_async_sqlite_backend_keys(async_sqlite_backend):
    """Verify async SQLite backend can iterate over keys.
    """
    await async_sqlite_backend.aset('key1', 'value1', 300)
    await async_sqlite_backend.aset('key2', 'value2', 300)

    keys = [key async for key in async_sqlite_backend.akeys()]

    assert set(keys) == {'key1', 'key2'}


async def test_async_sqlite_backend_keys_pattern(async_sqlite_backend):
    """Verify async SQLite backend can filter keys by pattern.
    """
    await async_sqlite_backend.aset('user:1', 'value1', 300)
    await async_sqlite_backend.aset('user:2', 'value2', 300)
    await async_sqlite_backend.aset('other:1', 'value3', 300)

    keys = [key async for key in async_sqlite_backend.akeys('user:*')]

    assert set(keys) == {'user:1', 'user:2'}


async def test_async_sqlite_backend_count(async_sqlite_backend):
    """Verify async SQLite backend can count entries.
    """
    await async_sqlite_backend.aset('key1', 'value1', 300)
    await async_sqlite_backend.aset('key2', 'value2', 300)

    count = await async_sqlite_backend.acount()
    assert count == 2


async def test_async_sqlite_backend_count_pattern(async_sqlite_backend):
    """Verify async SQLite backend can count entries matching pattern.
    """
    await async_sqlite_backend.aset('user:1', 'value1', 300)
    await async_sqlite_backend.aset('user:2', 'value2', 300)
    await async_sqlite_backend.aset('other:1', 'value3', 300)

    count = await async_sqlite_backend.acount('user:*')
    assert count == 2


async def test_async_file_cache_decorator(temp_cache_dir):
    """Verify async file cache decorator works end-to-end.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='file')
    async def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = await expensive_func(5)
    result2 = await expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1
