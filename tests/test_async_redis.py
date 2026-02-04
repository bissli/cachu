"""Test async Redis cache backend operations.
"""
import cachu
import pytest
from cachu.backends import NO_VALUE
from cachu.backends.redis import RedisBackend


@pytest.fixture(autouse=True)
async def clear_async_backends():
    """Clear async backends before and after each test.
    """
    await cachu.clear_async_backends()
    yield
    await cachu.clear_async_backends()


@pytest.fixture
async def async_redis_backend(redis_docker):
    """Provide a Redis backend for async testing.
    """
    from fixtures.redis import redis_test_config

    url = f'redis://{redis_test_config.host}:{redis_test_config.port}/0'
    backend = RedisBackend(url)
    yield backend
    await backend.aclose()


@pytest.mark.redis
async def test_async_redis_backend_set_get(async_redis_backend):
    """Verify async Redis backend can set and get values.
    """
    await async_redis_backend.aset('key1', 'value1', 300)
    result = await async_redis_backend.aget('key1')
    assert result == 'value1'


@pytest.mark.redis
async def test_async_redis_backend_get_nonexistent(async_redis_backend):
    """Verify async Redis backend returns NO_VALUE for nonexistent keys.
    """
    result = await async_redis_backend.aget('nonexistent')
    assert result is NO_VALUE


@pytest.mark.redis
async def test_async_redis_backend_get_with_metadata(async_redis_backend):
    """Verify async Redis backend returns value with metadata.
    """
    await async_redis_backend.aset('key1', 'value1', 300)
    value, created_at = await async_redis_backend.aget_with_metadata('key1')
    assert value == 'value1'
    assert created_at is not None


@pytest.mark.redis
async def test_async_redis_backend_delete(async_redis_backend):
    """Verify async Redis backend can delete values.
    """
    await async_redis_backend.aset('key1', 'value1', 300)
    await async_redis_backend.adelete('key1')
    result = await async_redis_backend.aget('key1')
    assert result is NO_VALUE


@pytest.mark.redis
async def test_async_redis_backend_clear(async_redis_backend):
    """Verify async Redis backend can clear all entries.
    """
    await async_redis_backend.aset('key1', 'value1', 300)
    await async_redis_backend.aset('key2', 'value2', 300)

    count = await async_redis_backend.aclear()
    assert count >= 2

    result1 = await async_redis_backend.aget('key1')
    result2 = await async_redis_backend.aget('key2')
    assert result1 is NO_VALUE
    assert result2 is NO_VALUE


@pytest.mark.redis
async def test_async_redis_backend_clear_pattern(async_redis_backend):
    """Verify async Redis backend can clear entries matching pattern.
    """
    await async_redis_backend.aset('user:1', 'value1', 300)
    await async_redis_backend.aset('user:2', 'value2', 300)
    await async_redis_backend.aset('other:1', 'value3', 300)

    count = await async_redis_backend.aclear('user:*')
    assert count == 2

    result1 = await async_redis_backend.aget('user:1')
    result2 = await async_redis_backend.aget('user:2')
    result3 = await async_redis_backend.aget('other:1')

    assert result1 is NO_VALUE
    assert result2 is NO_VALUE
    assert result3 == 'value3'


@pytest.mark.redis
async def test_async_redis_backend_keys(async_redis_backend):
    """Verify async Redis backend can iterate over keys.
    """
    await async_redis_backend.aset('test:key1', 'value1', 300)
    await async_redis_backend.aset('test:key2', 'value2', 300)

    keys = [key async for key in async_redis_backend.akeys('test:*')]

    assert set(keys) == {'test:key1', 'test:key2'}


@pytest.mark.redis
async def test_async_redis_backend_count(async_redis_backend):
    """Verify async Redis backend can count entries.
    """
    await async_redis_backend.aset('count:key1', 'value1', 300)
    await async_redis_backend.aset('count:key2', 'value2', 300)

    count = await async_redis_backend.acount('count:*')
    assert count == 2


@pytest.mark.redis
async def test_async_redis_cache_decorator(redis_docker):
    """Verify async Redis cache decorator works end-to-end.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='redis')
    async def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = await expensive_func(5)
    result2 = await expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1
