"""Test async memory cache backend operations.
"""
import cachu
import pytest


@pytest.fixture(autouse=True)
async def clear_async_backends():
    """Clear async backends before and after each test.
    """
    await cachu.clear_async_backends()
    yield
    await cachu.clear_async_backends()


async def test_async_memory_cache_basic_decoration():
    """Verify async memory cache decorator caches function results.
    """
    call_count = 0

    @cachu.async_cache(ttl=300, backend='memory')
    async def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = await expensive_func(5)
    result2 = await expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


async def test_async_memory_cache_different_args():
    """Verify async memory cache distinguishes between different arguments.
    """
    call_count = 0

    @cachu.async_cache(ttl=300, backend='memory')
    async def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    await func(5)
    await func(10)

    assert call_count == 2


async def test_async_memory_cache_with_tag():
    """Verify tag parameter is accepted and used.
    """
    @cachu.async_cache(ttl=300, backend='memory', tag='users')
    async def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = await get_user(123)
    assert result['id'] == 123


async def test_async_memory_cache_cache_if():
    """Verify cache_if prevents caching of specified values.
    """
    call_count = 0

    @cachu.async_cache(ttl=300, backend='memory', cache_if=lambda r: r is not None)
    async def get_value(x: int) -> int | None:
        nonlocal call_count
        call_count += 1
        return None if x < 0 else x

    result1 = await get_value(-1)
    result2 = await get_value(-1)

    assert result1 is None
    assert result2 is None
    assert call_count == 2


async def test_async_memory_cache_with_kwargs():
    """Verify async memory cache handles keyword arguments correctly.
    """
    call_count = 0

    @cachu.async_cache(ttl=300, backend='memory')
    async def func(x: int, y: int = 10) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    result1 = await func(5, y=10)
    result2 = await func(5, 10)
    result3 = await func(x=5, y=10)

    assert result1 == result2 == result3 == 15
    assert call_count == 1


async def test_async_memory_cache_skip_cache():
    """Verify _skip_cache bypasses the cache.
    """
    call_count = 0

    @cachu.async_cache(ttl=300, backend='memory')
    async def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = await func(5)
    assert call_count == 1

    result2 = await func(5, _skip_cache=True)
    assert call_count == 2
    assert result1 == result2 == 10


async def test_async_memory_cache_overwrite_cache():
    """Verify _overwrite_cache refreshes the cached value.
    """
    counter = [0]

    @cachu.async_cache(ttl=300, backend='memory')
    async def func(x: int) -> int:
        counter[0] += 1
        return x * counter[0]

    result1 = await func(5)
    assert result1 == 5

    result2 = await func(5)
    assert result2 == 5

    result3 = await func(5, _overwrite_cache=True)
    assert result3 == 10

    result4 = await func(5)
    assert result4 == 10


async def test_async_memory_cache_info():
    """Verify async_cache_info returns statistics.
    """
    @cachu.async_cache(ttl=300, backend='memory')
    async def func(x: int) -> int:
        return x * 2

    await func(5)
    await func(5)
    await func(10)
    await func(5)

    info = await cachu.async_cache_info(func)
    assert info.hits == 2
    assert info.misses == 2
