"""Tests for TTL-based backend isolation.

These tests verify that different TTL values result in separate backends,
ensuring cache isolation and independent clearing behavior.
"""
import cachu
import pytest
from cachu.decorator import get_async_backend, get_backend, manager


async def test_different_ttl_creates_separate_backends_memory(temp_cache_dir):
    """Verify different TTLs create separate memory backend instances.
    """
    backend_5min = await manager.aget_backend(None, 'memory', 300)
    backend_24h = await manager.aget_backend(None, 'memory', 86400)

    assert backend_5min is not backend_24h


async def test_same_ttl_reuses_backend(temp_cache_dir):
    """Verify same TTL reuses the same backend instance.
    """
    backend1 = await manager.aget_backend(None, 'memory', 300)
    backend2 = await manager.aget_backend(None, 'memory', 300)

    assert backend1 is backend2


async def test_different_ttl_regions_are_isolated(temp_cache_dir):
    """Verify different TTLs store data in separate regions.
    """
    call_count_5min = 0
    call_count_24h = 0

    @cachu.cache(ttl=300, backend='file')
    async def func_5min(x: int) -> int:
        nonlocal call_count_5min
        call_count_5min += 1
        return x * 2

    @cachu.cache(ttl=86400, backend='file')
    async def func_24h(x: int) -> int:
        nonlocal call_count_24h
        call_count_24h += 1
        return x * 3

    await func_5min(1)
    await func_24h(1)
    assert call_count_5min == 1
    assert call_count_24h == 1

    await cachu.async_cache_clear(backend='file', ttl=300)

    await func_5min(1)
    await func_24h(1)
    assert call_count_5min == 2
    assert call_count_24h == 1


async def test_backend_with_different_ttl_is_isolated(temp_cache_dir):
    """Verify count() returns 0 when querying with different TTL.

    This documents the TTL isolation behavior: caching with ttl=86400 but
    querying count with ttl=300 returns 0 because they use different regions.
    """
    backend_cache = await manager.aget_backend(None, 'file', 86400)
    await backend_cache.aset('key1', 'value1', 86400)
    await backend_cache.aset('key2', 'value2', 86400)

    backend_query = await manager.aget_backend(None, 'file', 300)
    wrong_ttl_count = await backend_query.acount()

    correct_ttl_count = await backend_cache.acount()

    assert wrong_ttl_count == 0
    assert correct_ttl_count == 2


async def test_get_async_backend_public_api_with_ttl(temp_cache_dir):
    """Verify public aget_backend() respects TTL parameter.
    """
    backend1 = await get_async_backend(backend_type='file', ttl=300)
    backend2 = await get_async_backend(backend_type='file', ttl=86400)

    assert backend1 is not backend2

    await backend1.aset('test_key', 'value1', 300)
    count1 = await backend1.acount()
    count2 = await backend2.acount()

    assert count1 == 1
    assert count2 == 0


async def test_async_get_backend_requires_ttl(temp_cache_dir):
    """Verify aget_backend() requires ttl parameter.
    """
    with pytest.raises(TypeError, match='ttl'):
        await get_async_backend(backend_type='file')


async def test_decorator_and_get_backend_must_match_ttl(temp_cache_dir):
    """Demonstrate correct pattern: decorator TTL must match get_backend TTL.
    """
    call_count = 0

    @cachu.cache(ttl=86400, backend='file')
    async def cached_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    await cached_func(5)
    assert call_count == 1

    backend_correct = await get_async_backend(backend_type='file', ttl=86400)
    correct_count = await backend_correct.acount()
    assert correct_count >= 1

    backend_wrong = await get_async_backend(backend_type='file', ttl=300)
    wrong_count = await backend_wrong.acount()
    assert wrong_count == 0


def test_sync_different_ttl_creates_separate_backends_memory(temp_cache_dir):
    """Verify different TTLs create separate memory backend instances.
    """
    backend_5min = manager.get_backend(None, 'memory', 300)
    backend_24h = manager.get_backend(None, 'memory', 86400)

    assert backend_5min is not backend_24h


def test_sync_same_ttl_reuses_backend(temp_cache_dir):
    """Verify same TTL reuses the same backend instance.
    """
    backend1 = manager.get_backend(None, 'memory', 300)
    backend2 = manager.get_backend(None, 'memory', 300)

    assert backend1 is backend2


def test_sync_different_ttl_regions_are_isolated(temp_cache_dir):
    """Verify different TTLs store data in separate regions.
    """
    call_count_5min = 0
    call_count_24h = 0

    @cachu.cache(ttl=300, backend='file')
    def func_5min(x: int) -> int:
        nonlocal call_count_5min
        call_count_5min += 1
        return x * 2

    @cachu.cache(ttl=86400, backend='file')
    def func_24h(x: int) -> int:
        nonlocal call_count_24h
        call_count_24h += 1
        return x * 3

    func_5min(1)
    func_24h(1)
    assert call_count_5min == 1
    assert call_count_24h == 1

    cachu.cache_clear(backend='file', ttl=300)

    func_5min(1)
    func_24h(1)
    assert call_count_5min == 2
    assert call_count_24h == 1


def test_sync_backend_with_different_ttl_is_isolated(temp_cache_dir):
    """Verify count() returns 0 when querying with different TTL.
    """
    backend_cache = manager.get_backend(None, 'file', 86400)
    backend_cache.set('key1', 'value1', 86400)
    backend_cache.set('key2', 'value2', 86400)

    backend_query = manager.get_backend(None, 'file', 300)
    wrong_ttl_count = backend_query.count()

    correct_ttl_count = backend_cache.count()

    assert wrong_ttl_count == 0
    assert correct_ttl_count == 2


def test_sync_get_backend_public_api_with_ttl(temp_cache_dir):
    """Verify public get_backend() respects TTL parameter.
    """
    backend1 = get_backend(backend_type='file', ttl=300)
    backend2 = get_backend(backend_type='file', ttl=86400)

    assert backend1 is not backend2

    backend1.set('test_key', 'value1', 300)
    count1 = backend1.count()
    count2 = backend2.count()

    assert count1 == 1
    assert count2 == 0


def test_sync_get_backend_requires_ttl(temp_cache_dir):
    """Verify get_backend() requires ttl parameter.
    """
    with pytest.raises(TypeError, match='ttl'):
        get_backend(backend_type='file')


def test_sync_decorator_and_get_backend_must_match_ttl(temp_cache_dir):
    """Demonstrate correct pattern: decorator TTL must match get_backend TTL.
    """
    call_count = 0

    @cachu.cache(ttl=86400, backend='file')
    def cached_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    cached_func(5)
    assert call_count == 1

    backend_correct = get_backend(backend_type='file', ttl=86400)
    correct_count = backend_correct.count()
    assert correct_count >= 1

    backend_wrong = get_backend(backend_type='file', ttl=300)
    wrong_count = backend_wrong.count()
    assert wrong_count == 0
