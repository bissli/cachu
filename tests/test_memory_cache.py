"""Test memory cache backend operations.
"""
import time

import cachu
import pytest


def test_memory_cache_basic_decoration():
    """Verify memory cache decorator caches function results.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_memory_cache_different_args():
    """Verify memory cache distinguishes between different arguments.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    func(10)

    assert call_count == 2


def test_memory_cache_with_tag():
    """Verify tag parameter is accepted and used.
    """
    @cachu.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_memory_cache_cache_if():
    """Verify cache_if prevents caching of specified values.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', cache_if=lambda r: r is not None)
    def get_value(x: int) -> int | None:
        nonlocal call_count
        call_count += 1
        return None if x < 0 else x

    result1 = get_value(-1)
    result2 = get_value(-1)

    assert result1 is None
    assert result2 is None
    assert call_count == 2  # Called twice since None wasn't cached


def test_memory_cache_with_kwargs():
    """Verify memory cache handles keyword arguments correctly.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int, y: int = 10) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    result1 = func(5, y=10)
    result2 = func(5, 10)
    result3 = func(x=5, y=10)

    assert result1 == result2 == result3 == 15
    assert call_count == 1


def test_memory_cache_skip_cache():
    """Verify _skip_cache bypasses the cachu.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = func(5)
    assert call_count == 1

    # This should skip cache and call the function
    result2 = func(5, _skip_cache=True)
    assert call_count == 2
    assert result1 == result2 == 10


def test_memory_cache_overwrite_cache():
    """Verify _overwrite_cache refreshes the cached value.
    """
    counter = [0]

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        counter[0] += 1
        return x * counter[0]

    result1 = func(5)
    assert result1 == 5  # 5 * 1

    result2 = func(5)
    assert result2 == 5  # Cached

    result3 = func(5, _overwrite_cache=True)
    assert result3 == 10  # 5 * 2, and overwrites cache

    result4 = func(5)
    assert result4 == 10  # Returns new cached value


def test_memory_cache_info():
    """Verify cache_info returns statistics.
    """
    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        return x * 2

    func(5)  # miss
    func(5)  # hit
    func(10)  # miss
    func(5)  # hit

    info = cachu.cache_info(func)
    assert info.hits == 2
    assert info.misses == 2


@pytest.mark.slow
def test_memory_cache_ttl_expiration():
    """Verify cached values expire after TTL.
    """
    call_count = 0

    @cachu.cache(ttl=1, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    assert call_count == 1
    func(5)
    assert call_count == 1

    time.sleep(1.5)

    func(5)
    assert call_count == 2


@pytest.mark.slow
def test_memory_cache_ttl_boundary_value_still_valid():
    """Verify cached value is still valid just before TTL expires.

    A value cached with TTL=1s should still be valid at 0.9s.
    """
    call_count = 0

    @cachu.cache(ttl=1, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    assert call_count == 1

    time.sleep(0.5)

    result = func(5)
    assert result == 10
    assert call_count == 1


@pytest.mark.slow
def test_memory_cache_ttl_just_expired():
    """Verify cached value expires just after TTL boundary.

    A value cached with TTL=1s should be expired at 1.1s.
    """
    call_count = 0

    @cachu.cache(ttl=1, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    assert call_count == 1

    time.sleep(1.1)

    func(5)
    assert call_count == 2


def test_memory_cache_zero_ttl_recomputes_every_call():
    """Verify TTL=0 causes immediate expiration (no caching).

    Per dogpile.cache behavior: TTL=0 means the value expires immediately,
    so every access recomputes. This is useful for testing or disabling
    caching on a per-function basis.
    """
    call_count = 0

    @cachu.cache(ttl=0, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    func(5)
    assert call_count == 2, "TTL=0 should cause recomputation on every call"


def test_memory_backend_stats_direct():
    """Verify MemoryBackend stats methods work directly.
    """
    from cachu.backends.memory import MemoryBackend

    backend = MemoryBackend()

    backend.incr_stat('test_func', 'hits')
    backend.incr_stat('test_func', 'hits')
    backend.incr_stat('test_func', 'misses')

    hits, misses = backend.get_stats('test_func')
    assert hits == 2
    assert misses == 1

    backend.clear_stats('test_func')
    assert backend.get_stats('test_func') == (0, 0)


def test_memory_backend_stats_unknown_function():
    """Verify MemoryBackend returns (0, 0) for unknown functions.
    """
    from cachu.backends.memory import MemoryBackend

    backend = MemoryBackend()
    assert backend.get_stats('unknown_func') == (0, 0)


def test_memory_backend_clear_all_stats():
    """Verify MemoryBackend can clear all stats.
    """
    from cachu.backends.memory import MemoryBackend

    backend = MemoryBackend()
    backend.incr_stat('func1', 'hits')
    backend.incr_stat('func2', 'misses')

    backend.clear_stats()

    assert backend.get_stats('func1') == (0, 0)
    assert backend.get_stats('func2') == (0, 0)


@pytest.mark.asyncio
async def test_memory_backend_async_stats():
    """Verify MemoryBackend async stats methods work.
    """
    from cachu.backends.memory import MemoryBackend

    backend = MemoryBackend()

    await backend.aincr_stat('async_func', 'hits')
    await backend.aincr_stat('async_func', 'misses')
    await backend.aincr_stat('async_func', 'misses')

    hits, misses = await backend.aget_stats('async_func')
    assert hits == 1
    assert misses == 2

    await backend.aclear_stats('async_func')
    assert await backend.aget_stats('async_func') == (0, 0)
