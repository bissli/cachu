"""Tests for TTL-based backend isolation.

These tests verify that different TTL values result in separate backends
(and separate database files for file backend), documenting the behavior
that can cause confusion when using get_async_backend() without matching TTL.
"""
import cachu
import pytest
from cachu.decorator import async_manager, clear_backends, get_async_backend
from cachu.decorator import get_backend, manager


@pytest.fixture(autouse=True)
async def clear_async_backends():
    """Clear async backends before and after each test.
    """
    await cachu.clear_async_backends()
    yield
    await cachu.clear_async_backends()


@pytest.fixture(autouse=True)
def clear_sync_backends():
    """Clear sync backends before and after each test.
    """
    clear_backends()
    yield
    clear_backends()


async def test_different_ttl_creates_separate_backends_memory(temp_cache_dir):
    """Verify different TTLs create separate memory backend instances.
    """
    backend_5min = await async_manager.get_backend(None, 'memory', 300)
    backend_24h = await async_manager.get_backend(None, 'memory', 86400)

    assert backend_5min is not backend_24h


async def test_same_ttl_reuses_backend(temp_cache_dir):
    """Verify same TTL reuses the same backend instance.
    """
    backend1 = await async_manager.get_backend(None, 'memory', 300)
    backend2 = await async_manager.get_backend(None, 'memory', 300)

    assert backend1 is backend2


async def test_ttl_to_filename_seconds(temp_cache_dir):
    """Verify TTL < 60s maps to cache{ttl}sec.db filename.
    """
    backend = await async_manager.get_backend(None, 'file', 30)
    assert 'cache30sec.db' in backend._filepath


async def test_ttl_to_filename_minutes(temp_cache_dir):
    """Verify TTL 60-3599s maps to cache{minutes}min.db filename.
    """
    backend = await async_manager.get_backend(None, 'file', 300)
    assert 'cache5min.db' in backend._filepath


async def test_ttl_to_filename_hours(temp_cache_dir):
    """Verify TTL >= 3600s maps to cache{hours}hour.db filename.
    """
    backend = await async_manager.get_backend(None, 'file', 86400)
    assert 'cache24hour.db' in backend._filepath


async def test_different_ttl_uses_different_files(temp_cache_dir):
    """Verify different TTLs create backends with different database files.
    """
    backend_5min = await async_manager.get_backend(None, 'file', 300)
    backend_24h = await async_manager.get_backend(None, 'file', 86400)

    assert backend_5min._filepath != backend_24h._filepath
    assert 'cache5min.db' in backend_5min._filepath
    assert 'cache24hour.db' in backend_24h._filepath


async def test_count_returns_zero_with_wrong_ttl(temp_cache_dir):
    """Document gotcha: count() returns 0 when querying with wrong TTL.

    This test documents the behavior that caused the kynex-proxy bug:
    caching with ttl=86400 but querying count with ttl=300 returns 0
    because they use different database files.
    """
    backend_cache = await async_manager.get_backend(None, 'file', 86400)
    await backend_cache.set('key1', 'value1', 86400)
    await backend_cache.set('key2', 'value2', 86400)

    backend_query = await async_manager.get_backend(None, 'file', 300)
    wrong_ttl_count = await backend_query.count()

    correct_ttl_count = await backend_cache.count()

    assert wrong_ttl_count == 0
    assert correct_ttl_count == 2


async def test_get_async_backend_public_api_with_ttl(temp_cache_dir):
    """Verify public get_async_backend() respects TTL parameter.
    """
    backend1 = await get_async_backend(backend_type='file', ttl=300)
    backend2 = await get_async_backend(backend_type='file', ttl=86400)

    assert backend1 is not backend2
    assert 'cache5min.db' in backend1._filepath
    assert 'cache24hour.db' in backend2._filepath


async def test_async_get_backend_requires_ttl(temp_cache_dir):
    """Verify get_async_backend() requires ttl parameter.
    """
    with pytest.raises(TypeError, match='ttl'):
        await get_async_backend(backend_type='file')


async def test_decorator_and_get_backend_must_match_ttl(temp_cache_dir):
    """Demonstrate correct pattern: decorator TTL must match get_backend TTL.
    """
    call_count = 0

    @cachu.async_cache(ttl=86400, backend='file')
    async def cached_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    await cached_func(5)
    assert call_count == 1

    backend_correct = await get_async_backend(backend_type='file', ttl=86400)
    correct_count = await backend_correct.count()
    assert correct_count >= 1

    backend_wrong = await get_async_backend(backend_type='file', ttl=300)
    wrong_count = await backend_wrong.count()
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


def test_sync_ttl_to_filename_seconds(temp_cache_dir):
    """Verify TTL < 60s maps to cache{ttl}sec.db filename.
    """
    backend = manager.get_backend(None, 'file', 30)
    assert 'cache30sec.db' in backend._filepath


def test_sync_ttl_to_filename_minutes(temp_cache_dir):
    """Verify TTL 60-3599s maps to cache{minutes}min.db filename.
    """
    backend = manager.get_backend(None, 'file', 300)
    assert 'cache5min.db' in backend._filepath


def test_sync_ttl_to_filename_hours(temp_cache_dir):
    """Verify TTL >= 3600s maps to cache{hours}hour.db filename.
    """
    backend = manager.get_backend(None, 'file', 86400)
    assert 'cache24hour.db' in backend._filepath


def test_sync_different_ttl_uses_different_files(temp_cache_dir):
    """Verify different TTLs create backends with different database files.
    """
    backend_5min = manager.get_backend(None, 'file', 300)
    backend_24h = manager.get_backend(None, 'file', 86400)

    assert backend_5min._filepath != backend_24h._filepath
    assert 'cache5min.db' in backend_5min._filepath
    assert 'cache24hour.db' in backend_24h._filepath


def test_sync_count_returns_zero_with_wrong_ttl(temp_cache_dir):
    """Document gotcha: count() returns 0 when querying with wrong TTL.
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
    assert 'cache5min.db' in backend1._filepath
    assert 'cache24hour.db' in backend2._filepath


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
