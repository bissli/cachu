"""Test defaultcache routing functionality.
"""
import cache
import pytest


def test_defaultcache_routes_to_memory_by_default():
    """Verify defaultcache routes to memory cache by default.
    """
    call_count = 0

    @cache.defaultcache(seconds=300).cache_on_arguments()
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_defaultcache_routes_to_file(temp_cache_dir):
    """Verify defaultcache routes to file cache when configured.
    """
    cache.configure(default_backend='file')
    call_count = 0

    @cache.defaultcache(seconds=300).cache_on_arguments()
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_defaultcache_invalid_backend():
    """Verify defaultcache raises error for invalid backend.
    """
    cache.configure(default_backend='invalid')

    with pytest.raises(ValueError) as exc_info:
        cache.defaultcache(seconds=300)

    assert 'Unknown default_backend' in str(exc_info.value)


def test_defaultcache_with_namespace():
    """Verify defaultcache works with namespace.
    """
    @cache.defaultcache(seconds=300).cache_on_arguments(namespace='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_clear_defaultcache():
    """Verify clear_defaultcache clears the underlying backend.
    """
    call_count = 0

    @cache.defaultcache(seconds=300).cache_on_arguments()
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    expensive_func(5)
    assert call_count == 1

    cache.clear_defaultcache(seconds=300)

    expensive_func(5)
    assert call_count == 2


def test_set_defaultcache_key():
    """Verify set_defaultcache_key sets value in underlying backend.
    """
    @cache.defaultcache(seconds=300).cache_on_arguments(namespace='test')
    def get_value(key: str) -> str:
        return f'computed_{key}'

    cache.set_defaultcache_key(300, 'test', get_value, 'preset_value', key='mykey')

    result = get_value('mykey')
    assert result == 'preset_value'


def test_delete_defaultcache_key():
    """Verify delete_defaultcache_key removes value from underlying backend.
    """
    call_count = 0

    @cache.defaultcache(seconds=300).cache_on_arguments(namespace='test')
    def get_value(key: str) -> str:
        nonlocal call_count
        call_count += 1
        return f'computed_{key}'

    get_value('mykey')
    assert call_count == 1

    cache.delete_defaultcache_key(300, 'test', get_value, key='mykey')

    get_value('mykey')
    assert call_count == 2


def test_defaultcache_backend_switching():
    """Verify defaultcache correctly switches between backends.
    """
    cache.configure(default_backend='memory')
    assert cache.config.default_backend == 'memory'

    memory_region = cache.defaultcache(seconds=60)
    assert 60 in cache.cache._memory_cache_regions

    cache.configure(default_backend='file', tmpdir='/tmp')
    assert cache.config.default_backend == 'file'

    file_region = cache.defaultcache(seconds=120)
    assert 120 in cache.cache._file_cache_regions
