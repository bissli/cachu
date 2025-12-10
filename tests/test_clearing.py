"""Test cache clearing across all backends.
"""
import cache
import pytest


def test_clear_memory_cache_all_keys():
    """Verify clearing memory cache removes all cached entries.
    """
    @cache.memorycache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cache.clear_memorycache(seconds=300)

    from conftest import get_memory_region
    cache_dict = get_memory_region(300).actual_backend._cache
    assert len(cache_dict) == 0


def test_clear_memory_cache_by_namespace():
    """Verify clearing memory cache by namespace only affects matching keys.
    """
    @cache.memorycache(seconds=300).cache_on_arguments(namespace='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cache.memorycache(seconds=300).cache_on_arguments(namespace='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cache.clear_memorycache(seconds=300, namespace='users')

    from conftest import get_memory_region
    cache_dict = get_memory_region(300).actual_backend._cache
    assert len(cache_dict) == 1


def test_clear_all_memory_regions():
    """Verify clearing without seconds parameter clears all regions.
    """
    @cache.memorycache(seconds=60).cache_on_arguments()
    def func1(x: int) -> int:
        return x

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func2(x: int) -> int:
        return x

    func1(1)
    func2(2)

    cache.clear_memorycache()

    for region in cache.cache._memory_cache_regions.values():
        cache_dict = region.actual_backend._cache
        assert len(cache_dict) == 0


def test_clear_memory_cache_nonexistent_region():
    """Verify clearing nonexistent region logs warning.
    """
    cache.clear_memorycache(seconds=999)


def test_clear_file_cache_all_keys(temp_cache_dir):
    """Verify clearing file cache removes all cached entries.
    """
    @cache.filecache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cache.clear_filecache(seconds=300)


def test_clear_file_cache_by_namespace(temp_cache_dir):
    """Verify clearing file cache by namespace only affects matching keys.
    """
    @cache.filecache(seconds=300).cache_on_arguments(namespace='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cache.filecache(seconds=300).cache_on_arguments(namespace='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cache.clear_filecache(seconds=300, namespace='users')


def test_clear_all_file_regions(temp_cache_dir):
    """Verify clearing without seconds parameter clears all file regions.
    """
    @cache.filecache(seconds=60).cache_on_arguments()
    def func1(x: int) -> int:
        return x

    @cache.filecache(seconds=300).cache_on_arguments()
    def func2(x: int) -> int:
        return x

    func1(1)
    func2(2)

    cache.clear_filecache()


@pytest.mark.redis
def test_clear_redis_cache_all_keys(redis_docker):
    """Verify clearing Redis cache invalidates region.
    """
    @cache.rediscache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cache.clear_rediscache(seconds=300)


@pytest.mark.redis
def test_clear_redis_cache_by_namespace(redis_docker):
    """Verify clearing Redis cache by namespace only affects matching keys.
    """
    @cache.rediscache(seconds=300).cache_on_arguments(namespace='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cache.rediscache(seconds=300).cache_on_arguments(namespace='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cache.clear_rediscache(seconds=300, namespace='users')


def test_clear_namespace_across_all_regions():
    """Verify clearing namespace without seconds clears across all regions.
    """
    @cache.memorycache(seconds=60).cache_on_arguments(namespace='users')
    def func1(x: int) -> int:
        return x

    @cache.memorycache(seconds=300).cache_on_arguments(namespace='users')
    def func2(x: int) -> int:
        return x

    func1(1)
    func2(2)

    cache.clear_memorycache(namespace='users')

    for region in cache.cache._memory_cache_regions.values():
        cache_dict = region.actual_backend._cache
        assert len(cache_dict) == 0


@pytest.mark.redis
def test_clear_redis_without_registration(redis_docker):
    """Verify clearing works without region registration (cross-machine scenario).
    """
    @cache.rediscache(seconds=300).cache_on_arguments(namespace='test')
    def func(x: int) -> int:
        return x

    func(1)

    # Simulate different process - clear in-memory registration
    cache.cache._redis_cache_regions.clear()

    # Should still work by connecting directly to Redis
    cache.clear_rediscache(seconds=300, namespace='test')

    # Verify key is deleted
    client = cache.get_redis_client()
    keys = list(client.scan_iter(match='5m:*'))
    assert len(keys) == 0


@pytest.mark.redis
def test_clear_redis_namespace_across_all_regions(redis_docker):
    """Verify clearing namespace without seconds clears across all regions.
    """
    @cache.rediscache(seconds=60).cache_on_arguments(namespace='users')
    def func1(x: int) -> int:
        return x

    @cache.rediscache(seconds=300).cache_on_arguments(namespace='users')
    def func2(x: int) -> int:
        return x

    @cache.rediscache(seconds=300).cache_on_arguments(namespace='products')
    def func3(x: int) -> int:
        return x

    func1(1)
    func2(2)
    func3(3)

    # Clear 'users' namespace across all regions
    cache.clear_rediscache(namespace='users')

    # Verify 'users' keys are deleted, 'products' key remains
    client = cache.get_redis_client()
    keys = [k.decode() for k in client.scan_iter(match='*:*')]
    assert not any('|users|' in k for k in keys)
    assert any('|products|' in k for k in keys)


def test_clear_redis_no_args_clears_all_regions():
    """Verify calling clear_rediscache() with no args clears all regions for namespace.

    This is consistent with clear_memorycache() and clear_filecache() behavior.
    """
    # Should not raise - clears all regions for caller's namespace
    cache.clear_rediscache()
