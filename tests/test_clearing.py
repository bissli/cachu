"""Test cache clearing across all backends.
"""
import cache
import pytest


def test_cache_clear_all_keys():
    """Verify cache_clear removes all cached entries.
    """
    @cache.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cache.cache_clear(backend='memory', ttl=300)


def test_cache_clear_by_tag():
    """Verify cache_clear by tag only affects matching keys.
    """
    @cache.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cache.cache(ttl=300, backend='memory', tag='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cache.cache_clear(tag='users', backend='memory', ttl=300)


def test_cache_clear_all_ttls():
    """Verify cache_clear without ttl parameter clears all TTLs.
    """
    @cache.cache(ttl=60, backend='memory')
    def func1(x: int) -> int:
        return x

    @cache.cache(ttl=300, backend='memory')
    def func2(x: int) -> int:
        return x

    func1(1)
    func2(2)

    cache.cache_clear(backend='memory')


def test_cache_clear_file_backend(temp_cache_dir):
    """Verify cache_clear works with file backend.
    """
    @cache.cache(ttl=300, backend='file')
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cache.cache_clear(backend='file', ttl=300)


def test_cache_clear_file_by_tag(temp_cache_dir):
    """Verify cache_clear by tag works with file backend.
    """
    @cache.cache(ttl=300, backend='file', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cache.cache(ttl=300, backend='file', tag='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cache.cache_clear(tag='users', backend='file', ttl=300)


@pytest.mark.redis
def test_cache_clear_redis_backend(redis_docker):
    """Verify cache_clear works with Redis backend.
    """
    @cache.cache(ttl=300, backend='redis')
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cache.cache_clear(backend='redis', ttl=300)


@pytest.mark.redis
def test_cache_clear_redis_by_tag(redis_docker):
    """Verify cache_clear by tag works with Redis backend.
    """
    @cache.cache(ttl=300, backend='redis', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cache.cache(ttl=300, backend='redis', tag='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cache.cache_clear(tag='users', backend='redis', ttl=300)
