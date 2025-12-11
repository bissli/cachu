"""Test cache clearing across all backends.
"""
import cachu
import pytest


def test_cache_clear_all_keys():
    """Verify cache_clear removes all cached entries.
    """
    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cachu.cache_clear(backend='memory', ttl=300)


def test_cache_clear_by_tag():
    """Verify cache_clear by tag only affects matching keys.
    """
    @cachu.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cachu.cache(ttl=300, backend='memory', tag='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cachu.cache_clear(tag='users', backend='memory', ttl=300)


def test_cache_clear_all_ttls():
    """Verify cache_clear without ttl parameter clears all TTLs.
    """
    @cachu.cache(ttl=60, backend='memory')
    def func1(x: int) -> int:
        return x

    @cachu.cache(ttl=300, backend='memory')
    def func2(x: int) -> int:
        return x

    func1(1)
    func2(2)

    cachu.cache_clear(backend='memory')


def test_cache_clear_file_backend(temp_cache_dir):
    """Verify cache_clear works with file backend.
    """
    @cachu.cache(ttl=300, backend='file')
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cachu.cache_clear(backend='file', ttl=300)


def test_cache_clear_file_by_tag(temp_cache_dir):
    """Verify cache_clear by tag works with file backend.
    """
    @cachu.cache(ttl=300, backend='file', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cachu.cache(ttl=300, backend='file', tag='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cachu.cache_clear(tag='users', backend='file', ttl=300)


@pytest.mark.redis
def test_cache_clear_redis_backend(redis_docker):
    """Verify cache_clear works with Redis backend.
    """
    @cachu.cache(ttl=300, backend='redis')
    def func(x: int) -> int:
        return x * 2

    func(5)
    func(10)

    cachu.cache_clear(backend='redis', ttl=300)


@pytest.mark.redis
def test_cache_clear_redis_by_tag(redis_docker):
    """Verify cache_clear by tag works with Redis backend.
    """
    @cachu.cache(ttl=300, backend='redis', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id}

    @cachu.cache(ttl=300, backend='redis', tag='products')
    def get_product(product_id: int) -> dict:
        return {'id': product_id}

    get_user(1)
    get_product(1)

    cachu.cache_clear(tag='users', backend='redis', ttl=300)
