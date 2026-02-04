"""Test cache clearing across all backends.
"""
import cachu
import pytest


def test_cache_clear_all_keys():
    """Verify cache_clear removes all cached entries.
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

    func(5)
    func(10)
    assert call_count == 2

    cachu.cache_clear(backend='memory', ttl=300)

    func(5)
    func(10)
    assert call_count == 4


def test_cache_clear_by_tag():
    """Verify cache_clear by tag only affects matching keys.
    """
    user_call_count = 0
    product_call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        nonlocal user_call_count
        user_call_count += 1
        return {'id': user_id}

    @cachu.cache(ttl=300, backend='memory', tag='products')
    def get_product(product_id: int) -> dict:
        nonlocal product_call_count
        product_call_count += 1
        return {'id': product_id}

    get_user(1)
    get_product(1)
    assert user_call_count == 1
    assert product_call_count == 1

    cachu.cache_clear(tag='users', backend='memory', ttl=300)

    get_user(1)
    get_product(1)
    assert user_call_count == 2
    assert product_call_count == 1


def test_cache_clear_all_ttls():
    """Verify cache_clear without ttl parameter clears all TTLs.
    """
    call_count_1 = 0
    call_count_2 = 0

    @cachu.cache(ttl=60, backend='memory')
    def func1(x: int) -> int:
        nonlocal call_count_1
        call_count_1 += 1
        return x

    @cachu.cache(ttl=300, backend='memory')
    def func2(x: int) -> int:
        nonlocal call_count_2
        call_count_2 += 1
        return x

    func1(1)
    func2(2)
    assert call_count_1 == 1
    assert call_count_2 == 1

    cachu.cache_clear(backend='memory')

    func1(1)
    func2(2)
    assert call_count_1 == 2
    assert call_count_2 == 2


def test_cache_clear_file_backend(temp_cache_dir):
    """Verify cache_clear works with file backend.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='file')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    func(10)
    assert call_count == 2

    func(5)
    func(10)
    assert call_count == 2

    cachu.cache_clear(backend='file', ttl=300)

    func(5)
    func(10)
    assert call_count == 4


def test_cache_clear_file_by_tag(temp_cache_dir):
    """Verify cache_clear by tag works with file backend.
    """
    user_call_count = 0
    product_call_count = 0

    @cachu.cache(ttl=300, backend='file', tag='users')
    def get_user(user_id: int) -> dict:
        nonlocal user_call_count
        user_call_count += 1
        return {'id': user_id}

    @cachu.cache(ttl=300, backend='file', tag='products')
    def get_product(product_id: int) -> dict:
        nonlocal product_call_count
        product_call_count += 1
        return {'id': product_id}

    get_user(1)
    get_product(1)
    assert user_call_count == 1
    assert product_call_count == 1

    cachu.cache_clear(tag='users', backend='file', ttl=300)

    get_user(1)
    get_product(1)
    assert user_call_count == 2
    assert product_call_count == 1


@pytest.mark.redis
def test_cache_clear_redis_backend(redis_docker):
    """Verify cache_clear works with Redis backend.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='redis')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    func(10)
    assert call_count == 2

    func(5)
    func(10)
    assert call_count == 2

    cachu.cache_clear(backend='redis', ttl=300)

    func(5)
    func(10)
    assert call_count == 4


@pytest.mark.redis
def test_cache_clear_redis_by_tag(redis_docker):
    """Verify cache_clear by tag works with Redis backend.
    """
    user_call_count = 0
    product_call_count = 0

    @cachu.cache(ttl=300, backend='redis', tag='users')
    def get_user(user_id: int) -> dict:
        nonlocal user_call_count
        user_call_count += 1
        return {'id': user_id}

    @cachu.cache(ttl=300, backend='redis', tag='products')
    def get_product(product_id: int) -> dict:
        nonlocal product_call_count
        product_call_count += 1
        return {'id': product_id}

    get_user(1)
    get_product(1)
    assert user_call_count == 1
    assert product_call_count == 1

    cachu.cache_clear(tag='users', backend='redis', ttl=300)

    get_user(1)
    get_product(1)
    assert user_call_count == 2
    assert product_call_count == 1


def test_cache_clear_without_instantiated_backend():
    """Verify cache_clear creates backend when none exists.

    This tests that cache_clear() properly creates a backend instance when
    both backend and ttl are specified, even if no cached function has been called.

    This is essential for distributed caches (Redis) where cache_clear may be called
    from a different process than the one that populated the cache.
    """
    call_count = 0

    @cachu.cache(ttl=999, backend='memory', tag='test_tag')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x

    func(1)
    assert call_count == 1
    func(1)
    assert call_count == 1

    cachu.cache_clear(backend='memory', ttl=999, tag='test_tag')

    func(1)
    assert call_count == 2


def test_cache_clear_creates_backend_and_clears(temp_cache_dir):
    """Verify cache_clear can clear data in file backend without prior instantiation.

    File backend persists data to disk, allowing us to verify that cache_clear
    can find and delete cached data even when called from a 'fresh' process state.
    """
    from cachu.backends import NO_VALUE
    from cachu.config import _get_caller_package
    from cachu.decorator import clear_backends, manager

    package = _get_caller_package()

    backend = manager.get_backend(package, 'file', 888)
    backend.set('14m:test_func||file_tag||x=1', 'test_value', 888)
    assert backend.get('14m:test_func||file_tag||x=1') == 'test_value'

    clear_backends()

    cachu.cache_clear(backend='file', ttl=888, tag='file_tag')

    backend = manager.get_backend(package, 'file', 888)
    assert backend.get('14m:test_func||file_tag||x=1') is NO_VALUE


def test_cache_clear_resets_stats():
    """Verify cache_clear resets hit/miss statistics.
    """
    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        return x * 2

    func(1)
    func(1)
    func(2)

    info = cachu.cache_info(func)
    assert info.hits == 1
    assert info.misses == 2

    cachu.cache_clear(backend='memory', ttl=300)

    info = cachu.cache_info(func)
    assert info.hits == 0
    assert info.misses == 0


@pytest.mark.asyncio
async def test_async_cache_clear_resets_stats():
    """Verify async_cache_clear resets hit/miss statistics.
    """
    @cachu.cache(ttl=300, backend='memory')
    async def func(x: int) -> int:
        return x * 2

    await func(1)
    await func(1)
    await func(2)

    info = await cachu.async_cache_info(func)
    assert info.hits == 1
    assert info.misses == 2

    await cachu.async_cache_clear(backend='memory', ttl=300)

    info = await cachu.async_cache_info(func)
    assert info.hits == 0
    assert info.misses == 0
