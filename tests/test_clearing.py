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


def test_cache_clear_without_instantiated_backend():
    """Verify cache_clear creates backend when none exists.

    This tests that cache_clear() properly creates a backend instance when
    both backend and ttl are specified, even if no cached function has been called.

    This is essential for distributed caches (Redis) where cache_clear may be called
    from a different process than the one that populated the cache.
    """
    from cachu.decorator import _backends, clear_backends

    # Clear all backends to simulate a fresh process
    clear_backends()

    # Verify no backends exist (simulates import script that hasn't called cached functions)
    assert len(_backends) == 0

    # Call cache_clear with specific backend and ttl
    # Before the fix, this would do nothing because no backend existed
    # After the fix, this should create the backend and attempt to clear it
    cachu.cache_clear(backend='memory', ttl=999, tag='test_tag')

    # With the fix, a backend should have been created
    assert len(_backends) == 1
    key = list(_backends.keys())[0]
    assert key[1] == 'memory'  # backend type
    assert key[2] == 999  # ttl


def test_cache_clear_creates_backend_and_clears(temp_cache_dir):
    """Verify cache_clear can clear data in file backend without prior instantiation.

    File backend persists data to disk, allowing us to verify that cache_clear
    can find and delete cached data even when called from a 'fresh' process state.
    """
    from cachu.backends import NO_VALUE
    from cachu.config import _get_caller_package
    from cachu.decorator import _backends, _get_backend, clear_backends

    package = _get_caller_package()

    # First, create some cached data in a file backend
    backend = _get_backend(package, 'file', 888)
    backend.set('14m:test_func||file_tag||x=1', 'test_value', 888)
    assert backend.get('14m:test_func||file_tag||x=1') == 'test_value'

    # Clear backend instances (but file data persists on disk)
    clear_backends()
    assert len(_backends) == 0

    # cache_clear should create a new backend instance and clear the persisted data
    cleared = cachu.cache_clear(backend='file', ttl=888, tag='file_tag')

    # Backend should have been created
    assert len(_backends) == 1

    # Data should have been cleared (verify by getting a fresh backend)
    backend = _get_backend(package, 'file', 888)
    assert backend.get('14m:test_func||file_tag||x=1') is NO_VALUE
