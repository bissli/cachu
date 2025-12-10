"""Test Redis cache backend operations.
"""
import cache
import pytest

pytestmark = pytest.mark.redis


def test_redis_cache_basic_decoration(redis_docker):
    """Verify Redis cache decorator caches function results.
    """
    call_count = 0

    @cache.rediscache(seconds=300).cache_on_arguments()
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


@pytest.mark.parametrize(('seconds', 'expected_name'), [
    (30, '30s'),
    (120, '2m'),
    (7200, '2h'),
    (172800, '2d'),
])
def test_redis_cache_naming_convention(redis_docker, seconds, expected_name):
    """Verify Redis cache uses correct naming convention for regions.
    """
    @cache.rediscache(seconds=seconds).cache_on_arguments()
    def func(x: int) -> int:
        return x

    func(1)

    from conftest import get_redis_region
    region = get_redis_region(seconds)
    assert region.name == expected_name


def test_redis_cache_with_namespace(redis_docker):
    """Verify Redis cache namespace parameter is accepted.
    """
    @cache.rediscache(seconds=300).cache_on_arguments(namespace='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_redis_cache_distributed_lock_configuration(redis_docker):
    """Verify Redis cache respects distributed lock configuration.
    """
    cache.configure(redis_distributed=True)

    @cache.rediscache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        return x * 2

    func(5)

    from conftest import get_redis_region
    region = get_redis_region(300)
    assert hasattr(region.backend, 'distributed_lock')
    assert region.backend.distributed_lock is True
