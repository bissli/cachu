"""Test Redis cache backend operations.
"""
import cachu
import pytest

pytestmark = pytest.mark.redis


def test_redis_cache_basic_decoration(redis_docker):
    """Verify Redis cache decorator caches function results.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='redis')
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_redis_cache_different_args(redis_docker):
    """Verify Redis cache distinguishes between different arguments.
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


def test_redis_cache_with_tag(redis_docker):
    """Verify Redis cache tag parameter is accepted.
    """
    @cachu.cache(ttl=300, backend='redis', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_redis_cache_with_kwargs(redis_docker):
    """Verify Redis cache handles keyword arguments correctly.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='redis')
    def func(x: int, y: int = 10) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    result1 = func(5, y=10)
    result2 = func(5, 10)
    result3 = func(x=5, y=10)

    assert result1 == result2 == result3 == 15
    assert call_count == 1


def test_redis_cache_complex_objects(redis_docker):
    """Verify Redis cache can store complex objects.
    """
    @cachu.cache(ttl=300, backend='redis')
    def get_data() -> dict:
        return {
            'users': [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
            'metadata': {'version': '1.0', 'count': 2},
        }

    result1 = get_data()
    result2 = get_data()

    assert result1 == result2
    assert len(result1['users']) == 2
