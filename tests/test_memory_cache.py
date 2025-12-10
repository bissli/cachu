"""Test memory cache backend operations.
"""
import cache


def test_memory_cache_basic_decoration():
    """Verify memory cache decorator caches function results.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments()
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

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    func(10)

    assert call_count == 2


def test_memory_cache_with_namespace():
    """Verify namespace parameter is accepted and used.
    """
    @cache.memorycache(seconds=300).cache_on_arguments(namespace='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_memory_cache_should_cache_fn():
    """Verify should_cache_fn prevents caching of falsy values.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments()
    def get_value(x: int) -> int | None:
        nonlocal call_count
        call_count += 1
        return None if x < 0 else x

    result1 = get_value(-1)
    result2 = get_value(-1)

    assert result1 is None
    assert result2 is None
    assert call_count == 2


def test_memory_cache_multiple_regions():
    """Verify multiple expiration times create separate regions.
    """
    @cache.memorycache(seconds=60).cache_on_arguments()
    def func1(x: int) -> int:
        return x * 2

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func2(x: int) -> int:
        return x * 3

    func1(5)
    func2(5)

    from conftest import has_memory_region
    assert has_memory_region(60)
    assert has_memory_region(300)


def test_memory_cache_with_kwargs():
    """Verify memory cache handles keyword arguments correctly.
    """
    call_count = 0

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func(x: int, y: int = 10) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    result1 = func(5, y=10)
    result2 = func(5, 10)
    result3 = func(x=5, y=10)

    assert result1 == result2 == result3 == 15
    assert call_count == 1
