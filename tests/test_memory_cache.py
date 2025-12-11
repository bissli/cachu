"""Test memory cache backend operations.
"""
import cachu


def test_memory_cache_basic_decoration():
    """Verify memory cache decorator caches function results.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
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

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    func(5)
    func(10)

    assert call_count == 2


def test_memory_cache_with_tag():
    """Verify tag parameter is accepted and used.
    """
    @cachu.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_memory_cache_cache_if():
    """Verify cache_if prevents caching of specified values.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', cache_if=lambda r: r is not None)
    def get_value(x: int) -> int | None:
        nonlocal call_count
        call_count += 1
        return None if x < 0 else x

    result1 = get_value(-1)
    result2 = get_value(-1)

    assert result1 is None
    assert result2 is None
    assert call_count == 2  # Called twice since None wasn't cached


def test_memory_cache_with_kwargs():
    """Verify memory cache handles keyword arguments correctly.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int, y: int = 10) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    result1 = func(5, y=10)
    result2 = func(5, 10)
    result3 = func(x=5, y=10)

    assert result1 == result2 == result3 == 15
    assert call_count == 1


def test_memory_cache_skip_cache():
    """Verify _skip_cache bypasses the cachu.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = func(5)
    assert call_count == 1

    # This should skip cache and call the function
    result2 = func(5, _skip_cache=True)
    assert call_count == 2
    assert result1 == result2 == 10


def test_memory_cache_overwrite_cache():
    """Verify _overwrite_cache refreshes the cached value.
    """
    counter = [0]

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        counter[0] += 1
        return x * counter[0]

    result1 = func(5)
    assert result1 == 5  # 5 * 1

    result2 = func(5)
    assert result2 == 5  # Cached

    result3 = func(5, _overwrite_cache=True)
    assert result3 == 10  # 5 * 2, and overwrites cache

    result4 = func(5)
    assert result4 == 10  # Returns new cached value


def test_memory_cache_info():
    """Verify cache_info returns statistics.
    """
    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        return x * 2

    func(5)  # miss
    func(5)  # hit
    func(10)  # miss
    func(5)  # hit

    info = cachu.cache_info(func)
    assert info.hits == 2
    assert info.misses == 2
