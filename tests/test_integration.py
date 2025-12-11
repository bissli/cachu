"""Integration tests for cache decorator usage.
"""
import cachu


def test_instance_method_caching():
    """Verify cache works correctly with instance methods.
    """
    class Repository:
        def __init__(self, db_conn):
            self.conn = db_conn
            self.call_count = 0

        @cachu.cache(ttl=300, backend='memory')
        def get_data(self, user_id: int) -> dict:
            self.call_count += 1
            return {'id': user_id, 'data': 'test'}

    repo1 = Repository('conn1')
    repo2 = Repository('conn2')

    result1 = repo1.get_data(123)
    result2 = repo2.get_data(123)

    assert result1 == result2
    assert repo1.call_count == 1
    assert repo2.call_count == 0


def test_class_method_caching():
    """Verify cache works correctly with class methods.
    """
    class Calculator:
        call_count = 0

        @classmethod
        @cachu.cache(ttl=300, backend='memory')
        def compute(cls, x: int) -> int:
            cls.call_count += 1
            return x * 2

    result1 = Calculator.compute(5)
    result2 = Calculator.compute(5)

    assert result1 == result2 == 10
    assert Calculator.call_count == 1


def test_static_method_caching():
    """Verify cache works correctly with static methods.
    """
    call_count = 0

    class Utils:
        @staticmethod
        @cachu.cache(ttl=300, backend='memory')
        def calculate(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

    result1 = Utils.calculate(5)
    result2 = Utils.calculate(5)

    assert result1 == result2 == 10
    assert call_count == 1


def test_multiple_decorators_same_backend():
    """Verify multiple functions can share same cache backend.
    """
    call_count_1 = 0
    call_count_2 = 0

    @cachu.cache(ttl=300, backend='memory')
    def func1(x: int) -> int:
        nonlocal call_count_1
        call_count_1 += 1
        return x * 2

    @cachu.cache(ttl=300, backend='memory')
    def func2(x: int) -> int:
        nonlocal call_count_2
        call_count_2 += 1
        return x * 3

    func1(5)
    func1(5)
    func2(5)
    func2(5)

    assert call_count_1 == 1
    assert call_count_2 == 1


def test_tag_isolation():
    """Verify tags properly isolate cached values.
    """
    @cachu.cache(ttl=300, backend='memory', tag='ns1')
    def func_ns1(x: int) -> int:
        return x * 2

    @cachu.cache(ttl=300, backend='memory', tag='ns2')
    def func_ns2(x: int) -> int:
        return x * 3

    result1 = func_ns1(5)
    result2 = func_ns2(5)

    assert result1 == 10
    assert result2 == 15


def test_varargs_caching():
    """Verify cache handles variable positional arguments.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(*args) -> int:
        nonlocal call_count
        call_count += 1
        return sum(args)

    result1 = func(1, 2, 3)
    result2 = func(1, 2, 3)
    result3 = func(1, 2, 3, 4)

    assert result1 == result2 == 6
    assert result3 == 10
    assert call_count == 2


def test_kwargs_caching():
    """Verify cache handles keyword arguments.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(**kwargs) -> dict:
        nonlocal call_count
        call_count += 1
        return kwargs

    result1 = func(a=1, b=2)
    result2 = func(a=1, b=2)
    result3 = func(b=2, a=1)

    assert result1 == result2 == result3 == {'a': 1, 'b': 2}
    assert call_count == 1


def test_mixed_backends(temp_cache_dir):
    """Verify different backends can be used for different functions.
    """
    @cachu.cache(ttl=60, backend='memory')
    def memory_func(x: int) -> int:
        return x * 2

    @cachu.cache(ttl=300, backend='file')
    def file_func(x: int) -> int:
        return x * 3

    result1 = memory_func(5)
    result2 = file_func(5)

    assert result1 == 10
    assert result2 == 15


def test_cache_if_callback():
    """Verify cache_if callback controls caching.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', cache_if=lambda r: r is not None)
    def find_item(item_id: int) -> dict | None:
        nonlocal call_count
        call_count += 1
        if item_id > 0:
            return {'id': item_id}
        return None

    result1 = find_item(1)
    result2 = find_item(1)
    assert result1 == {'id': 1}
    assert call_count == 1

    result3 = find_item(-1)
    result4 = find_item(-1)
    assert result3 is None
    assert result4 is None
    assert call_count == 3


def test_validate_callback():
    """Verify validate callback controls cache validity.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', validate=lambda e: e.value.get('version') == 2)
    def get_config() -> dict:
        nonlocal call_count
        call_count += 1
        return {'version': call_count, 'data': 'test'}

    result1 = get_config()
    assert result1 == {'version': 1, 'data': 'test'}
    assert call_count == 1

    result2 = get_config()
    assert result2 == {'version': 2, 'data': 'test'}
    assert call_count == 2

    result3 = get_config()
    assert result3 == {'version': 2, 'data': 'test'}
    assert call_count == 2


def test_skip_cache_kwarg():
    """Verify _skip_cache kwarg bypasses cachu.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = func(5)
    assert call_count == 1

    result2 = func(5)
    assert call_count == 1

    result3 = func(5, _skip_cache=True)
    assert call_count == 2

    result4 = func(5)
    assert call_count == 2


def test_overwrite_cache_kwarg():
    """Verify _overwrite_cache kwarg refreshes cachu.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * call_count

    result1 = func(5)
    assert result1 == 5
    assert call_count == 1

    result2 = func(5)
    assert result2 == 5
    assert call_count == 1

    result3 = func(5, _overwrite_cache=True)
    assert result3 == 10
    assert call_count == 2

    result4 = func(5)
    assert result4 == 10
    assert call_count == 2


def test_cache_info_statistics():
    """Verify cache_info returns hit/miss statistics.
    """
    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        return x * 2

    func(1)
    func(1)
    func(2)
    func(2)
    func(1)

    info = cachu.cache_info(func)
    assert info.hits == 3
    assert info.misses == 2
