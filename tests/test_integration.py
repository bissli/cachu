"""Integration tests for cache decorator usage.
"""
import cache


def test_instance_method_caching():
    """Verify cache works correctly with instance methods.
    """
    class Repository:
        def __init__(self, db_conn):
            self.conn = db_conn
            self.call_count = 0

        @cache.memorycache(seconds=300).cache_on_arguments()
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
        @cache.memorycache(seconds=300).cache_on_arguments()
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
        @cache.memorycache(seconds=300).cache_on_arguments()
        def calculate(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

    result1 = Utils.calculate(5)
    result2 = Utils.calculate(5)

    assert result1 == result2 == 10
    assert call_count == 1


def test_multiple_decorators_same_region():
    """Verify multiple functions can share same cache region.
    """
    call_count_1 = 0
    call_count_2 = 0

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func1(x: int) -> int:
        nonlocal call_count_1
        call_count_1 += 1
        return x * 2

    @cache.memorycache(seconds=300).cache_on_arguments()
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


def test_namespace_isolation():
    """Verify namespaces properly isolate cached values.
    """
    @cache.memorycache(seconds=300).cache_on_arguments(namespace='ns1')
    def func_ns1(x: int) -> int:
        return x * 2

    @cache.memorycache(seconds=300).cache_on_arguments(namespace='ns2')
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

    @cache.memorycache(seconds=300).cache_on_arguments()
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

    @cache.memorycache(seconds=300).cache_on_arguments()
    def func(**kwargs) -> dict:
        nonlocal call_count
        call_count += 1
        return kwargs

    result1 = func(a=1, b=2)
    result2 = func(a=1, b=2)
    result3 = func(b=2, a=1)

    assert result1 == result2 == result3 == {'a': 1, 'b': 2}
    assert call_count == 1


def test_mixed_backends():
    """Verify different backends can be used for different functions.
    """
    @cache.memorycache(seconds=60).cache_on_arguments()
    def memory_func(x: int) -> int:
        return x * 2

    @cache.filecache(seconds=300).cache_on_arguments()
    def file_func(x: int) -> int:
        return x * 3

    result1 = memory_func(5)
    result2 = file_func(5)

    assert result1 == 10
    assert result2 == 15

    from conftest import has_file_region, has_memory_region
    assert has_memory_region(60)
    assert has_file_region(300)
