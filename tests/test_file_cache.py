"""Test file cache backend operations.
"""
import cachu


def test_file_cache_basic_decoration(temp_cache_dir):
    """Verify file cache decorator caches function results.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='file')
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_file_cache_different_args(temp_cache_dir):
    """Verify file cache distinguishes between different arguments.
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


def test_file_cache_with_tag(temp_cache_dir):
    """Verify tag parameter is accepted and used.
    """
    @cachu.cache(ttl=300, backend='file', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_file_cache_with_kwargs(temp_cache_dir):
    """Verify file cache handles keyword arguments correctly.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='file')
    def func(x: int, y: int = 10) -> int:
        nonlocal call_count
        call_count += 1
        return x + y

    result1 = func(5, y=10)
    result2 = func(5, 10)
    result3 = func(x=5, y=10)

    assert result1 == result2 == result3 == 15
    assert call_count == 1


def test_file_cache_complex_objects(temp_cache_dir):
    """Verify file cache can store complex objects.
    """
    @cachu.cache(ttl=300, backend='file')
    def get_data() -> dict:
        return {
            'users': [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
            'metadata': {'version': '1.0', 'count': 2},
        }

    result1 = get_data()
    result2 = get_data()

    assert result1 == result2
    assert len(result1['users']) == 2
