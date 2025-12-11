"""Test default backend routing functionality.
"""
import cachu


def test_cache_uses_configured_default_backend():
    """Verify @cache without backend uses configured default.
    """
    cachu.configure(backend='memory')
    call_count = 0

    @cachu.cache(ttl=300)
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_cache_with_file_default(temp_cache_dir):
    """Verify @cache uses file backend when configured as default.
    """
    cachu.configure(backend='file')
    call_count = 0

    @cachu.cache(ttl=300)
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = expensive_func(5)
    result2 = expensive_func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1


def test_cache_with_tag():
    """Verify @cache works with tag.
    """
    @cachu.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        return {'id': user_id, 'name': 'test'}

    result = get_user(123)
    assert result['id'] == 123


def test_cache_clear_by_backend():
    """Verify cache_clear clears the specified backend.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def expensive_func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    expensive_func(5)
    assert call_count == 1

    cachu.cache_clear(backend='memory', ttl=300)

    expensive_func(5)
    assert call_count == 2


def test_cache_set_updates_value():
    """Verify cache_set updates cached value.
    """
    @cachu.cache(ttl=300, backend='memory', tag='test')
    def get_value(key: str) -> str:
        return f'computed_{key}'

    cachu.cache_set(get_value, 'preset_value', key='mykey')

    result = get_value('mykey')
    assert result == 'preset_value'


def test_cache_delete_removes_value():
    """Verify cache_delete removes cached value.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='test')
    def get_value(key: str) -> str:
        nonlocal call_count
        call_count += 1
        return f'computed_{key}'

    get_value('mykey')
    assert call_count == 1

    cachu.cache_delete(get_value, key='mykey')

    get_value('mykey')
    assert call_count == 2


def test_explicit_backend_overrides_default():
    """Verify explicit backend parameter overrides configured default.
    """
    cachu.configure(backend='file')

    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    result1 = func(5)
    result2 = func(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count == 1
