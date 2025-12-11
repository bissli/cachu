"""Test cache_delete functionality for deleting specific cache keys.
"""
import cachu
import pytest


def test_cache_delete_basic():
    """Verify cache_delete removes only the specified entry.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': user_id, 'name': f'user_{user_id}'}

    result1 = get_user(123)
    result2 = get_user(456)
    assert call_count == 2

    result3 = get_user(123)
    result4 = get_user(456)
    assert call_count == 2

    cachu.cache_delete(get_user, user_id=123)

    result5 = get_user(123)
    assert call_count == 3

    result6 = get_user(456)
    assert call_count == 3


def test_cache_delete_with_multiple_params():
    """Verify cache_delete with multiple parameters.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='data')
    def get_data(user_id: int, key: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id, 'key': key, 'value': 'data'}

    get_data(123, 'profile')
    get_data(123, 'settings')
    get_data(456, 'profile')
    assert call_count == 3

    get_data(123, 'profile')
    get_data(123, 'settings')
    get_data(456, 'profile')
    assert call_count == 3

    cachu.cache_delete(get_data, user_id=123, key='profile')

    get_data(123, 'profile')
    assert call_count == 4

    get_data(123, 'settings')
    get_data(456, 'profile')
    assert call_count == 4


def test_cache_delete_with_defaults():
    """Verify cache_delete works with default parameter values.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='api')
    def fetch_data(resource: str, latest: bool = False) -> dict:
        nonlocal call_count
        call_count += 1
        return {'resource': resource, 'latest': latest, 'data': 'value'}

    fetch_data('users', latest=True)
    fetch_data('users', latest=False)
    fetch_data('users')
    assert call_count == 2

    cachu.cache_delete(fetch_data, resource='users', latest=True)

    fetch_data('users', latest=True)
    assert call_count == 3

    fetch_data('users', latest=False)
    fetch_data('users')
    assert call_count == 3


def test_cache_delete_file_backend(temp_cache_dir):
    """Verify cache_delete works with file backend.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='file', tag='items')
    def get_item(item_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': item_id, 'data': f'data_{item_id}'}

    get_item(100)
    get_item(200)
    assert call_count == 2

    get_item(100)
    get_item(200)
    assert call_count == 2

    cachu.cache_delete(get_item, item_id=100)

    get_item(100)
    assert call_count == 3

    get_item(200)
    assert call_count == 3


@pytest.mark.redis
def test_cache_delete_redis_backend(redis_docker):
    """Verify cache_delete works with Redis backend.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='redis', tag='items')
    def get_item(item_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': item_id, 'data': f'data_{item_id}'}

    get_item(100)
    get_item(200)
    assert call_count == 2

    get_item(100)
    get_item(200)
    assert call_count == 2

    cachu.cache_delete(get_item, item_id=100)

    get_item(100)
    assert call_count == 3

    get_item(200)
    assert call_count == 3


def test_cache_delete_not_decorated_raises():
    """Verify cache_delete raises ValueError for non-decorated functions.
    """
    def plain_func(x: int) -> int:
        return x * 2

    with pytest.raises(ValueError, match='not decorated with @cache'):
        cachu.cache_delete(plain_func, x=5)
