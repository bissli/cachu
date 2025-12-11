"""Test cache_set functionality for setting specific cache keys.
"""
import cachu
import pytest


def test_cache_set_basic():
    """Verify cache_set updates the cached value.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='users')
    def get_user(user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': user_id, 'name': f'user_{user_id}'}

    result1 = get_user(123)
    assert result1 == {'id': 123, 'name': 'user_123'}
    assert call_count == 1

    result2 = get_user(123)
    assert result2 == {'id': 123, 'name': 'user_123'}
    assert call_count == 1

    cachu.cache_set(get_user, {'id': 123, 'name': 'updated_user'}, user_id=123)

    result3 = get_user(123)
    assert result3 == {'id': 123, 'name': 'updated_user'}
    assert call_count == 1


def test_cache_set_with_multiple_params():
    """Verify cache_set with multiple parameters.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='data')
    def get_data(user_id: int, key: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {'user_id': user_id, 'key': key, 'value': 'original'}

    get_data(123, 'profile')
    get_data(123, 'settings')
    assert call_count == 2

    cachu.cache_set(get_data, {'user_id': 123, 'key': 'profile', 'value': 'updated'}, user_id=123, key='profile')

    result = get_data(123, 'profile')
    assert result['value'] == 'updated'
    assert call_count == 2

    result = get_data(123, 'settings')
    assert result['value'] == 'original'
    assert call_count == 2


def test_cache_set_with_defaults():
    """Verify cache_set works with default parameter values.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory', tag='api')
    def fetch_data(resource: str, latest: bool = False) -> dict:
        nonlocal call_count
        call_count += 1
        return {'resource': resource, 'latest': latest, 'data': 'original'}

    fetch_data('users', latest=True)
    assert call_count == 1

    cachu.cache_set(fetch_data, {'resource': 'users', 'latest': True, 'data': 'updated'}, resource='users', latest=True)

    result = fetch_data('users', latest=True)
    assert result['data'] == 'updated'
    assert call_count == 1


def test_cache_set_file_backend(temp_cache_dir):
    """Verify cache_set works with file backend.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='file', tag='items')
    def get_item(item_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': item_id, 'data': 'original_data'}

    get_item(100)
    assert call_count == 1

    cachu.cache_set(get_item, {'id': 100, 'data': 'updated_data'}, item_id=100)

    result = get_item(100)
    assert result['data'] == 'updated_data'
    assert call_count == 1


@pytest.mark.redis
def test_cache_set_redis_backend(redis_docker):
    """Verify cache_set works with Redis backend.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='redis', tag='items')
    def get_item(item_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {'id': item_id, 'data': 'original_data'}

    get_item(100)
    assert call_count == 1

    cachu.cache_set(get_item, {'id': 100, 'data': 'updated_data'}, item_id=100)

    result = get_item(100)
    assert result['data'] == 'updated_data'
    assert call_count == 1


def test_cache_set_not_decorated_raises():
    """Verify cache_set raises ValueError for non-decorated functions.
    """
    def plain_func(x: int) -> int:
        return x * 2

    with pytest.raises(ValueError, match='not decorated with @cache'):
        cachu.cache_set(plain_func, 10, x=5)
