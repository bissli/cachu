"""Tests for cache operations: cache_get, cache_set, cache_delete.
"""
import cachu
import pytest
from cachu import cache
from cachu.operations import async_cache_get, async_cache_set, cache_get
from cachu.operations import cache_set


class TestCacheGet:
    """Tests for sync cache_get() function.
    """

    def test_returns_cached_value(self):
        """Verify cache_get returns value that was cached via function call.
        """
        call_count = 0

        @cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        assert call_count == 1

        result = cache_get(compute, x=5)
        assert result == 10
        assert call_count == 1

    def test_raises_keyerror_when_not_cached(self):
        """Verify cache_get raises KeyError when no cached value exists.
        """
        @cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        with pytest.raises(KeyError) as exc_info:
            cache_get(compute, x=999)

        assert 'No cached value' in str(exc_info.value)

    def test_returns_default_when_not_cached(self):
        """Verify cache_get returns default value when not cached.
        """
        @cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        result = cache_get(compute, default='fallback', x=888)
        assert result == 'fallback'

    def test_returns_none_default(self):
        """Verify cache_get can return None as explicit default.
        """
        @cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        result = cache_get(compute, default=None, x=777)
        assert result is None

    def test_with_cache_set(self):
        """Verify cache_get returns value set via cache_set.
        """
        @cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        cache_set(compute, 'manually_set', x=100)

        result = cache_get(compute, x=100)
        assert result == 'manually_set'

    def test_with_multiple_params(self):
        """Verify cache_get works with multiple parameters.
        """
        call_count = 0

        @cache(ttl=60, backend='memory')
        def compute(a: int, b: str) -> str:
            nonlocal call_count
            call_count += 1
            return f'{a}-{b}'

        compute(1, 'test')
        assert call_count == 1

        result = cache_get(compute, a=1, b='test')
        assert result == '1-test'
        assert call_count == 1

    def test_raises_valueerror_for_undecorated_function(self):
        """Verify cache_get raises ValueError for non-decorated function.
        """
        def not_decorated(x: int) -> int:
            return x

        with pytest.raises(ValueError) as exc_info:
            cache_get(not_decorated, x=1)

        assert 'not decorated' in str(exc_info.value)


class TestAsyncCacheGet:
    """Tests for async_cache_get() function.
    """

    async def test_returns_cached_value(self):
        """Verify async_cache_get returns value that was cached via function call.
        """
        call_count = 0

        @cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await compute(5)
        assert call_count == 1

        result = await async_cache_get(compute, x=5)
        assert result == 10
        assert call_count == 1

    async def test_raises_keyerror_when_not_cached(self):
        """Verify async_cache_get raises KeyError when no cached value exists.
        """
        @cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            return x * 2

        with pytest.raises(KeyError) as exc_info:
            await async_cache_get(compute, x=999)

        assert 'No cached value' in str(exc_info.value)

    async def test_returns_default_when_not_cached(self):
        """Verify async_cache_get returns default value when not cached.
        """
        @cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            return x * 2

        result = await async_cache_get(compute, default='fallback', x=888)
        assert result == 'fallback'

    async def test_returns_none_default(self):
        """Verify async_cache_get can return None as explicit default.
        """
        @cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            return x * 2

        result = await async_cache_get(compute, default=None, x=777)
        assert result is None

    async def test_with_cache_set(self):
        """Verify async_cache_get returns value set via async_cache_set.
        """
        @cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            return x * 2

        await async_cache_set(compute, 'manually_set', x=100)

        result = await async_cache_get(compute, x=100)
        assert result == 'manually_set'

    async def test_with_multiple_params(self):
        """Verify async_cache_get works with multiple parameters.
        """
        call_count = 0

        @cache(ttl=60, backend='memory')
        async def compute(a: int, b: str) -> str:
            nonlocal call_count
            call_count += 1
            return f'{a}-{b}'

        await compute(1, 'test')
        assert call_count == 1

        result = await async_cache_get(compute, a=1, b='test')
        assert result == '1-test'
        assert call_count == 1

    async def test_raises_valueerror_for_undecorated_function(self):
        """Verify async_cache_get raises ValueError for non-decorated function.
        """
        async def not_decorated(x: int) -> int:
            return x

        with pytest.raises(ValueError) as exc_info:
            await async_cache_get(not_decorated, x=1)

        assert 'not decorated' in str(exc_info.value)


class TestCacheSet:
    """Tests for cache_set() function.
    """

    def test_basic(self):
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

    def test_with_multiple_params(self):
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

        cachu.cache_set(
            get_data,
            {'user_id': 123, 'key': 'profile', 'value': 'updated'},
            user_id=123,
            key='profile')

        result = get_data(123, 'profile')
        assert result['value'] == 'updated'
        assert call_count == 2

        result = get_data(123, 'settings')
        assert result['value'] == 'original'
        assert call_count == 2

    def test_with_defaults(self):
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

        cachu.cache_set(
            fetch_data,
            {'resource': 'users', 'latest': True, 'data': 'updated'},
            resource='users',
            latest=True)

        result = fetch_data('users', latest=True)
        assert result['data'] == 'updated'
        assert call_count == 1

    def test_file_backend(self, temp_cache_dir):
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
    def test_redis_backend(self, redis_docker):
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

    def test_not_decorated_raises(self):
        """Verify cache_set raises ValueError for non-decorated functions.
        """
        def plain_func(x: int) -> int:
            return x * 2

        with pytest.raises(ValueError, match='not decorated with @cache'):
            cachu.cache_set(plain_func, 10, x=5)


class TestCacheDelete:
    """Tests for cache_delete() function.
    """

    def test_basic(self):
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

    def test_with_multiple_params(self):
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

    def test_with_defaults(self):
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

    def test_file_backend(self, temp_cache_dir):
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
    def test_redis_backend(self, redis_docker):
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

    def test_not_decorated_raises(self):
        """Verify cache_delete raises ValueError for non-decorated functions.
        """
        def plain_func(x: int) -> int:
            return x * 2

        with pytest.raises(ValueError, match='not decorated with @cache'):
            cachu.cache_delete(plain_func, x=5)
