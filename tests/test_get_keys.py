"""Tests for cache_get and async_cache_get functions.
"""
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
