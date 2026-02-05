"""Reusable test suites for cache backends.

Backend-specific test classes inherit from these suites to get comprehensive
test coverage without code duplication. Follows dogpile.cache's testing pattern.
"""
import time
from abc import ABC, abstractmethod
from typing import Any

import cachu
import pytest
from cachu.backends import NO_VALUE


class _GenericBackendTestSuite(ABC):
    """Base test suite that all sync backends inherit.

    Subclasses must set:
        backend: str - The backend name ('memory', 'file', 'redis')

    Subclasses may override:
        get_backend_config() - Return additional config for cachu.configure()
        supports_zero_ttl: bool - Whether backend supports TTL=0 (default True)
    """

    backend: str = None
    supports_zero_ttl: bool = True

    def get_backend_config(self) -> dict[str, Any]:
        """Return backend-specific configuration.
        """
        return {}

    def test_basic_caching(self):
        """Verify decorator caches function results.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        def expensive_func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = expensive_func(5)
        result2 = expensive_func(5)

        assert result1 == 10
        assert result2 == 10
        assert call_count == 1

    def test_different_args_create_different_entries(self):
        """Verify different arguments result in separate cache entries.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        func(5)
        func(10)

        assert call_count == 2

    def test_with_tag(self):
        """Verify tag parameter is accepted and creates working cache.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend, tag='users')
        def get_user(user_id: int) -> dict:
            nonlocal call_count
            call_count += 1
            return {'id': user_id, 'name': 'test'}

        result1 = get_user(123)
        result2 = get_user(123)

        assert result1['id'] == 123
        assert result2['id'] == 123
        assert call_count == 1

    def test_cache_if_prevents_caching(self):
        """Verify cache_if lambda controls what gets cached.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend, cache_if=lambda r: r is not None)
        def get_value(x: int) -> int | None:
            nonlocal call_count
            call_count += 1
            return None if x < 0 else x

        result1 = get_value(-1)
        result2 = get_value(-1)

        assert result1 is None
        assert result2 is None
        assert call_count == 2

    def test_kwargs_normalization(self):
        """Verify kwargs produce consistent cache keys.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        def func(x: int, y: int = 10) -> int:
            nonlocal call_count
            call_count += 1
            return x + y

        result1 = func(5, y=10)
        result2 = func(5, 10)
        result3 = func(x=5, y=10)

        assert result1 == result2 == result3 == 15
        assert call_count == 1

    def test_skip_cache_bypasses_cache(self):
        """Verify _skip_cache=True bypasses the cache.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = func(5)
        assert call_count == 1

        result2 = func(5, _skip_cache=True)
        assert call_count == 2
        assert result1 == result2 == 10

    def test_overwrite_cache_refreshes_value(self):
        """Verify _overwrite_cache=True refreshes cached value.
        """
        counter = [0]

        @cachu.cache(ttl=300, backend=self.backend)
        def func(x: int) -> int:
            counter[0] += 1
            return x * counter[0]

        result1 = func(5)
        assert result1 == 5

        result2 = func(5)
        assert result2 == 5

        result3 = func(5, _overwrite_cache=True)
        assert result3 == 10

        result4 = func(5)
        assert result4 == 10

    def test_cache_info_tracks_hits_misses(self):
        """Verify cache_info returns accurate statistics.
        """
        @cachu.cache(ttl=300, backend=self.backend)
        def func(x: int) -> int:
            return x * 2

        func(5)
        func(5)
        func(10)
        func(5)

        info = cachu.cache_info(func)
        assert info.hits == 2
        assert info.misses == 2

    def test_invalidate_removes_entry(self):
        """Verify func.invalidate() removes specific cache entry.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        assert call_count == 1
        compute(5)
        assert call_count == 1

        compute.invalidate(x=5)

        compute(5)
        assert call_count == 2

    def test_invalidate_only_removes_matching_key(self):
        """Verify func.invalidate() doesn't affect other keys.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        compute(10)
        assert call_count == 2

        compute.invalidate(x=5)

        compute(5)
        assert call_count == 3
        compute(10)
        assert call_count == 3

    def test_refresh_recomputes_and_caches(self):
        """Verify func.refresh() recomputes and stores new value.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = compute(5)
        assert call_count == 1
        assert result1 == 10

        result2 = compute.refresh(x=5)
        assert call_count == 2
        assert result2 == 10

        result3 = compute(5)
        assert call_count == 2
        assert result3 == 10

    def test_get_returns_cached_value(self):
        """Verify func.get() retrieves cached value without execution.
        """
        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            return x * 2

        compute(5)
        result = compute.get(x=5)
        assert result == 10

    def test_get_raises_keyerror_when_not_cached(self):
        """Verify func.get() raises KeyError for missing key.
        """
        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            return x * 2

        with pytest.raises(KeyError):
            compute.get(x=999)

    def test_get_returns_default_when_not_cached(self):
        """Verify func.get() returns default parameter for missing key.
        """
        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            return x * 2

        result = compute.get(default='fallback', x=888)
        assert result == 'fallback'

    def test_set_stores_value_without_execution(self):
        """Verify func.set() stores value without calling function.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute.set('preset_value', x=42)

        result = compute(42)
        assert result == 'preset_value'
        assert call_count == 0

    def test_original_calls_unwrapped_function(self):
        """Verify func.original() calls function bypassing cache.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        assert call_count == 1

        result = compute.original(5)
        assert result == 10
        assert call_count == 2

        compute(5)
        assert call_count == 2

    def test_complex_objects_roundtrip(self):
        """Verify complex nested objects serialize/deserialize correctly.
        """
        @cachu.cache(ttl=300, backend=self.backend)
        def get_data() -> dict:
            return {
                'users': [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
                'metadata': {'version': '1.0', 'count': 2},
            }

        result1 = get_data()
        result2 = get_data()

        assert result1 == result2
        assert len(result1['users']) == 2

    def test_zero_ttl_recomputes_every_call(self):
        """Verify TTL=0 causes immediate expiration (no caching).
        """
        if not self.supports_zero_ttl:
            pytest.skip('Backend does not support TTL=0')

        call_count = 0

        @cachu.cache(ttl=0, backend=self.backend)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        func(5)
        func(5)
        assert call_count == 2


class _GenericBackendTestSuiteWithTTL(_GenericBackendTestSuite):
    """Extended test suite with TTL tests that require time.sleep().

    These tests are slower and marked with @pytest.mark.slow.
    """

    @pytest.mark.slow
    def test_ttl_expiration(self):
        """Verify cached values expire after TTL.
        """
        call_count = 0

        @cachu.cache(ttl=1, backend=self.backend)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        func(5)
        assert call_count == 1
        func(5)
        assert call_count == 1

        time.sleep(1.5)

        func(5)
        assert call_count == 2

    @pytest.mark.slow
    def test_ttl_boundary_still_valid(self):
        """Verify cached value is valid just before TTL expires.
        """
        call_count = 0

        @cachu.cache(ttl=1, backend=self.backend)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        func(5)
        assert call_count == 1

        time.sleep(0.5)

        result = func(5)
        assert result == 10
        assert call_count == 1

    @pytest.mark.slow
    def test_ttl_just_expired(self):
        """Verify cached value expires just after TTL boundary.
        """
        call_count = 0

        @cachu.cache(ttl=1, backend=self.backend)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        func(5)
        assert call_count == 1

        time.sleep(1.1)

        func(5)
        assert call_count == 2


class _GenericAsyncBackendTestSuite(ABC):
    """Base test suite that all async backends inherit.

    Subclasses must set:
        backend: str - The backend name ('memory', 'file', 'redis')
    """

    backend: str = None

    def get_backend_config(self) -> dict[str, Any]:
        """Return backend-specific configuration.
        """
        return {}

    async def test_async_basic_caching(self):
        """Verify async decorator caches function results.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        async def expensive_func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = await expensive_func(5)
        result2 = await expensive_func(5)

        assert result1 == 10
        assert result2 == 10
        assert call_count == 1

    async def test_async_different_args_create_different_entries(self):
        """Verify different arguments result in separate cache entries.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        async def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await func(5)
        await func(10)

        assert call_count == 2

    async def test_async_with_tag(self):
        """Verify tag parameter is accepted and creates working cache.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend, tag='users')
        async def get_user(user_id: int) -> dict:
            nonlocal call_count
            call_count += 1
            return {'id': user_id, 'name': 'test'}

        result1 = await get_user(123)
        result2 = await get_user(123)

        assert result1['id'] == 123
        assert result2['id'] == 123
        assert call_count == 1

    async def test_async_cache_if_prevents_caching(self):
        """Verify cache_if lambda controls what gets cached.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend, cache_if=lambda r: r is not None)
        async def get_value(x: int) -> int | None:
            nonlocal call_count
            call_count += 1
            return None if x < 0 else x

        result1 = await get_value(-1)
        result2 = await get_value(-1)

        assert result1 is None
        assert result2 is None
        assert call_count == 2

    async def test_async_kwargs_normalization(self):
        """Verify kwargs produce consistent cache keys.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        async def func(x: int, y: int = 10) -> int:
            nonlocal call_count
            call_count += 1
            return x + y

        result1 = await func(5, y=10)
        result2 = await func(5, 10)
        result3 = await func(x=5, y=10)

        assert result1 == result2 == result3 == 15
        assert call_count == 1

    async def test_async_skip_cache_bypasses_cache(self):
        """Verify _skip_cache=True bypasses the cache.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend=self.backend)
        async def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = await func(5)
        assert call_count == 1

        result2 = await func(5, _skip_cache=True)
        assert call_count == 2
        assert result1 == result2 == 10

    async def test_async_overwrite_cache_refreshes_value(self):
        """Verify _overwrite_cache=True refreshes cached value.
        """
        counter = [0]

        @cachu.cache(ttl=300, backend=self.backend)
        async def func(x: int) -> int:
            counter[0] += 1
            return x * counter[0]

        result1 = await func(5)
        assert result1 == 5

        result2 = await func(5)
        assert result2 == 5

        result3 = await func(5, _overwrite_cache=True)
        assert result3 == 10

        result4 = await func(5)
        assert result4 == 10

    async def test_async_cache_info_tracks_hits_misses(self):
        """Verify async_cache_info returns accurate statistics.
        """
        @cachu.cache(ttl=300, backend=self.backend)
        async def func(x: int) -> int:
            return x * 2

        await func(5)
        await func(5)
        await func(10)
        await func(5)

        info = await cachu.async_cache_info(func)
        assert info.hits == 2
        assert info.misses == 2

    async def test_async_invalidate_removes_entry(self):
        """Verify async func.invalidate() removes specific cache entry.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await compute(5)
        assert call_count == 1
        await compute(5)
        assert call_count == 1

        await compute.invalidate(x=5)

        await compute(5)
        assert call_count == 2

    async def test_async_invalidate_only_removes_matching_key(self):
        """Verify async func.invalidate() doesn't affect other keys.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await compute(5)
        await compute(10)
        assert call_count == 2

        await compute.invalidate(x=5)

        await compute(5)
        assert call_count == 3
        await compute(10)
        assert call_count == 3

    async def test_async_refresh_recomputes_and_caches(self):
        """Verify async func.refresh() recomputes and stores new value.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = await compute(5)
        assert call_count == 1
        assert result1 == 10

        result2 = await compute.refresh(x=5)
        assert call_count == 2
        assert result2 == 10

        result3 = await compute(5)
        assert call_count == 2
        assert result3 == 10

    async def test_async_get_returns_cached_value(self):
        """Verify async func.get() retrieves cached value.
        """
        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            return x * 2

        await compute(5)
        result = await compute.get(x=5)
        assert result == 10

    async def test_async_get_raises_keyerror_when_not_cached(self):
        """Verify async func.get() raises KeyError for missing key.
        """
        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            return x * 2

        with pytest.raises(KeyError):
            await compute.get(x=999)

    async def test_async_get_returns_default_when_not_cached(self):
        """Verify async func.get() returns default for missing key.
        """
        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            return x * 2

        result = await compute.get(default='fallback', x=888)
        assert result == 'fallback'

    async def test_async_set_stores_value_without_execution(self):
        """Verify async func.set() stores value without calling function.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await compute.set('preset_value', x=42)

        result = await compute(42)
        assert result == 'preset_value'
        assert call_count == 0

    async def test_async_original_calls_unwrapped_function(self):
        """Verify async func.original() calls function bypassing cache.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend=self.backend)
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await compute(5)
        assert call_count == 1

        result = await compute.original(5)
        assert result == 10
        assert call_count == 2

        await compute(5)
        assert call_count == 2


class _GenericDirectBackendTestSuite(ABC):
    """Test suite for direct backend API (not via decorator).

    This tests the backend implementation directly, useful for verifying
    backend-specific behaviors like pattern matching and cleanup.

    Subclasses must implement create_backend() to provide backend instance.
    """

    @abstractmethod
    def create_backend(self):
        """Create and return a backend instance for testing.
        """

    def test_set_get_roundtrip(self):
        """Verify direct set/get works.
        """
        backend = self.create_backend()
        backend.set('key1', 'value1', 300)
        result = backend.get('key1')
        assert result == 'value1'

    def test_get_nonexistent_returns_no_value(self):
        """Verify get() returns NO_VALUE for missing key.
        """
        backend = self.create_backend()
        result = backend.get('nonexistent')
        assert result is NO_VALUE

    def test_get_with_metadata(self):
        """Verify get_with_metadata returns value and timestamp.
        """
        backend = self.create_backend()
        backend.set('key1', 'value1', 300)
        value, created_at = backend.get_with_metadata('key1')
        assert value == 'value1'
        assert created_at is not None

    def test_delete(self):
        """Verify delete removes entry.
        """
        backend = self.create_backend()
        backend.set('key1', 'value1', 300)
        backend.delete('key1')
        result = backend.get('key1')
        assert result is NO_VALUE

    def test_clear_all(self):
        """Verify clear() removes all entries.
        """
        backend = self.create_backend()
        backend.set('key1', 'value1', 300)
        backend.set('key2', 'value2', 300)

        count = backend.clear()
        assert count == 2

        assert backend.get('key1') is NO_VALUE
        assert backend.get('key2') is NO_VALUE

    def test_clear_pattern(self):
        """Verify clear(pattern) removes matching entries only.
        """
        backend = self.create_backend()
        backend.set('user:1', 'value1', 300)
        backend.set('user:2', 'value2', 300)
        backend.set('other:1', 'value3', 300)

        count = backend.clear('user:*')
        assert count == 2

        assert backend.get('user:1') is NO_VALUE
        assert backend.get('user:2') is NO_VALUE
        assert backend.get('other:1') == 'value3'

    def test_keys(self):
        """Verify keys() iterates over all keys.
        """
        backend = self.create_backend()
        backend.set('key1', 'value1', 300)
        backend.set('key2', 'value2', 300)

        keys = list(backend.keys())
        assert set(keys) == {'key1', 'key2'}

    def test_keys_pattern(self):
        """Verify keys(pattern) filters keys.
        """
        backend = self.create_backend()
        backend.set('user:1', 'value1', 300)
        backend.set('user:2', 'value2', 300)
        backend.set('other:1', 'value3', 300)

        keys = list(backend.keys('user:*'))
        assert set(keys) == {'user:1', 'user:2'}

    def test_count(self):
        """Verify count() returns correct count.
        """
        backend = self.create_backend()
        backend.set('key1', 'value1', 300)
        backend.set('key2', 'value2', 300)

        count = backend.count()
        assert count == 2

    def test_count_pattern(self):
        """Verify count(pattern) counts matching keys only.
        """
        backend = self.create_backend()
        backend.set('user:1', 'value1', 300)
        backend.set('user:2', 'value2', 300)
        backend.set('other:1', 'value3', 300)

        count = backend.count('user:*')
        assert count == 2

    def test_stats_incr_and_get(self):
        """Verify stats increment and retrieval.
        """
        backend = self.create_backend()
        backend.incr_stat('my_func', 'hits')
        backend.incr_stat('my_func', 'hits')
        backend.incr_stat('my_func', 'misses')

        hits, misses = backend.get_stats('my_func')
        assert hits == 2
        assert misses == 1

    def test_stats_unknown_function(self):
        """Verify stats return (0, 0) for unknown function.
        """
        backend = self.create_backend()
        hits, misses = backend.get_stats('unknown_func')
        assert hits == 0
        assert misses == 0

    def test_clear_stats_specific(self):
        """Verify clear_stats(fn_name) clears only that function.
        """
        backend = self.create_backend()
        backend.incr_stat('func1', 'hits')
        backend.incr_stat('func2', 'hits')

        backend.clear_stats('func1')

        assert backend.get_stats('func1') == (0, 0)
        assert backend.get_stats('func2') == (1, 0)

    def test_clear_stats_all(self):
        """Verify clear_stats() clears all stats.
        """
        backend = self.create_backend()
        backend.incr_stat('func1', 'hits')
        backend.incr_stat('func2', 'misses')

        backend.clear_stats()

        assert backend.get_stats('func1') == (0, 0)
        assert backend.get_stats('func2') == (0, 0)


class _GenericAsyncDirectBackendTestSuite(ABC):
    """Async test suite for direct backend API.

    Subclasses must implement create_backend() to provide backend instance.
    """

    @abstractmethod
    def create_backend(self):
        """Create and return a backend instance for async testing.
        """

    async def test_async_set_get_roundtrip(self):
        """Verify async set/get works.
        """
        backend = self.create_backend()
        await backend.aset('key1', 'value1', 300)
        result = await backend.aget('key1')
        assert result == 'value1'

    async def test_async_get_nonexistent_returns_no_value(self):
        """Verify aget() returns NO_VALUE for missing key.
        """
        backend = self.create_backend()
        result = await backend.aget('nonexistent')
        assert result is NO_VALUE

    async def test_async_get_with_metadata(self):
        """Verify aget_with_metadata returns value and timestamp.
        """
        backend = self.create_backend()
        await backend.aset('key1', 'value1', 300)
        value, created_at = await backend.aget_with_metadata('key1')
        assert value == 'value1'
        assert created_at is not None

    async def test_async_delete(self):
        """Verify adelete removes entry.
        """
        backend = self.create_backend()
        await backend.aset('key1', 'value1', 300)
        await backend.adelete('key1')
        result = await backend.aget('key1')
        assert result is NO_VALUE

    async def test_async_clear_all(self):
        """Verify aclear() removes all entries.
        """
        backend = self.create_backend()
        await backend.aset('key1', 'value1', 300)
        await backend.aset('key2', 'value2', 300)

        count = await backend.aclear()
        assert count == 2

        assert await backend.aget('key1') is NO_VALUE
        assert await backend.aget('key2') is NO_VALUE

    async def test_async_clear_pattern(self):
        """Verify aclear(pattern) removes matching entries only.
        """
        backend = self.create_backend()
        await backend.aset('user:1', 'value1', 300)
        await backend.aset('user:2', 'value2', 300)
        await backend.aset('other:1', 'value3', 300)

        count = await backend.aclear('user:*')
        assert count == 2

        assert await backend.aget('user:1') is NO_VALUE
        assert await backend.aget('user:2') is NO_VALUE
        assert await backend.aget('other:1') == 'value3'

    async def test_async_keys(self):
        """Verify akeys() iterates over all keys.
        """
        backend = self.create_backend()
        await backend.aset('key1', 'value1', 300)
        await backend.aset('key2', 'value2', 300)

        keys = [k async for k in backend.akeys()]
        assert set(keys) == {'key1', 'key2'}

    async def test_async_keys_pattern(self):
        """Verify akeys(pattern) filters keys.
        """
        backend = self.create_backend()
        await backend.aset('user:1', 'value1', 300)
        await backend.aset('user:2', 'value2', 300)
        await backend.aset('other:1', 'value3', 300)

        keys = [k async for k in backend.akeys('user:*')]
        assert set(keys) == {'user:1', 'user:2'}

    async def test_async_count(self):
        """Verify acount() returns correct count.
        """
        backend = self.create_backend()
        await backend.aset('key1', 'value1', 300)
        await backend.aset('key2', 'value2', 300)

        count = await backend.acount()
        assert count == 2

    async def test_async_count_pattern(self):
        """Verify acount(pattern) counts matching keys only.
        """
        backend = self.create_backend()
        await backend.aset('user:1', 'value1', 300)
        await backend.aset('user:2', 'value2', 300)
        await backend.aset('other:1', 'value3', 300)

        count = await backend.acount('user:*')
        assert count == 2

    async def test_async_stats_incr_and_get(self):
        """Verify async stats increment and retrieval.
        """
        backend = self.create_backend()
        await backend.aincr_stat('async_func', 'hits')
        await backend.aincr_stat('async_func', 'misses')
        await backend.aincr_stat('async_func', 'misses')

        hits, misses = await backend.aget_stats('async_func')
        assert hits == 1
        assert misses == 2

    async def test_async_clear_stats(self):
        """Verify async clear_stats works.
        """
        backend = self.create_backend()
        await backend.aincr_stat('func1', 'hits')
        await backend.aclear_stats('func1')

        hits, misses = await backend.aget_stats('func1')
        assert hits == 0
        assert misses == 0
