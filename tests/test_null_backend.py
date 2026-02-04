"""Tests for null backend (passthrough, no caching).
"""
import cachu
from cachu.backends import NO_VALUE
from cachu.backends.null import NullBackend


class TestNullBackendDirect:
    """Tests for NullBackend direct API.
    """

    def test_get_returns_no_value(self):
        """Verify get() always returns NO_VALUE.
        """
        backend = NullBackend()
        assert backend.get('any_key') is NO_VALUE

    def test_get_with_metadata_returns_no_value(self):
        """Verify get_with_metadata() returns (NO_VALUE, None).
        """
        backend = NullBackend()
        value, created_at = backend.get_with_metadata('any_key')
        assert value is NO_VALUE
        assert created_at is None

    def test_set_is_noop(self):
        """Verify set() does nothing (value not retrievable).
        """
        backend = NullBackend()
        backend.set('key', 'value', 60)
        assert backend.get('key') is NO_VALUE

    def test_delete_is_noop(self):
        """Verify delete() does nothing without error.
        """
        backend = NullBackend()
        backend.delete('nonexistent_key')

    def test_clear_returns_zero(self):
        """Verify clear() returns 0.
        """
        backend = NullBackend()
        assert backend.clear() == 0
        assert backend.clear('pattern*') == 0

    def test_keys_yields_nothing(self):
        """Verify keys() yields no items.
        """
        backend = NullBackend()
        assert list(backend.keys()) == []
        assert list(backend.keys('pattern*')) == []

    def test_count_returns_zero(self):
        """Verify count() always returns 0.
        """
        backend = NullBackend()
        assert backend.count() == 0
        assert backend.count('pattern*') == 0

    def test_get_mutex_returns_null_mutex(self):
        """Verify get_mutex() returns NullMutex.
        """
        from cachu.mutex import NullMutex

        backend = NullBackend()
        mutex = backend.get_mutex('key')
        assert isinstance(mutex, NullMutex)

    def test_incr_stat_is_noop(self):
        """Verify incr_stat() does nothing.
        """
        backend = NullBackend()
        backend.incr_stat('func', 'hits')
        backend.incr_stat('func', 'misses')

    def test_get_stats_returns_zeros(self):
        """Verify get_stats() always returns (0, 0).
        """
        backend = NullBackend()
        backend.incr_stat('func', 'hits')
        assert backend.get_stats('func') == (0, 0)
        assert backend.get_stats('unknown') == (0, 0)

    def test_clear_stats_is_noop(self):
        """Verify clear_stats() does nothing without error.
        """
        backend = NullBackend()
        backend.clear_stats('func')
        backend.clear_stats()


class TestNullBackendAsync:
    """Tests for NullBackend async API.
    """

    async def test_aget_returns_no_value(self):
        """Verify aget() always returns NO_VALUE.
        """
        backend = NullBackend()
        assert await backend.aget('any_key') is NO_VALUE

    async def test_aget_with_metadata_returns_no_value(self):
        """Verify aget_with_metadata() returns (NO_VALUE, None).
        """
        backend = NullBackend()
        value, created_at = await backend.aget_with_metadata('any_key')
        assert value is NO_VALUE
        assert created_at is None

    async def test_aset_is_noop(self):
        """Verify aset() does nothing (value not retrievable).
        """
        backend = NullBackend()
        await backend.aset('key', 'value', 60)
        assert await backend.aget('key') is NO_VALUE

    async def test_adelete_is_noop(self):
        """Verify adelete() does nothing without error.
        """
        backend = NullBackend()
        await backend.adelete('nonexistent_key')

    async def test_aclear_returns_zero(self):
        """Verify aclear() returns 0.
        """
        backend = NullBackend()
        assert await backend.aclear() == 0
        assert await backend.aclear('pattern*') == 0

    async def test_akeys_yields_nothing(self):
        """Verify akeys() yields no items.
        """
        backend = NullBackend()
        keys = [k async for k in backend.akeys()]
        assert keys == []

    async def test_acount_returns_zero(self):
        """Verify acount() always returns 0.
        """
        backend = NullBackend()
        assert await backend.acount() == 0
        assert await backend.acount('pattern*') == 0

    async def test_get_async_mutex_returns_null_async_mutex(self):
        """Verify get_async_mutex() returns NullAsyncMutex.
        """
        from cachu.mutex import NullAsyncMutex

        backend = NullBackend()
        mutex = backend.get_async_mutex('key')
        assert isinstance(mutex, NullAsyncMutex)

    async def test_aincr_stat_is_noop(self):
        """Verify aincr_stat() does nothing.
        """
        backend = NullBackend()
        await backend.aincr_stat('func', 'hits')
        await backend.aincr_stat('func', 'misses')

    async def test_aget_stats_returns_zeros(self):
        """Verify aget_stats() always returns (0, 0).
        """
        backend = NullBackend()
        await backend.aincr_stat('func', 'hits')
        assert await backend.aget_stats('func') == (0, 0)
        assert await backend.aget_stats('unknown') == (0, 0)

    async def test_aclear_stats_is_noop(self):
        """Verify aclear_stats() does nothing without error.
        """
        backend = NullBackend()
        await backend.aclear_stats('func')
        await backend.aclear_stats()


class TestNullBackendWithDecorator:
    """Tests for null backend integration with @cache decorator.
    """

    def test_sync_function_always_executes(self):
        """Verify sync function always executes (no caching).
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='null')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = compute(5)
        assert result1 == 10
        assert call_count == 1

        result2 = compute(5)
        assert result2 == 10
        assert call_count == 2

        result3 = compute(5)
        assert result3 == 10
        assert call_count == 3

    async def test_async_function_always_executes(self):
        """Verify async function always executes (no caching).
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='null')
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = await compute(5)
        assert result1 == 10
        assert call_count == 1

        result2 = await compute(5)
        assert result2 == 10
        assert call_count == 2

        result3 = await compute(5)
        assert result3 == 10
        assert call_count == 3

    def test_invalidate_is_noop(self):
        """Verify invalidate() works without error on null backend.
        """
        @cachu.cache(ttl=60, backend='null')
        def compute(x: int) -> int:
            return x * 2

        compute(5)
        compute.invalidate(x=5)

    def test_refresh_always_executes(self):
        """Verify refresh() always executes function on null backend.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='null')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute.refresh(x=5)
        assert call_count == 1

        compute.refresh(x=5)
        assert call_count == 2
