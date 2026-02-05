"""Tests for null backend (passthrough, no caching).
"""
import cachu


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
