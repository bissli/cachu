"""Tests for decorator helper methods (.invalidate, .refresh).
"""
import cachu
import pytest


class TestSyncHelperMethods:
    """Tests for sync helper methods on decorated functions.
    """

    def test_invalidate_removes_cached_entry(self):
        """Verify func.invalidate() removes specific entry.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
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
        """Verify func.invalidate() only removes the matching key.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
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

    def test_refresh_invalidates_and_recaches(self):
        """Verify func.refresh() invalidates and recaches.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
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

    def test_invalidate_with_multiple_params(self):
        """Verify func.invalidate() works with multiple parameters.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        def compute(a: int, b: int) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        compute(1, 2)
        compute(3, 4)
        assert call_count == 2

        compute.invalidate(a=1, b=2)

        compute(1, 2)
        assert call_count == 3
        compute(3, 4)
        assert call_count == 3

    def test_helper_methods_exist(self):
        """Verify helper methods are attached to decorated function.
        """
        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        assert hasattr(compute, 'invalidate')
        assert hasattr(compute, 'refresh')
        assert callable(compute.invalidate)
        assert callable(compute.refresh)


class TestAsyncHelperMethods:
    """Tests for async helper methods on decorated functions.
    """

    async def test_invalidate_removes_cached_entry(self):
        """Verify async func.invalidate() removes specific entry.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
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

    async def test_invalidate_only_removes_matching_key(self):
        """Verify async func.invalidate() only removes the matching key.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
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

    async def test_refresh_invalidates_and_recaches(self):
        """Verify async func.refresh() invalidates and recaches.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
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

    async def test_invalidate_with_multiple_params(self):
        """Verify async func.invalidate() works with multiple parameters.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        async def compute(a: int, b: int) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        await compute(1, 2)
        await compute(3, 4)
        assert call_count == 2

        await compute.invalidate(a=1, b=2)

        await compute(1, 2)
        assert call_count == 3
        await compute(3, 4)
        assert call_count == 3

    async def test_helper_methods_exist(self):
        """Verify async helper methods are attached to decorated function.
        """
        @cachu.cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            return x * 2

        assert hasattr(compute, 'invalidate')
        assert hasattr(compute, 'refresh')


class TestHelperMethodsWithFileBackend:
    """Tests for helper methods with file (SQLite) backend.
    """

    def test_sync_invalidate(self, temp_cache_dir):
        """Verify sync invalidate works with file backend.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='file')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        assert call_count == 1

        compute.invalidate(x=5)

        compute(5)
        assert call_count == 2

    async def test_async_invalidate(self, temp_cache_dir):
        """Verify async invalidate works with file backend.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='file')
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await compute(5)
        assert call_count == 1

        await compute.invalidate(x=5)

        await compute(5)
        assert call_count == 2


@pytest.mark.redis
class TestHelperMethodsWithRedisBackend:
    """Tests for helper methods with Redis backend.
    """

    def test_sync_invalidate(self, redis_docker):
        """Verify sync invalidate works with Redis backend.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='redis')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        assert call_count == 1

        compute.invalidate(x=5)

        compute(5)
        assert call_count == 2

    async def test_async_invalidate(self, redis_docker):
        """Verify async invalidate works with Redis backend.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='redis')
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        await compute(5)
        assert call_count == 1

        await compute.invalidate(x=5)

        await compute(5)
        assert call_count == 2
