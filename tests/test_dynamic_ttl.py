"""Tests for callable/dynamic TTL support.
"""

import cachu
from cachu.decorator import manager


class TestCallableTTL:
    """Tests for callable TTL in sync functions.
    """

    def test_callable_ttl_receives_result(self):
        """Verify callable TTL receives the function result.
        """
        received_results = []

        def capture_ttl(result: dict) -> int:
            received_results.append(result)
            return 60

        @cachu.cache(ttl=capture_ttl, backend='memory')
        def compute(x: int) -> dict:
            return {'value': x * 2}

        compute(5)

        assert len(received_results) == 1
        assert received_results[0] == {'value': 10}

    def test_callable_ttl_returns_dynamic_value(self):
        """Verify callable TTL can return different values based on result.
        """
        def dynamic_ttl(result: dict) -> int:
            return result.get('cache_seconds', 60)

        @cachu.cache(ttl=dynamic_ttl, backend='memory')
        def get_config(key: str) -> dict:
            if key == 'short':
                return {'value': 1, 'cache_seconds': 10}
            return {'value': 2, 'cache_seconds': 300}

        get_config('short')
        get_config('long')

    def test_callable_ttl_caches_result(self):
        """Verify results are cached when using callable TTL.
        """
        call_count = 0

        def fixed_ttl(result: int) -> int:
            return 60

        @cachu.cache(ttl=fixed_ttl, backend='memory')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = compute(5)
        assert result1 == 10
        assert call_count == 1

        result2 = compute(5)
        assert result2 == 10
        assert call_count == 1

    def test_callable_ttl_with_cache_if(self):
        """Verify callable TTL works with cache_if.
        """
        ttl_calls = []

        def track_ttl(result: int) -> int:
            ttl_calls.append(result)
            return 60

        @cachu.cache(
            ttl=track_ttl,
            backend='memory',
            cache_if=lambda x: x > 0,
        )
        def compute(x: int) -> int:
            return x

        compute(5)
        assert len(ttl_calls) == 1

        compute(-1)
        assert len(ttl_calls) == 1

    def test_int_ttl_still_works(self):
        """Verify integer TTL still works as before.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = compute(5)
        assert result1 == 10
        assert call_count == 1

        result2 = compute(5)
        assert result2 == 10
        assert call_count == 1


class TestAsyncCallableTTL:
    """Tests for callable TTL in async functions.
    """

    async def test_callable_ttl_receives_result(self):
        """Verify callable TTL receives the async function result.
        """
        received_results = []

        def capture_ttl(result: dict) -> int:
            received_results.append(result)
            return 60

        @cachu.cache(ttl=capture_ttl, backend='memory')
        async def compute(x: int) -> dict:
            return {'value': x * 2}

        await compute(5)

        assert len(received_results) == 1
        assert received_results[0] == {'value': 10}

    async def test_callable_ttl_caches_result(self):
        """Verify results are cached when using callable TTL with async.
        """
        call_count = 0

        def fixed_ttl(result: int) -> int:
            return 60

        @cachu.cache(ttl=fixed_ttl, backend='memory')
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = await compute(5)
        assert result1 == 10
        assert call_count == 1

        result2 = await compute(5)
        assert result2 == 10
        assert call_count == 1

    async def test_callable_ttl_with_cache_if(self):
        """Verify callable TTL works with cache_if in async.
        """
        ttl_calls = []

        def track_ttl(result: int) -> int:
            ttl_calls.append(result)
            return 60

        @cachu.cache(
            ttl=track_ttl,
            backend='memory',
            cache_if=lambda x: x > 0,
        )
        async def compute(x: int) -> int:
            return x

        await compute(5)
        assert len(ttl_calls) == 1

        await compute(-1)
        assert len(ttl_calls) == 1


class TestCallableTTLBackendKeying:
    """Tests for backend keying with callable TTL.
    """

    def test_callable_ttl_uses_sentinel_for_backend_key(self):
        """Verify callable TTL uses -1 sentinel for backend keying.
        """
        def dynamic_ttl(result: int) -> int:
            return result * 10

        @cachu.cache(ttl=dynamic_ttl, backend='memory', package='test_sentinel')
        def compute(x: int) -> int:
            return x

        compute(5)

        key = ('test_sentinel', 'memory', -1)
        assert key in manager.backends

    def test_different_callable_ttls_share_backend(self):
        """Verify functions with different callable TTLs share backend (same sentinel).
        """
        def ttl_a(result: int) -> int:
            return 60

        def ttl_b(result: int) -> int:
            return 120

        @cachu.cache(ttl=ttl_a, backend='memory', package='test_shared')
        def compute_a(x: int) -> int:
            return x

        @cachu.cache(ttl=ttl_b, backend='memory', package='test_shared')
        def compute_b(x: int) -> int:
            return x

        compute_a(1)
        compute_b(2)

        backend_keys = [k for k in manager.backends if k[0] == 'test_shared']
        assert len(backend_keys) == 1
        assert backend_keys[0][2] == -1
