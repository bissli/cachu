"""Tests for dogpile prevention (thundering herd protection).

These tests verify that concurrent calls with the same cache key only
execute the underlying function once, preventing the dogpile effect.
"""
import asyncio
import threading
import time

import cachu
import pytest


class TestSyncDogpilePrevention:
    """Tests for synchronous dogpile prevention.
    """

    def test_concurrent_calls_single_execution(self):
        """Verify concurrent calls result in single function execution.
        """
        call_count = 0
        barrier = threading.Barrier(5)

        @cachu.cache(ttl=60, backend='memory')
        def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            time.sleep(0.1)
            return x * 2

        def worker():
            barrier.wait()
            slow_fn(1)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 1

    def test_different_keys_execute_in_parallel(self):
        """Verify calls with different keys can execute in parallel.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            time.sleep(0.05)
            return x * 2

        threads = [
            threading.Thread(target=lambda x=i: slow_fn(x))
            for i in range(3)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 3

    def test_cached_value_returned_to_waiting_threads(self):
        """Verify waiting threads receive the same cached result.
        """
        results = []
        barrier = threading.Barrier(5)

        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            time.sleep(0.1)
            return x * 2

        def worker():
            barrier.wait()
            result = compute(5)
            results.append(result)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r == 10 for r in results)

    def test_second_wave_uses_cache(self):
        """Verify second wave of concurrent calls uses cached value.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            time.sleep(0.05)
            return x * 2

        compute(1)
        assert call_count == 1

        barrier = threading.Barrier(5)

        def worker():
            barrier.wait()
            compute(1)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 1

    def test_concurrent_calls_return_same_computed_value(self):
        """Verify all concurrent callers receive the same computed value.

        All callers should receive the value from the same computation
        (same computed_at timestamp), not from multiple computations.
        """
        results = []
        barrier = threading.Barrier(5)

        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> dict:
            time.sleep(0.1)
            return {'value': x * 2, 'computed_at': time.time()}

        def worker():
            barrier.wait()
            result = compute(5)
            results.append(result)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        first = results[0]
        assert all(r == first for r in results)
        assert all(r['computed_at'] == first['computed_at'] for r in results)


class TestAsyncDogpilePrevention:
    """Tests for asynchronous dogpile prevention.
    """

    async def test_concurrent_calls_single_execution(self):
        """Verify async concurrent calls result in single function execution.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        async def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return x * 2

        tasks = [asyncio.create_task(slow_fn(1)) for _ in range(5)]
        await asyncio.gather(*tasks)

        assert call_count == 1

    async def test_different_keys_execute_in_parallel(self):
        """Verify async calls with different keys can execute in parallel.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        async def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)
            return x * 2

        tasks = [asyncio.create_task(slow_fn(i)) for i in range(3)]
        await asyncio.gather(*tasks)

        assert call_count == 3

    async def test_cached_value_returned_to_waiting_tasks(self):
        """Verify waiting tasks receive the same cached result.
        """
        @cachu.cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            await asyncio.sleep(0.1)
            return x * 2

        tasks = [asyncio.create_task(compute(5)) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        assert all(r == 10 for r in results)

    async def test_second_wave_uses_cache(self):
        """Verify second wave of async concurrent calls uses cached value.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)
            return x * 2

        await compute(1)
        assert call_count == 1

        tasks = [asyncio.create_task(compute(1)) for _ in range(5)]
        await asyncio.gather(*tasks)

        assert call_count == 1

    async def test_concurrent_calls_return_same_computed_value(self):
        """Verify all async concurrent callers receive the same computed value.

        All callers should receive the value from the same computation
        (same computed_at timestamp), not from multiple computations.
        """
        @cachu.cache(ttl=60, backend='memory')
        async def compute(x: int) -> dict:
            await asyncio.sleep(0.1)
            return {'value': x * 2, 'computed_at': time.time()}

        tasks = [asyncio.create_task(compute(5)) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        first = results[0]
        assert all(r == first for r in results)
        assert all(r['computed_at'] == first['computed_at'] for r in results)


class TestDogpileWithFileBackend:
    """Tests for dogpile prevention with file (SQLite) backend.
    """

    def test_sync_concurrent_calls(self, temp_cache_dir):
        """Verify sync dogpile prevention works with file backend.
        """
        call_count = 0
        barrier = threading.Barrier(3)

        @cachu.cache(ttl=60, backend='file')
        def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            time.sleep(0.1)
            return x * 2

        def worker():
            barrier.wait()
            slow_fn(1)

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 1

    async def test_async_concurrent_calls(self, temp_cache_dir):
        """Verify async dogpile prevention works with file backend.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='file')
        async def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return x * 2

        tasks = [asyncio.create_task(slow_fn(1)) for _ in range(3)]
        await asyncio.gather(*tasks)

        assert call_count == 1


@pytest.mark.redis
class TestDogpileWithRedisBackend:
    """Tests for dogpile prevention with Redis backend (distributed lock).
    """

    def test_sync_concurrent_calls(self, redis_docker):
        """Verify sync dogpile prevention works with Redis backend.
        """
        call_count = 0
        barrier = threading.Barrier(3)

        @cachu.cache(ttl=60, backend='redis')
        def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            time.sleep(0.1)
            return x * 2

        def worker():
            barrier.wait()
            slow_fn(1)

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 1

    async def test_async_concurrent_calls(self, redis_docker):
        """Verify async dogpile prevention works with Redis backend.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='redis')
        async def slow_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return x * 2

        tasks = [asyncio.create_task(slow_fn(1)) for _ in range(3)]
        await asyncio.gather(*tasks)

        assert call_count == 1


class TestConcurrentInvalidation:
    """Tests for invalidation during concurrent computation.
    """

    def test_invalidate_during_computation_result_is_cached(self):
        """Verify result is cached despite mid-computation invalidation.

        Per dogpile.cache behavior: when Thread A is computing and Thread B
        calls invalidate() mid-computation, the computed value has a newer
        timestamp than the invalidation, so it gets cached normally.
        """
        call_count = 0
        computation_started = threading.Event()
        invalidation_done = threading.Event()

        @cachu.cache(ttl=60, backend='memory')
        def slow_compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            computation_started.set()
            invalidation_done.wait(timeout=1)
            time.sleep(0.05)
            return x * 2

        def compute_worker():
            slow_compute(5)

        def invalidate_worker():
            computation_started.wait(timeout=1)
            slow_compute.invalidate(x=5)
            invalidation_done.set()

        t1 = threading.Thread(target=compute_worker)
        t2 = threading.Thread(target=invalidate_worker)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        result = slow_compute(5)
        assert result == 10
        assert call_count == 1, "Invalidation during computation should not prevent caching"

    def test_invalidate_after_computation_causes_recompute(self):
        """Verify invalidation after computation causes subsequent recompute.
        """
        call_count = 0

        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        assert call_count == 1

        compute.invalidate(x=5)

        compute(5)
        assert call_count == 2


class TestConcurrentStatistics:
    """Tests for cache statistics accuracy under concurrent access.
    """

    def test_stats_accurate_under_concurrent_hits(self):
        """Verify hit/miss counts are accurate with concurrent access.

        Multiple threads accessing the same cached value should result
        in accurate hit/miss statistics (1 miss, N-1 hits).
        """
        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            time.sleep(0.05)
            return x * 2

        compute(5)

        barrier = threading.Barrier(5)

        def worker():
            barrier.wait()
            compute(5)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        info = cachu.cache_info(compute)
        assert info.misses == 1
        assert info.hits == 5

    def test_stats_accurate_with_mixed_keys(self):
        """Verify statistics track hits/misses correctly for different keys.
        """
        @cachu.cache(ttl=60, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        compute(1)
        compute(2)
        compute(3)

        compute(1)
        compute(2)
        compute(1)

        info = cachu.cache_info(compute)
        assert info.misses == 3
        assert info.hits == 3


class TestAsyncConcurrentInvalidation:
    """Tests for async invalidation during concurrent computation.
    """

    async def test_invalidate_during_async_computation_result_is_cached(self):
        """Verify async result is cached despite mid-computation invalidation.

        Per dogpile.cache behavior: computed value has a newer timestamp
        than the invalidation, so it gets cached normally.
        """
        call_count = 0
        computation_started = asyncio.Event()
        invalidation_done = asyncio.Event()

        @cachu.cache(ttl=60, backend='memory')
        async def slow_compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            computation_started.set()
            await asyncio.wait_for(invalidation_done.wait(), timeout=1)
            await asyncio.sleep(0.05)
            return x * 2

        async def compute_task():
            return await slow_compute(5)

        async def invalidate_task():
            await asyncio.wait_for(computation_started.wait(), timeout=1)
            await slow_compute.invalidate(x=5)
            invalidation_done.set()

        await asyncio.gather(compute_task(), invalidate_task())

        result = await slow_compute(5)
        assert result == 10
        assert call_count == 1, "Invalidation during computation should not prevent caching"

    async def test_async_stats_accurate_under_concurrent_hits(self):
        """Verify async hit/miss counts are accurate with concurrent access.
        """
        @cachu.cache(ttl=60, backend='memory')
        async def compute(x: int) -> int:
            await asyncio.sleep(0.05)
            return x * 2

        await compute(5)

        tasks = [asyncio.create_task(compute(5)) for _ in range(5)]
        await asyncio.gather(*tasks)

        info = await cachu.async_cache_info(compute)
        assert info.misses == 1
        assert info.hits == 5
