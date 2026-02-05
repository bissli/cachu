"""Tests for stats persistence across backend instances.

These tests verify that stats are stored in the backend (not in-memory)
and can be shared across processes/instances.
"""
import tempfile

import pytest
from cachu.backends.sqlite import SqliteBackend


class TestSqliteStatsPersistence:
    """Tests for SQLite stats persistence.
    """

    def test_stats_persist_across_backend_recreation(self):
        """Verify stats survive backend close/reopen.
        """
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            filepath = f.name

        backend1 = SqliteBackend(filepath)
        backend1.incr_stat('my_func', 'hits')
        backend1.incr_stat('my_func', 'hits')
        backend1.incr_stat('my_func', 'misses')
        backend1.close()

        backend2 = SqliteBackend(filepath)
        hits, misses = backend2.get_stats('my_func')
        backend2.close()

        assert hits == 2
        assert misses == 1

    def test_stats_shared_across_instances(self):
        """Verify two backend instances see same stats (simulates multi-process).
        """
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            filepath = f.name

        backend1 = SqliteBackend(filepath)
        backend2 = SqliteBackend(filepath)

        backend1.incr_stat('shared_func', 'hits')
        backend2.incr_stat('shared_func', 'hits')
        backend1.incr_stat('shared_func', 'misses')

        hits1, misses1 = backend1.get_stats('shared_func')
        hits2, misses2 = backend2.get_stats('shared_func')

        assert hits1 == 2
        assert misses1 == 1
        assert hits2 == 2
        assert misses2 == 1

        backend1.close()
        backend2.close()

    @pytest.mark.asyncio
    async def test_async_stats_persist_across_backend_recreation(self):
        """Verify async stats survive backend close/reopen.
        """
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            filepath = f.name

        backend1 = SqliteBackend(filepath)
        await backend1.aincr_stat('async_func', 'hits')
        await backend1.aincr_stat('async_func', 'misses')
        await backend1.aclose()

        backend2 = SqliteBackend(filepath)
        hits, misses = await backend2.aget_stats('async_func')
        await backend2.aclose()

        assert hits == 1
        assert misses == 1


@pytest.mark.redis
class TestRedisStatsPersistence:
    """Tests for Redis stats shared across instances.
    """

    def test_stats_shared_across_instances(self, redis_docker):
        """Verify two backend instances see same stats.
        """
        from cachu.backends.redis import RedisBackend
        from _fixtures.redis import redis_test_config

        url = f'redis://{redis_test_config.host}:{redis_test_config.port}/0'

        backend1 = RedisBackend(url)
        backend2 = RedisBackend(url)

        backend1.incr_stat('redis_shared_func', 'hits')
        backend2.incr_stat('redis_shared_func', 'hits')
        backend1.incr_stat('redis_shared_func', 'misses')

        hits1, misses1 = backend1.get_stats('redis_shared_func')
        hits2, misses2 = backend2.get_stats('redis_shared_func')

        assert hits1 == 2
        assert misses1 == 1
        assert hits2 == 2
        assert misses2 == 1

        backend1.close()
        backend2.close()

    @pytest.mark.asyncio
    async def test_async_stats_shared_across_instances(self, redis_docker):
        """Verify two async backend instances see same stats.
        """
        from cachu.backends.redis import RedisBackend
        from _fixtures.redis import redis_test_config

        url = f'redis://{redis_test_config.host}:{redis_test_config.port}/0'

        backend1 = RedisBackend(url)
        backend2 = RedisBackend(url)

        await backend1.aincr_stat('async_redis_func', 'hits')
        await backend2.aincr_stat('async_redis_func', 'misses')
        await backend1.aincr_stat('async_redis_func', 'misses')

        hits1, misses1 = await backend1.aget_stats('async_redis_func')
        hits2, misses2 = await backend2.aget_stats('async_redis_func')

        assert hits1 == 1
        assert misses1 == 2
        assert hits2 == 1
        assert misses2 == 2

        await backend1.aclose()
        await backend2.aclose()
