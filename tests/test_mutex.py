"""Tests for mutex implementations used in dogpile prevention.
"""
import threading
import time

import pytest
from cachu.mutex import AsyncioMutex, AsyncRedisMutex, RedisMutex
from cachu.mutex import ThreadingMutex


class TestThreadingMutex:
    """Tests for ThreadingMutex (per-key threading.Lock).
    """

    def test_acquire_and_release(self):
        """Verify ThreadingMutex can acquire and release.
        """
        mutex = ThreadingMutex('test_key')
        assert mutex.acquire() is True
        mutex.release()

    def test_same_key_uses_same_lock(self):
        """Verify same key returns mutex with same underlying lock.
        """
        mutex1 = ThreadingMutex('shared_key')
        mutex2 = ThreadingMutex('shared_key')
        assert mutex1._lock is mutex2._lock

    def test_different_keys_use_different_locks(self):
        """Verify different keys return mutexes with different locks.
        """
        mutex1 = ThreadingMutex('key1')
        mutex2 = ThreadingMutex('key2')
        assert mutex1._lock is not mutex2._lock

    def test_context_manager(self):
        """Verify ThreadingMutex works as a context manager.
        """
        mutex = ThreadingMutex('context_test')
        with mutex:
            pass

    def test_timeout_when_locked(self):
        """Verify ThreadingMutex.acquire() times out when lock is held.
        """
        mutex1 = ThreadingMutex('timeout_test')
        mutex2 = ThreadingMutex('timeout_test')

        mutex1.acquire()
        try:
            start = time.time()
            result = mutex2.acquire(timeout=0.1)
            elapsed = time.time() - start
            assert result is False
            assert elapsed >= 0.1
        finally:
            mutex1.release()


class TestAsyncioMutex:
    """Tests for AsyncioMutex (per-key asyncio.Lock).
    """

    async def test_acquire_and_release(self):
        """Verify AsyncioMutex can acquire and release.
        """
        mutex = AsyncioMutex('async_test_key')
        assert await mutex.acquire() is True
        await mutex.release()

    async def test_same_key_uses_same_lock(self):
        """Verify same key returns mutex with same underlying lock.
        """
        mutex1 = AsyncioMutex('async_shared')
        mutex2 = AsyncioMutex('async_shared')
        assert mutex1._lock is mutex2._lock

    async def test_different_keys_use_different_locks(self):
        """Verify different keys return mutexes with different locks.
        """
        mutex1 = AsyncioMutex('async_key1')
        mutex2 = AsyncioMutex('async_key2')
        assert mutex1._lock is not mutex2._lock

    async def test_context_manager(self):
        """Verify AsyncioMutex works as an async context manager.
        """
        mutex = AsyncioMutex('async_context')
        async with mutex:
            pass

    async def test_timeout_when_locked(self):
        """Verify AsyncioMutex.acquire() times out when lock is held.
        """
        mutex1 = AsyncioMutex('async_timeout')
        mutex2 = AsyncioMutex('async_timeout')

        await mutex1.acquire()
        try:
            start = time.time()
            result = await mutex2.acquire(timeout=0.1)
            elapsed = time.time() - start
            assert result is False
            assert elapsed >= 0.1
        finally:
            await mutex1.release()


class TestMutexSafety:
    """Tests for mutex _acquired flag safety checks.
    """

    def test_threading_release_without_acquire_is_noop(self):
        """Verify ThreadingMutex.release() without acquire does nothing.
        """
        mutex = ThreadingMutex('safety_test_1')
        mutex.release()

    def test_threading_double_release_is_noop(self):
        """Verify ThreadingMutex double release does nothing.
        """
        mutex = ThreadingMutex('safety_test_2')
        mutex.acquire()
        mutex.release()
        mutex.release()


class TestAsyncMutexSafety:
    """Tests for async mutex _acquired flag safety checks.
    """

    async def test_asyncio_release_without_acquire_is_noop(self):
        """Verify AsyncioMutex.release() without acquire does nothing.
        """
        mutex = AsyncioMutex('async_safety_test_1')
        await mutex.release()

    async def test_asyncio_double_release_is_noop(self):
        """Verify AsyncioMutex double release does nothing.
        """
        mutex = AsyncioMutex('async_safety_test_2')
        await mutex.acquire()
        await mutex.release()
        await mutex.release()


class TestAsyncioMutexThreadSafety:
    """Tests for AsyncioMutex thread-safety during creation.
    """

    def test_concurrent_creation_same_key(self):
        """Verify concurrent mutex creation returns same lock instance.

        Spawns multiple threads that simultaneously create AsyncioMutex
        instances for the same key. Without proper locking, each thread
        may create its own Lock object, breaking mutual exclusion.
        """
        AsyncioMutex.clear_locks()
        key = 'concurrent_test'
        mutexes = []
        results_lock = threading.Lock()
        barrier = threading.Barrier(10)

        def create_mutex():
            barrier.wait()
            mutex = AsyncioMutex(key)
            with results_lock:
                mutexes.append(mutex)

        threads = [threading.Thread(target=create_mutex) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        locks = [m._lock for m in mutexes]
        unique_locks = len({id(lock) for lock in locks})
        assert unique_locks == 1, (
            f'Race condition detected: {unique_locks} different locks created for same key'
        )


@pytest.mark.redis
class TestRedisMutex:
    """Tests for RedisMutex (distributed lock via Redis).
    """

    @pytest.fixture
    def redis_client(self, redis_docker):
        """Provide a Redis client for testing.
        """
        import redis
        from _fixtures.redis import redis_test_config

        client = redis.Redis(
            host=redis_test_config.host,
            port=redis_test_config.port,
            db=0,
        )
        yield client
        client.close()

    def test_acquire_and_release(self, redis_client):
        """Verify RedisMutex can acquire and release.
        """
        mutex = RedisMutex(redis_client, 'lock:test', lock_timeout=10.0)
        assert mutex.acquire() is True
        mutex.release()

    def test_lock_is_set_in_redis(self, redis_client):
        """Verify RedisMutex sets a key in Redis when acquired.
        """
        mutex = RedisMutex(redis_client, 'lock:check', lock_timeout=10.0)
        mutex.acquire()
        try:
            assert redis_client.exists('lock:check') == 1
        finally:
            mutex.release()

    def test_lock_is_removed_after_release(self, redis_client):
        """Verify RedisMutex removes key from Redis when released.
        """
        mutex = RedisMutex(redis_client, 'lock:remove', lock_timeout=10.0)
        mutex.acquire()
        mutex.release()
        assert redis_client.exists('lock:remove') == 0

    def test_safe_release_with_lua_script(self, redis_client):
        """Verify RedisMutex only releases if token matches (Lua script).
        """
        mutex = RedisMutex(redis_client, 'lock:lua', lock_timeout=10.0)
        mutex.acquire()
        redis_client.set('lock:lua', 'different_token')
        mutex.release()
        assert redis_client.get('lock:lua') == b'different_token'
        redis_client.delete('lock:lua')

    def test_context_manager(self, redis_client):
        """Verify RedisMutex works as a context manager.
        """
        mutex = RedisMutex(redis_client, 'lock:context', lock_timeout=10.0)
        with mutex:
            assert redis_client.exists('lock:context') == 1

    def test_timeout_when_locked(self, redis_client):
        """Verify RedisMutex.acquire() times out when lock is held.
        """
        mutex1 = RedisMutex(redis_client, 'lock:timeout', lock_timeout=10.0)
        mutex2 = RedisMutex(redis_client, 'lock:timeout', lock_timeout=10.0)

        mutex1.acquire()
        try:
            start = time.time()
            result = mutex2.acquire(timeout=0.15)
            elapsed = time.time() - start
            assert result is False
            assert elapsed >= 0.15
        finally:
            mutex1.release()


@pytest.mark.redis
class TestAsyncRedisMutex:
    """Tests for AsyncRedisMutex (async distributed lock via Redis).
    """

    @pytest.fixture
    async def async_redis_client(self, redis_docker):
        """Provide an async Redis client for testing.
        """
        import redis.asyncio as aioredis
        from _fixtures.redis import redis_test_config

        client = aioredis.Redis(
            host=redis_test_config.host,
            port=redis_test_config.port,
            db=0,
        )
        yield client
        await client.aclose()

    async def test_acquire_and_release(self, async_redis_client):
        """Verify AsyncRedisMutex can acquire and release.
        """
        mutex = AsyncRedisMutex(async_redis_client, 'lock:async_test', lock_timeout=10.0)
        assert await mutex.acquire() is True
        await mutex.release()

    async def test_lock_is_set_in_redis(self, async_redis_client):
        """Verify AsyncRedisMutex sets a key in Redis when acquired.
        """
        mutex = AsyncRedisMutex(async_redis_client, 'lock:async_check', lock_timeout=10.0)
        await mutex.acquire()
        try:
            assert await async_redis_client.exists('lock:async_check') == 1
        finally:
            await mutex.release()

    async def test_lock_is_removed_after_release(self, async_redis_client):
        """Verify AsyncRedisMutex removes key from Redis when released.
        """
        mutex = AsyncRedisMutex(async_redis_client, 'lock:async_remove', lock_timeout=10.0)
        await mutex.acquire()
        await mutex.release()
        assert await async_redis_client.exists('lock:async_remove') == 0

    async def test_context_manager(self, async_redis_client):
        """Verify AsyncRedisMutex works as an async context manager.
        """
        mutex = AsyncRedisMutex(async_redis_client, 'lock:async_ctx', lock_timeout=10.0)
        async with mutex:
            assert await async_redis_client.exists('lock:async_ctx') == 1

    async def test_timeout_when_locked(self, async_redis_client):
        """Verify AsyncRedisMutex.acquire() times out when lock is held.
        """
        mutex1 = AsyncRedisMutex(async_redis_client, 'lock:async_to', lock_timeout=10.0)
        mutex2 = AsyncRedisMutex(async_redis_client, 'lock:async_to', lock_timeout=10.0)

        await mutex1.acquire()
        try:
            start = time.time()
            result = await mutex2.acquire(timeout=0.15)
            elapsed = time.time() - start
            assert result is False
            assert elapsed >= 0.15
        finally:
            await mutex1.release()
