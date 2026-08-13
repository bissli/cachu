"""Tests for mutex implementations used in dogpile prevention.
"""
import asyncio
import time

import pytest
import redis
import redis.asyncio as aioredis
from _fixtures.redis import redis_test_config
from cachu.mutex import AsyncioMutex, AsyncRedisMutex, RedisMutex
from cachu.mutex import ThreadingMutex


@pytest.fixture
def redis_client(redis_docker):
    """Provide a sync Redis client pointed at the test container.
    """
    client = redis.Redis(
        host=redis_test_config.host,
        port=redis_test_config.port,
        db=0,
    )
    yield client
    client.close()


@pytest.fixture
async def async_redis_client(redis_docker):
    """Provide an async Redis client pointed at the test container.
    """
    client = aioredis.Redis(
        host=redis_test_config.host,
        port=redis_test_config.port,
        db=0,
    )
    yield client
    await client.aclose()


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
        """Verify same key resolves to the same underlying lock on a loop.
        """
        mutex1 = AsyncioMutex('async_shared')
        mutex2 = AsyncioMutex('async_shared')
        await mutex1.acquire()
        lock1 = mutex1._lock
        await mutex1.release()
        await mutex2.acquire()
        lock2 = mutex2._lock
        await mutex2.release()
        assert lock1 is lock2

    async def test_different_keys_use_different_locks(self):
        """Verify different keys resolve to different underlying locks.
        """
        mutex1 = AsyncioMutex('async_key1')
        mutex2 = AsyncioMutex('async_key2')
        await mutex1.acquire()
        await mutex2.acquire()
        try:
            assert mutex1._lock is not mutex2._lock
        finally:
            await mutex1.release()
            await mutex2.release()

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

    def test_concurrent_resolution_same_key(self):
        """Verify concurrent resolution returns one lock instance per loop.

        Many coroutines resolving the same key on one event loop must share a
        single Lock object; otherwise mutual exclusion breaks.
        """
        AsyncioMutex.clear_locks()
        key = 'concurrent_test'

        async def scenario():
            mutexes = [AsyncioMutex(key) for _ in range(10)]
            return [m._resolve_lock() for m in mutexes]

        locks = asyncio.run(scenario())
        unique_locks = len({id(lock) for lock in locks})
        assert unique_locks == 1, (
            f'Race condition detected: {unique_locks} different locks created for same key'
        )


@pytest.mark.redis
class TestRedisMutex:
    """Tests for RedisMutex (distributed lock via Redis).
    """

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


class TestZeroTimeoutSemantics:
    """A timeout of 0 means "try once"; only None means "use the default".

    Notes
    -----
    - RedisMutex.acquire resolved its argument with
      `timeout or self._lock_timeout`, so an explicit 0 was falsy and
      silently became a full-length wait.
    - The decorator clamps the dogpile wait to what is left of
      `cache_deadline`, so a zero remainder has to mean zero.
    """

    @pytest.mark.redis
    def test_zero_timeout_returns_immediately_when_held(self, redis_client):
        """acquire(timeout=0) on a held lock fails fast instead of waiting.

        Mutation: restore `timeout = timeout or self._lock_timeout`, turning
        an explicit 0 into the configured 10s wait.
        Oracle: elapsed wall time, which must stay far below the 10s
        lock_timeout that a falsy-check would have used.
        """
        holder = RedisMutex(redis_client, 'lock:zero', lock_timeout=10.0)
        waiter = RedisMutex(redis_client, 'lock:zero', lock_timeout=10.0)

        assert holder.acquire() is True
        try:
            start = time.monotonic()
            assert waiter.acquire(timeout=0) is False
            assert time.monotonic() - start < 1.0
        finally:
            holder.release()

    @pytest.mark.redis
    def test_zero_timeout_still_makes_one_attempt(self, redis_client):
        """acquire(timeout=0) on a free lock succeeds, like threading.Lock.

        Mutation: keep the `while time.time() < end` pre-test loop, which
        never attempts at all when timeout is 0.
        Oracle: threading.Lock.acquire(timeout=0), which does attempt once.
        """
        mutex = RedisMutex(redis_client, 'lock:zero_free', lock_timeout=10.0)

        try:
            assert mutex.acquire(timeout=0) is True
        finally:
            mutex.release()

    @pytest.mark.redis
    def test_none_timeout_still_uses_the_configured_default(self, redis_client):
        """acquire() with no argument still waits lock_timeout seconds.

        Mutation: default the timeout to 0, turning every unqualified acquire
        into a single attempt and destroying dogpile suppression.
        Oracle: the configured lock_timeout, 0.3s, as a lower bound on the
        observed wait.
        """
        holder = RedisMutex(redis_client, 'lock:none_to', lock_timeout=0.3)
        waiter = RedisMutex(redis_client, 'lock:none_to', lock_timeout=0.3)

        assert holder.acquire() is True
        try:
            start = time.monotonic()
            assert waiter.acquire() is False
            assert time.monotonic() - start >= 0.3
        finally:
            holder.release()

    @pytest.mark.redis
    async def test_async_zero_timeout_returns_immediately(self, async_redis_client):
        """The async Redis mutex honors a zero timeout too.

        Mutation: fix only the sync mutex.
        Oracle: elapsed wall time, well below the 10s lock_timeout.
        """
        holder = AsyncRedisMutex(async_redis_client, 'lock:azero', lock_timeout=10.0)
        waiter = AsyncRedisMutex(async_redis_client, 'lock:azero', lock_timeout=10.0)

        assert await holder.acquire() is True
        try:
            start = time.monotonic()
            assert await waiter.acquire(timeout=0) is False
            assert time.monotonic() - start < 1.0
        finally:
            await holder.release()


class TestLocalMutexZeroTimeout:
    """The local mutexes honor a zero timeout like threading.Lock does.

    Notes
    -----
    - `_remaining_lock_wait` clamps the dogpile wait to what is left of
      `cache_deadline`, so a zero wait is reachable in normal operation.
    - `asyncio.wait_for(lock.acquire(), 0)` cannot express "try once": it
      wraps the coroutine in a not-yet-done task and raises even when the
      lock is free, which would surface as a spurious CacheLockTimeout on an
      uncontended key.
    """

    def test_threading_mutex_zero_timeout_on_free_lock(self):
        """ThreadingMutex(timeout=0) acquires a free lock.

        Mutation: short-circuit a non-positive timeout to False.
        Oracle: threading.Lock.acquire(timeout=0), which attempts once.
        """
        mutex = ThreadingMutex('zero:free')
        try:
            assert mutex.acquire(timeout=0) is True
        finally:
            mutex.release()

    def test_threading_mutex_zero_timeout_on_held_lock(self):
        """ThreadingMutex(timeout=0) fails immediately on a held lock.

        Mutation: fall back to a blocking acquire when timeout is falsy.
        Oracle: elapsed wall time, bounded well under a second.
        """
        holder = ThreadingMutex('zero:held')
        waiter = ThreadingMutex('zero:held')

        assert holder.acquire() is True
        try:
            start = time.monotonic()
            assert waiter.acquire(timeout=0) is False
            assert time.monotonic() - start < 1.0
        finally:
            holder.release()

    async def test_asyncio_mutex_zero_timeout_on_free_lock(self):
        """AsyncioMutex(timeout=0) acquires a free lock.

        Mutation: route a zero timeout through asyncio.wait_for, which
        returns False on an uncontended lock and diverges from every other
        mutex in the library.
        Oracle: ThreadingMutex and RedisMutex at timeout=0, both True.
        """
        mutex = AsyncioMutex('azero:free')
        try:
            assert await mutex.acquire(timeout=0) is True
        finally:
            await mutex.release()

    async def test_asyncio_mutex_zero_timeout_on_held_lock(self):
        """AsyncioMutex(timeout=0) fails immediately on a held lock.

        Mutation: drop the locked() test and always acquire, which would
        block forever behind the holder.
        Oracle: elapsed wall time, bounded well under a second.
        """
        holder = AsyncioMutex('azero:held')
        waiter = AsyncioMutex('azero:held')

        assert await holder.acquire() is True
        try:
            start = time.monotonic()
            assert await waiter.acquire(timeout=0) is False
            assert time.monotonic() - start < 1.0
        finally:
            await holder.release()

    async def test_asyncio_mutex_still_waits_for_a_positive_timeout(self):
        """A positive timeout keeps the real wait, it is not short-circuited.

        Mutation: treat every timeout as the zero case, destroying dogpile
        suppression for async callers.
        Oracle: the requested wait, 0.15s, as a lower bound.
        """
        holder = AsyncioMutex('await:held')
        waiter = AsyncioMutex('await:held')

        assert await holder.acquire() is True
        try:
            start = time.monotonic()
            assert await waiter.acquire(timeout=0.15) is False
            assert time.monotonic() - start >= 0.15
        finally:
            await holder.release()
